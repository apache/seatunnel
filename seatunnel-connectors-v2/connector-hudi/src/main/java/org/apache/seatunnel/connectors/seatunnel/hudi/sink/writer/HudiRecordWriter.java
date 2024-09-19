/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.client.WriteClientProvider;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.convert.HudiRecordConverter;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.state.HudiCommitInfo;

import org.apache.avro.Schema;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;

import static org.apache.seatunnel.connectors.seatunnel.hudi.sink.convert.AvroSchemaConverter.convertToSchema;

@Slf4j
public class HudiRecordWriter implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(HudiRecordWriter.class);

    private final HudiTableConfig hudiTableConfig;

    private final WriteClientProvider clientProvider;

    private final HudiRecordConverter recordConverter;

    private final SeaTunnelRowType seaTunnelRowType;

    private final boolean autoCommit;

    private Schema schema;

    private transient int batchCount = 0;

    private final List<HoodieRecord<HoodieAvroPayload>> writeRecords;

    private Stack<String> forceCommitTime;

    private String writeInstantTime;

    private List<WriteStatus> writeStatusList;

    private transient volatile boolean closed = false;

    private transient volatile Exception flushException;

    public HudiRecordWriter(
            HudiSinkConfig hudiSinkConfig,
            HudiTableConfig hudiTableConfig,
            WriteClientProvider clientProvider,
            SeaTunnelRowType seaTunnelRowType) {
        this.hudiTableConfig = hudiTableConfig;
        this.autoCommit = hudiSinkConfig.isAutoCommit();
        this.clientProvider = clientProvider;
        this.seaTunnelRowType = seaTunnelRowType;
        this.writeRecords = new ArrayList<>();
        this.writeStatusList = new ArrayList<>();
        this.forceCommitTime = new Stack<>();
        this.recordConverter = new HudiRecordConverter();
    }

    public HudiRecordWriter(
            HudiSinkConfig sinkConfig,
            HudiTableConfig tableConfig,
            WriteClientProvider writeClientProvider,
            SeaTunnelRowType seaTunnelRowType,
            HudiCommitInfo hudiCommitInfo) {
        this(sinkConfig, tableConfig, writeClientProvider, seaTunnelRowType);
        this.writeInstantTime = hudiCommitInfo.getWriteInstantTime();
        this.writeStatusList = hudiCommitInfo.getWriteStatusList();
    }

    public void open() {
        this.schema = new Schema.Parser().parse(convertToSchema(seaTunnelRowType).toString());
        try {
            HoodieJavaWriteClient<HoodieAvroPayload> writeClient =
                    clientProvider.getOrCreateClient();
            if (StringUtils.nonEmpty(writeInstantTime) && Objects.nonNull(writeStatusList)) {
                if (!writeClient.commit(writeInstantTime, writeStatusList)) {
                    LOG.warn("Failed to commit history data.");
                }
            }
        } catch (Exception e) {
            throw new HudiConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "Commit history data error.",
                    e);
        }
    }

    public void writeRecord(SeaTunnelRow record) {
        checkFlushException();
        try {
            prepareRecords(record);
            batchCount++;
            if (hudiTableConfig.getBatchSize() > 0
                    && batchCount >= hudiTableConfig.getBatchSize()) {
                flush(true);
            }
        } catch (Exception e) {
            throw new HudiConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "Writing records to Hudi failed.",
                    e);
        }
    }

    public synchronized void flush(boolean isNeedForceCommit) {
        if (batchCount == 0) {
            log.debug("No data needs to be refreshed, waiting for incoming data.");
            return;
        }
        checkFlushException();
        HoodieJavaWriteClient<HoodieAvroPayload> writeClient = clientProvider.getOrCreateClient();
        if (autoCommit || writeInstantTime == null) {
            writeInstantTime = writeClient.startCommit();
        }
        List<WriteStatus> currentWriteStatusList;
        // write records
        switch (hudiTableConfig.getOpType()) {
            case INSERT:
                currentWriteStatusList = writeClient.insert(writeRecords, writeInstantTime);
                break;
            case UPSERT:
                currentWriteStatusList = writeClient.upsert(writeRecords, writeInstantTime);
                break;
            case BULK_INSERT:
                currentWriteStatusList = writeClient.bulkInsert(writeRecords, writeInstantTime);
                break;
            default:
                throw new HudiConnectorException(
                        HudiErrorCode.UNSUPPORTED_OPERATION,
                        "Unsupported operation type: " + hudiTableConfig.getOpType());
        }
        if (!autoCommit) {
            this.writeStatusList.addAll(currentWriteStatusList);
        }
        /**
         * when the batch size of temporary records is reached, commit is forced here, even if
         * configured not to be auto commit. because a timeline supports only one commit.
         */
        forceCommit(isNeedForceCommit, autoCommit);
        writeRecords.clear();
        batchCount = 0;
    }

    public Optional<HudiCommitInfo> prepareCommit() {
        flush(false);
        if (!autoCommit) {
            return Optional.of(
                    new HudiCommitInfo(writeInstantTime, writeStatusList, forceCommitTime));
        }
        return Optional.empty();
    }

    private void commit() {
        if (StringUtils.nonEmpty(writeInstantTime) && !writeStatusList.isEmpty()) {
            log.debug(
                    "Commit hudi records, the instant time is {} and write status are {}",
                    writeInstantTime,
                    writeStatusList);
            clientProvider.getOrCreateClient().commit(writeInstantTime, writeStatusList);
            resetUpsertCommitInfo();
        }
    }

    private void forceCommit(boolean isNeedForceCommit, boolean isAutoCommit) {
        if (isNeedForceCommit && !isAutoCommit) {
            clientProvider.getOrCreateClient().commit(writeInstantTime, writeStatusList);
            forceCommitTime.add(writeInstantTime);
            resetUpsertCommitInfo();
        }
    }

    public HudiCommitInfo snapshotState() {
        HudiCommitInfo hudiCommitInfo =
                new HudiCommitInfo(writeInstantTime, writeStatusList, forceCommitTime);
        // reset commit info in here, because the commit info will be committed in committer.
        resetUpsertCommitInfo();
        // reset the force commit stack.
        forceCommitTime = new Stack<>();
        return hudiCommitInfo;
    }

    protected void resetUpsertCommitInfo() {
        writeInstantTime = null;
        writeStatusList = new ArrayList<>();
    }

    protected void prepareRecords(SeaTunnelRow element) {
        HoodieRecord<HoodieAvroPayload> hoodieAvroPayloadHoodieRecord =
                recordConverter.convertRow(schema, seaTunnelRowType, element, hudiTableConfig);
        writeRecords.add(hoodieAvroPayloadHoodieRecord);
    }

    protected void checkFlushException() {
        if (flushException != null) {
            throw new HudiConnectorException(
                    HudiErrorCode.FLUSH_DATA_FAILED,
                    "Flush records to Hudi failed.",
                    flushException);
        }
    }

    /** Executes prepared statement and closes all resources of this instance. */
    public synchronized void close() {
        if (!closed) {
            closed = true;
            try {
                flush(false);
                if (!autoCommit) {
                    commit();
                }
            } catch (Exception e) {
                LOG.warn("Flush records to Hudi failed.", e);
                flushException =
                        new HudiConnectorException(
                                CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                                "Flush records to Hudi failed.",
                                e);
            }

            try {
                if (clientProvider != null) {
                    clientProvider.close();
                }
            } catch (Exception e) {
                LOG.warn("Close Hudi record writer failed.", e);
            }
        }
        checkFlushException();
    }
}
