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

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.client.WriteClientProvider;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.convert.HudiRecordConverter;

import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.hudi.sink.convert.AvroSchemaConverter.convertToSchema;

@Slf4j
public class HudiRecordWriter implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(HudiRecordWriter.class);

    private final HudiTableConfig hudiTableConfig;

    private final WriteClientProvider clientProvider;

    private final HudiRecordConverter recordConverter;

    private final SeaTunnelRowType seaTunnelRowType;

    private Schema schema;

    private transient int batchCount = 0;

    private final List<HoodieRecord<HoodieAvroPayload>> writeRecords;

    private final List<HoodieKey> deleteRecordKeys;

    private final LinkedHashMap<HoodieKey, Pair<Boolean, HoodieRecord<HoodieAvroPayload>>> buffer =
            new LinkedHashMap<>();

    private transient volatile boolean closed = false;

    private transient volatile Exception flushException;

    public HudiRecordWriter(
            HudiTableConfig hudiTableConfig,
            WriteClientProvider clientProvider,
            SeaTunnelRowType seaTunnelRowType) {
        this.hudiTableConfig = hudiTableConfig;
        this.clientProvider = clientProvider;
        this.seaTunnelRowType = seaTunnelRowType;
        this.writeRecords = new ArrayList<>();
        this.deleteRecordKeys = new ArrayList<>();
        this.recordConverter = new HudiRecordConverter();
    }

    public void open() {
        this.schema =
                new Schema.Parser()
                        .parse(
                                convertToSchema(
                                                seaTunnelRowType,
                                                AvroSchemaUtils.getAvroRecordQualifiedName(
                                                        hudiTableConfig.getTableName()))
                                        .toString());
        try {
            clientProvider.getOrCreateClient();
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
                flush();
            }
        } catch (Exception e) {
            throw new HudiConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "Writing records to Hudi failed.",
                    e);
        }
    }

    public synchronized void flush() {
        if (batchCount == 0) {
            log.debug("No data needs to be refreshed, waiting for incoming data.");
            return;
        }
        checkFlushException();
        Boolean preChangeFlag = null;
        Set<Map.Entry<HoodieKey, Pair<Boolean, HoodieRecord<HoodieAvroPayload>>>> entries =
                buffer.entrySet();
        for (Map.Entry<HoodieKey, Pair<Boolean, HoodieRecord<HoodieAvroPayload>>> entry : entries) {
            boolean currentChangeFlag = entry.getValue().getKey();
            if (currentChangeFlag) {
                if (preChangeFlag != null && !preChangeFlag) {
                    executeDelete();
                }
                writeRecords.add(entry.getValue().getValue());
            } else {
                if (preChangeFlag != null && preChangeFlag) {
                    executeWrite();
                }
                deleteRecordKeys.add(entry.getKey());
            }
            preChangeFlag = currentChangeFlag;
        }

        if (preChangeFlag != null) {
            if (preChangeFlag) {
                executeWrite();
            } else {
                executeDelete();
            }
        }
        batchCount = 0;
        buffer.clear();
    }

    private void executeWrite() {
        HoodieJavaWriteClient<HoodieAvroPayload> writeClient = clientProvider.getOrCreateClient();
        String writeInstantTime = writeClient.startCommit();
        // write records
        switch (hudiTableConfig.getOpType()) {
            case INSERT:
                writeClient.insert(writeRecords, writeInstantTime);
                break;
            case UPSERT:
                writeClient.upsert(writeRecords, writeInstantTime);
                break;
            case BULK_INSERT:
                writeClient.bulkInsert(writeRecords, writeInstantTime);
                break;
            default:
                throw new HudiConnectorException(
                        HudiErrorCode.UNSUPPORTED_OPERATION,
                        "Unsupported operation type: " + hudiTableConfig.getOpType());
        }
        writeRecords.clear();
    }

    private void executeDelete() {
        HoodieJavaWriteClient<HoodieAvroPayload> writeClient = clientProvider.getOrCreateClient();
        writeClient.delete(deleteRecordKeys, writeClient.startCommit());
        deleteRecordKeys.clear();
    }

    protected void prepareRecords(SeaTunnelRow element) {
        HoodieRecord<HoodieAvroPayload> hoodieAvroPayloadHoodieRecord =
                recordConverter.convertRow(schema, seaTunnelRowType, element, hudiTableConfig);
        HoodieKey recordKey = hoodieAvroPayloadHoodieRecord.getKey();
        boolean changeFlag = changeFlag(element.getRowKind());
        buffer.put(recordKey, Pair.of(changeFlag, hoodieAvroPayloadHoodieRecord));
    }

    private boolean changeFlag(RowKind rowKind) {
        switch (rowKind) {
            case DELETE:
            case UPDATE_BEFORE:
                return false;
            case INSERT:
            case UPDATE_AFTER:
                return true;
            default:
                throw new UnsupportedOperationException("Unknown row kind: " + rowKind);
        }
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
                flush();
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
