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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hudi.state.HudiCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.state.HudiSinkState;
import org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer.AvroSchemaConverter.convertToSchema;
import static org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer.RowDataToAvroConverters.createConverter;

@Slf4j
public class HudiSinkWriter
        implements SinkWriter<SeaTunnelRow, HudiCommitInfo, HudiSinkState>,
                SupportMultiTableSinkWriter<Void> {

    public static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";
    protected static final String DEFAULT_PARTITION_PATH = "default";
    protected static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
    protected static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";
    private final HoodieJavaWriteClient<HoodieAvroPayload> writeClient;
    private final WriteOperationType opType;
    private final Schema schema;
    private final SeaTunnelRowType seaTunnelRowType;
    private final HudiSinkConfig hudiSinkConfig;
    private final List<HoodieRecord<HoodieAvroPayload>> hoodieRecords;
    private transient List<WriteStatus> writeStatusList;
    private transient String instantTime;
    private transient int batchCount = 0;
    private transient volatile boolean closed = false;
    private transient volatile Exception flushException;

    public HudiSinkWriter(
            SinkWriter.Context context,
            SeaTunnelRowType seaTunnelRowType,
            HudiSinkConfig hudiSinkConfig,
            List<HudiSinkState> hudiSinkState)
            throws IOException {

        this.hoodieRecords = new ArrayList<>(30);
        this.seaTunnelRowType = seaTunnelRowType;
        this.schema = new Schema.Parser().parse(convertToSchema(seaTunnelRowType).toString());
        this.opType = hudiSinkConfig.getOpType();
        this.hudiSinkConfig = hudiSinkConfig;
        Configuration hadoopConf = new Configuration();
        if (hudiSinkConfig.getConfFilesPath() != null) {
            hadoopConf = HudiUtil.getConfiguration(hudiSinkConfig.getConfFilesPath());
        }
        HadoopStorageConfiguration hudiStorageConfiguration =
                new HadoopStorageConfiguration(hadoopConf);

        // initialize the table, if not done already
        Path path = new Path(hudiSinkConfig.getTableDfsPath());
        FileSystem fs =
                HadoopFSUtils.getFs(hudiSinkConfig.getTableDfsPath(), hudiStorageConfiguration);
        HoodieTableMetaClient.withPropertyBuilder()
                .setTableType(hudiSinkConfig.getTableType())
                .setTableName(hudiSinkConfig.getTableName())
                .setPayloadClassName(HoodieAvroPayload.class.getName())
                .initTable(hudiStorageConfiguration, hudiSinkConfig.getTableDfsPath());
        HoodieWriteConfig cfg =
                HoodieWriteConfig.newBuilder()
                        .withEmbeddedTimelineServerEnabled(false)
                        .withEngineType(EngineType.JAVA)
                        .withPath(hudiSinkConfig.getTableDfsPath())
                        .withSchema(convertToSchema(seaTunnelRowType).toString())
                        .withParallelism(
                                hudiSinkConfig.getInsertShuffleParallelism(),
                                hudiSinkConfig.getUpsertShuffleParallelism())
                        .forTable(hudiSinkConfig.getTableName())
                        .withIndexConfig(
                                HoodieIndexConfig.newBuilder()
                                        .withIndexType(HoodieIndex.IndexType.INMEMORY)
                                        .build())
                        .withArchivalConfig(
                                HoodieArchivalConfig.newBuilder()
                                        .archiveCommitsWith(
                                                hudiSinkConfig.getMinCommitsToKeep(),
                                                hudiSinkConfig.getMaxCommitsToKeep())
                                        .build())
                        .withAutoCommit(false)
                        .withCleanConfig(
                                HoodieCleanConfig.newBuilder()
                                        .withAutoClean(true)
                                        .withAsyncClean(false)
                                        .build())
                        .build();

        writeClient =
                new HoodieJavaWriteClient<>(
                        new HoodieJavaEngineContext(hudiStorageConfiguration), cfg);

        if (!hudiSinkState.isEmpty()) {
            writeClient.commit(
                    hudiSinkState.get(0).getHudiCommitInfo().getInstantTime(),
                    hudiSinkState.get(0).getHudiCommitInfo().getWriteStatusList());
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        checkFlushException();

        batchCount++;
        prepareRecords(element);

        if (batchCount >= hudiSinkConfig.getMaxCommitsToKeep()) {
            flush();
        }
    }

    @Override
    public Optional<HudiCommitInfo> prepareCommit() {
        flush();
        return Optional.of(new HudiCommitInfo(instantTime, writeStatusList));
    }

    @Override
    public List<HudiSinkState> snapshotState(long checkpointId) throws IOException {
        return Collections.singletonList(
                new HudiSinkState(checkpointId, new HudiCommitInfo(instantTime, writeStatusList)));
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        if (!closed) {

            if (batchCount > 0) {
                try {
                    flush();
                } catch (Exception e) {
                    log.warn("Writing records to Hudi failed.", e);
                    throw new HudiConnectorException(
                            CommonErrorCode.WRITE_SEATUNNEL_ROW_ERROR,
                            "Writing records to hudi failed.",
                            e);
                }
            }
            if (writeClient != null) {
                writeClient.close();
            }
            closed = true;
            checkFlushException();
        }
    }

    private void prepareRecords(SeaTunnelRow element) {

        hoodieRecords.add(convertRow(element));
    }

    private HoodieRecord<HoodieAvroPayload> convertRow(SeaTunnelRow element) {
        GenericRecord rec = new GenericData.Record(schema);
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            rec.put(
                    seaTunnelRowType.getFieldNames()[i],
                    createConverter(seaTunnelRowType.getFieldType(i))
                            .convert(
                                    convertToSchema(seaTunnelRowType.getFieldType(i)),
                                    element.getField(i)));
        }
        return new HoodieAvroRecord<>(
                getHoodieKey(element, seaTunnelRowType), new HoodieAvroPayload(Option.of(rec)));
    }

    private HoodieKey getHoodieKey(SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType) {
        String partitionPath =
                hudiSinkConfig.getPartitionFields() == null
                        ? ""
                        : getRecordPartitionPath(element, seaTunnelRowType);
        String rowKey =
                hudiSinkConfig.getRecordKeyFields() == null
                                && hudiSinkConfig.getOpType().equals(WriteOperationType.INSERT)
                        ? UUID.randomUUID().toString()
                        : getRecordKey(element, seaTunnelRowType);
        return new HoodieKey(rowKey, partitionPath);
    }

    private String getRecordKey(SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType) {
        boolean keyIsNullEmpty = true;
        StringBuilder recordKey = new StringBuilder();
        for (String recordKeyField : hudiSinkConfig.getRecordKeyFields().split(",")) {
            String recordKeyValue =
                    getNestedFieldValAsString(element, seaTunnelRowType, recordKeyField);
            recordKeyField = recordKeyField.toLowerCase();
            if (recordKeyValue == null) {
                recordKey
                        .append(recordKeyField)
                        .append(":")
                        .append(NULL_RECORDKEY_PLACEHOLDER)
                        .append(",");
            } else if (recordKeyValue.isEmpty()) {
                recordKey
                        .append(recordKeyField)
                        .append(":")
                        .append(EMPTY_RECORDKEY_PLACEHOLDER)
                        .append(",");
            } else {
                recordKey.append(recordKeyField).append(":").append(recordKeyValue).append(",");
                keyIsNullEmpty = false;
            }
        }
        recordKey.deleteCharAt(recordKey.length() - 1);
        if (keyIsNullEmpty) {
            throw new HoodieKeyException(
                    "recordKey values: \""
                            + recordKey
                            + "\" for fields: "
                            + hudiSinkConfig.getRecordKeyFields()
                            + " cannot be entirely null or empty.");
        }
        return recordKey.toString();
    }

    private String getRecordPartitionPath(SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType) {
        if (hudiSinkConfig.getPartitionFields().isEmpty()) {
            return "";
        }

        StringBuilder partitionPath = new StringBuilder();
        String[] avroPartitionPathFields = hudiSinkConfig.getPartitionFields().split(",");
        for (String partitionPathField : avroPartitionPathFields) {
            String fieldVal =
                    getNestedFieldValAsString(element, seaTunnelRowType, partitionPathField);
            if (fieldVal == null || fieldVal.isEmpty()) {
                partitionPath.append(partitionPathField).append("=").append(DEFAULT_PARTITION_PATH);
            } else {
                partitionPath.append(partitionPathField).append("=").append(fieldVal);
            }
            partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
        }
        partitionPath.deleteCharAt(partitionPath.length() - 1);
        return partitionPath.toString();
    }

    private String getNestedFieldValAsString(
            SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType, String fieldName) {
        Object value = null;

        if (Arrays.stream(seaTunnelRowType.getFieldNames())
                .collect(Collectors.toList())
                .contains(fieldName)) {
            value = element.getField(seaTunnelRowType.indexOf(fieldName));
        }
        return StringUtils.objToString(value);
    }

    public synchronized void flush() {
        checkFlushException();
        instantTime = writeClient.startCommit();
        switch (opType) {
            case INSERT:
                writeStatusList = writeClient.insert(hoodieRecords, instantTime);
                break;
            case UPSERT:
                writeStatusList = writeClient.upsert(hoodieRecords, instantTime);
                break;
            case BULK_INSERT:
                writeStatusList = writeClient.bulkInsert(hoodieRecords, instantTime);
                break;
            default:
                throw new HudiConnectorException(
                        CommonErrorCode.OPERATION_NOT_SUPPORTED,
                        "Unsupported operation type: " + opType);
        }
        batchCount = 0;
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new HudiConnectorException(
                    CommonErrorCode.WRITE_SEATUNNEL_ROW_ERROR,
                    "Writing records to Hudi failed.",
                    flushException);
        }
    }
}
