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

package org.apache.seatunnel.connectors.seatunnel.hudi.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.index.HoodieIndex;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.BATCH_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.CDC_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.INDEX_CLASS_NAME;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.INDEX_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.INSERT_SHUFFLE_PARALLELISM;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.MAX_COMMITS_TO_KEEP;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.MIN_COMMITS_TO_KEEP;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.OP_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.PARTITION_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.RECORD_BYTE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.RECORD_KEY_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.TABLE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.TABLE_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.UPSERT_SHUFFLE_PARALLELISM;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@Slf4j
public class HudiTableConfig implements Serializable {

    @Tolerate
    public HudiTableConfig() {}

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("database")
    private String database;

    @JsonProperty("table_type")
    private HoodieTableType tableType;

    @JsonProperty("op_type")
    private WriteOperationType opType;

    @JsonProperty("record_key_fields")
    private String recordKeyFields;

    @JsonProperty("partition_fields")
    private String partitionFields;

    @JsonProperty("index_type")
    private HoodieIndex.IndexType indexType;

    @JsonProperty("index_class_name")
    private String indexClassName;

    @JsonProperty("record_byte_size")
    private Integer recordByteSize;

    @JsonProperty("batch_size")
    private int batchSize;

    @JsonProperty("batch_interval_ms")
    private int batchIntervalMs;

    @JsonProperty("insert_shuffle_parallelism")
    private int insertShuffleParallelism;

    @JsonProperty("upsert_shuffle_parallelism")
    private int upsertShuffleParallelism;

    @JsonProperty("min_commits_to_keep")
    private int minCommitsToKeep;

    @JsonProperty("max_commits_to_keep")
    private int maxCommitsToKeep;

    @JsonProperty("cdc_enabled")
    private boolean cdcEnabled;

    public static List<HudiTableConfig> of(ReadonlyConfig connectorConfig) {
        List<HudiTableConfig> tableList;
        if (connectorConfig.getOptional(HudiOptions.TABLE_LIST).isPresent()) {
            tableList = connectorConfig.get(HudiOptions.TABLE_LIST);
        } else {
            HudiTableConfig hudiTableConfig =
                    HudiTableConfig.builder()
                            .tableName(connectorConfig.get(TABLE_NAME))
                            .database(connectorConfig.get(DATABASE))
                            .tableType(connectorConfig.get(TABLE_TYPE))
                            .opType(connectorConfig.get(OP_TYPE))
                            .recordKeyFields(connectorConfig.get(RECORD_KEY_FIELDS))
                            .partitionFields(connectorConfig.get(PARTITION_FIELDS))
                            .indexType(connectorConfig.get(INDEX_TYPE))
                            .indexClassName(connectorConfig.get(INDEX_CLASS_NAME))
                            .recordByteSize(connectorConfig.get(RECORD_BYTE_SIZE))
                            .batchIntervalMs(connectorConfig.get(BATCH_INTERVAL_MS))
                            .batchSize(connectorConfig.get(BATCH_SIZE))
                            .insertShuffleParallelism(
                                    connectorConfig.get(INSERT_SHUFFLE_PARALLELISM))
                            .upsertShuffleParallelism(
                                    connectorConfig.get(UPSERT_SHUFFLE_PARALLELISM))
                            .minCommitsToKeep(connectorConfig.get(MIN_COMMITS_TO_KEEP))
                            .maxCommitsToKeep(connectorConfig.get(MAX_COMMITS_TO_KEEP))
                            .cdcEnabled(connectorConfig.get(CDC_ENABLED))
                            .build();
            tableList = Collections.singletonList(hudiTableConfig);
        }
        if (tableList.size() > 1) {
            Set<String> tableNameSet =
                    tableList.stream()
                            .map(HudiTableConfig::getTableName)
                            .collect(Collectors.toSet());
            if (tableNameSet.size() < tableList.size() - 1) {
                throw new IllegalArgumentException(
                        "Please configure unique `table_name`, not allow null/duplicate table name: "
                                + tableNameSet);
            }
        }
        for (HudiTableConfig hudiTableConfig : tableList) {
            if (Objects.isNull(hudiTableConfig.getTableName())) {
                throw new IllegalArgumentException(
                        "Please configure `table_name`, not allow null table name in config.");
            }
            if (Objects.isNull(hudiTableConfig.getDatabase())) {
                log.info(
                        "The hudi table '{}' not set database, will uses 'default' as its database.",
                        hudiTableConfig.getTableName());
                hudiTableConfig.setDatabase(DATABASE.defaultValue());
            }
            if (Objects.isNull(hudiTableConfig.getTableType())) {
                log.info(
                        "The hudi table '{}' not set table type, default uses 'COPY_ON_WRITE'.",
                        hudiTableConfig.getTableName());
                hudiTableConfig.setTableType(HoodieTableType.COPY_ON_WRITE);
            }
            if (Objects.isNull(hudiTableConfig.getIndexType())
                    && Objects.isNull(hudiTableConfig.getIndexClassName())) {
                hudiTableConfig.setIndexType(HoodieIndex.IndexType.BLOOM);
                log.info(
                        "The hudi table '{}' not set index type, default uses 'BLOOM'.",
                        hudiTableConfig.getTableName());
            }
            if (Objects.isNull(hudiTableConfig.getRecordByteSize())) {
                hudiTableConfig.setRecordByteSize(1024);
            }
            if (Objects.isNull(hudiTableConfig.getOpType())) {
                hudiTableConfig.setOpType(OP_TYPE.defaultValue());
            }
            if (hudiTableConfig.getBatchSize() == 0) {
                hudiTableConfig.setBatchSize(BATCH_SIZE.defaultValue());
            }
            if (hudiTableConfig.getBatchIntervalMs() == 0) {
                hudiTableConfig.setBatchIntervalMs(BATCH_INTERVAL_MS.defaultValue());
            }
            if (hudiTableConfig.getInsertShuffleParallelism() == 0) {
                hudiTableConfig.setInsertShuffleParallelism(
                        INSERT_SHUFFLE_PARALLELISM.defaultValue());
            }
            if (hudiTableConfig.getUpsertShuffleParallelism() == 0) {
                hudiTableConfig.setUpsertShuffleParallelism(
                        UPSERT_SHUFFLE_PARALLELISM.defaultValue());
            }
            if (hudiTableConfig.getMinCommitsToKeep() == 0) {
                hudiTableConfig.setMinCommitsToKeep(MIN_COMMITS_TO_KEEP.defaultValue());
            }
            if (hudiTableConfig.getMaxCommitsToKeep() == 0) {
                hudiTableConfig.setMaxCommitsToKeep(MAX_COMMITS_TO_KEEP.defaultValue());
            }
            if (Objects.isNull(hudiTableConfig.getRecordKeyFields())
                    && hudiTableConfig.getOpType() == WriteOperationType.UPSERT) {
                throw new IllegalArgumentException(
                        "Please configure `record_key_fields` of "
                                + hudiTableConfig.getTableName()
                                + ", it is necessary when the `op_type` is 'UPSERT'.");
            }
        }
        return tableList;
    }
}
