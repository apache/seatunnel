package org.apache.seatunnel.connectors.seatunnel.hudi.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.apache.hudi.common.model.HoodieTableType;
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

import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.INDEX_CLASS_NAME;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.INDEX_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.PARTITION_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.RECORD_BYTE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.RECORD_KEY_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.TABLE_DFS_PATH;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.TABLE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.TABLE_TYPE;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@Slf4j
public class HudiTableConfig implements Serializable {

    @Tolerate
    public HudiTableConfig() {}

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("table_dfs_path")
    private String tableDfsPath;

    @JsonProperty("table_type")
    private HoodieTableType tableType;

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

    public static List<HudiTableConfig> of(ReadonlyConfig connectorConfig) {
        List<HudiTableConfig> tableList;
        if (connectorConfig.getOptional(HudiOptions.TABLE_LIST).isPresent()) {
            tableList = connectorConfig.get(HudiOptions.TABLE_LIST);
        } else {
            HudiTableConfig hudiTableConfig =
                    HudiTableConfig.builder()
                            .tableName(connectorConfig.get(TABLE_NAME))
                            .tableDfsPath(connectorConfig.get(TABLE_DFS_PATH))
                            .tableType(connectorConfig.get(TABLE_TYPE))
                            .recordKeyFields(connectorConfig.get(RECORD_KEY_FIELDS))
                            .partitionFields(connectorConfig.get(PARTITION_FIELDS))
                            .indexType(connectorConfig.get(INDEX_TYPE))
                            .indexClassName(connectorConfig.get(INDEX_CLASS_NAME))
                            .recordByteSize(connectorConfig.get(RECORD_BYTE_SIZE))
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
            Set<String> tablePathSet =
                    tableList.stream()
                            .map(HudiTableConfig::getTableDfsPath)
                            .collect(Collectors.toSet());
            if (tablePathSet.size() < tableList.size() - 1) {
                throw new IllegalArgumentException(
                        "Please configure unique `table_dfs_path`, not allow null/duplicate table path: "
                                + tablePathSet);
            }
        }
        for (HudiTableConfig hudiTableConfig : tableList) {
            if (Objects.isNull(hudiTableConfig.getTableName())) {
                throw new IllegalArgumentException(
                        "Please configure `table_name`, not allow null table name in config.");
            }
            if (Objects.isNull(hudiTableConfig.getTableDfsPath())) {
                throw new IllegalArgumentException(
                        "Please configure `table_type`, not allow null table dfs path in config.");
            }
            if (Objects.isNull(hudiTableConfig.getIndexType())) {
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
        }
        return tableList;
    }
}
