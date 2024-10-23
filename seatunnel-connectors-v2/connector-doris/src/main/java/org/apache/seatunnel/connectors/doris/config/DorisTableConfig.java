package org.apache.seatunnel.connectors.doris.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.apache.commons.lang3.StringUtils;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DATABASE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_BATCH_SIZE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.TABLE;
import static org.apache.seatunnel.connectors.doris.config.DorisSourceOptions.DORIS_EXEC_MEM_LIMIT;
import static org.apache.seatunnel.connectors.doris.config.DorisSourceOptions.DORIS_FILTER_QUERY;
import static org.apache.seatunnel.connectors.doris.config.DorisSourceOptions.DORIS_READ_FIELD;
import static org.apache.seatunnel.connectors.doris.config.DorisSourceOptions.DORIS_TABLET_SIZE;
import static org.apache.seatunnel.connectors.doris.config.DorisSourceOptions.TABLE_LIST;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class DorisTableConfig implements Serializable {

    @JsonProperty("table")
    private String table;

    @JsonProperty("database")
    private String database;

    @JsonProperty("doris.read.field")
    private String readField;

    @JsonProperty("doris.filter.query")
    private String filterQuery;

    @JsonProperty("doris.batch.size")
    private int batchSize;

    @JsonProperty("doris.request.tablet.size")
    private int tabletSize;

    @JsonProperty("doris.exec.mem.limit")
    private long execMemLimit;

    @Tolerate
    public DorisTableConfig() {}

    public static List<DorisTableConfig> of(ReadonlyConfig connectorConfig) {
        List<DorisTableConfig> tableList;
        if (connectorConfig.getOptional(TABLE_LIST).isPresent()) {
            tableList = connectorConfig.get(TABLE_LIST);
        } else {
            DorisTableConfig tableProperty =
                    DorisTableConfig.builder()
                            .table(connectorConfig.get(TABLE))
                            .database(connectorConfig.get(DATABASE))
                            .readField(connectorConfig.get(DORIS_READ_FIELD))
                            .filterQuery(connectorConfig.get(DORIS_FILTER_QUERY))
                            .batchSize(connectorConfig.get(DORIS_BATCH_SIZE))
                            .tabletSize(connectorConfig.get(DORIS_TABLET_SIZE))
                            .execMemLimit(connectorConfig.get(DORIS_EXEC_MEM_LIMIT))
                            .build();
            tableList = Collections.singletonList(tableProperty);
        }

        if (tableList.size() > 1) {
            List<String> tableIds =
                    tableList.stream()
                            .map(DorisTableConfig::getTableIdentifier)
                            .collect(Collectors.toList());
            Set<String> tableIdSet = new HashSet<>(tableIds);
            if (tableIdSet.size() < tableList.size() - 1) {
                throw new IllegalArgumentException(
                        "Please configure unique `database`.`table`, not allow null/duplicate: "
                                + tableIds);
            }
        }

        for (DorisTableConfig dorisTableConfig : tableList) {
            if (StringUtils.isBlank(dorisTableConfig.getDatabase())) {
                throw new IllegalArgumentException(
                        "Please configure `database`, not allow null database in config.");
            }
            if (StringUtils.isBlank(dorisTableConfig.getTable())) {
                throw new IllegalArgumentException(
                        "Please configure `table`, not allow null table in config.");
            }
            if (dorisTableConfig.getBatchSize() <= 0) {
                dorisTableConfig.setBatchSize(DORIS_BATCH_SIZE.defaultValue());
            }
            if (dorisTableConfig.getExecMemLimit() <= 0) {
                dorisTableConfig.setExecMemLimit(DORIS_EXEC_MEM_LIMIT.defaultValue());
            }
            if (dorisTableConfig.getTabletSize() <= 0) {
                dorisTableConfig.setTabletSize(DORIS_TABLET_SIZE.defaultValue());
            }
        }
        return tableList;
    }

    public String getTableIdentifier() {
        return String.format("%s.%s", database, table);
    }
}
