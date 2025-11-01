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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.TABLE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceOptions.CLICKHOUSE_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceOptions.CLICKHOUSE_FILTER_QUERY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceOptions.CLICKHOUSE_PARTITION_LIST;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceOptions.CLICKHOUSE_SPLIT_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceOptions.SQL;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceOptions.TABLE_LIST;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClickhouseTableConfig implements Serializable {
    private static final long serialVersionUID = -6133096497433624821L;

    @JsonProperty("table_path")
    private String tablePath;

    @JsonProperty("sql")
    private String sql;

    @JsonProperty("filter_query")
    private String filterQuery;

    @JsonProperty("partition_list")
    private List<String> partitionList;

    @JsonProperty("batch_size")
    private int batchSize;

    @JsonProperty("split_size")
    private int splitSize;

    private boolean isSqlStrategyRead;

    @Tolerate
    public ClickhouseTableConfig() {}

    public static List<ClickhouseTableConfig> of(ReadonlyConfig readonlyConfig) {
        List<ClickhouseTableConfig> tableList;
        if (readonlyConfig.getOptional(TABLE_LIST).isPresent()) {
            tableList = readonlyConfig.get(TABLE_LIST);
        } else {
            ClickhouseTableConfig tableConfig =
                    ClickhouseTableConfig.builder()
                            .tablePath(readonlyConfig.get(TABLE_PATH))
                            .sql(readonlyConfig.get(SQL))
                            .filterQuery(readonlyConfig.get(CLICKHOUSE_FILTER_QUERY))
                            .partitionList(readonlyConfig.get(CLICKHOUSE_PARTITION_LIST))
                            .batchSize(readonlyConfig.get(CLICKHOUSE_BATCH_SIZE))
                            .splitSize(readonlyConfig.get(CLICKHOUSE_SPLIT_SIZE))
                            .build();

            tableList = Collections.singletonList(tableConfig);
        }

        if (tableList == null || tableList.isEmpty()) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.GET_TABLE_LIST_CONFIG_ERROR,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            "Clickhouse", PluginType.SOURCE, "Get table list config error."));
        }

        for (ClickhouseTableConfig tableConfig : tableList) {
            if (StringUtils.isEmpty(tableConfig.getTablePath())
                    && StringUtils.isEmpty(tableConfig.getSql())) {
                throw new IllegalArgumentException(
                        "`table_path` and `sql` parameter cannot be both empty.");
            }

            if (tableConfig.getBatchSize() <= 0) {
                tableConfig.setBatchSize(CLICKHOUSE_BATCH_SIZE.defaultValue());
            }

            if (tableConfig.getSplitSize() <= 0) {
                tableConfig.setSplitSize(CLICKHOUSE_SPLIT_SIZE.defaultValue());
            }

            tableConfig.setSqlStrategyRead(StringUtils.isNotEmpty(tableConfig.getSql()));
        }

        return tableList;
    }

    public TablePath getTableIdentifier() {
        if (StringUtils.isEmpty(tablePath)) {
            // Extract table identifier from SQL
            return ClickhouseUtil.extractTablePathFromSql(sql);
        }

        return TablePath.of(tablePath);
    }
}
