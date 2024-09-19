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

package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Data
@Builder
public class JdbcSourceTableConfig implements Serializable {
    private static final int DEFAULT_PARTITION_NUMBER = 10;

    private String tablePath;

    private String query;

    private String partitionColumn;

    private Integer partitionNumber;

    private BigDecimal partitionStart;

    private BigDecimal partitionEnd;

    @JsonProperty("use_select_count")
    private Boolean useSelectCount;

    @JsonProperty("skip_analyze")
    private Boolean skipAnalyze;

    @Tolerate
    public JdbcSourceTableConfig() {}

    public static List<JdbcSourceTableConfig> of(ReadonlyConfig connectorConfig) {
        List<JdbcSourceTableConfig> tableList;
        if (connectorConfig.getOptional(JdbcSourceOptions.TABLE_LIST).isPresent()) {
            if (connectorConfig.getOptional(JdbcOptions.QUERY).isPresent()
                    || connectorConfig.getOptional(JdbcSourceOptions.TABLE_PATH).isPresent()) {
                throw new IllegalArgumentException(
                        "Please configure either `table_list` or `table_path`/`query`, not both");
            }
            tableList = connectorConfig.get(JdbcSourceOptions.TABLE_LIST);
        } else {
            JdbcSourceTableConfig tableProperty =
                    JdbcSourceTableConfig.builder()
                            .tablePath(connectorConfig.get(JdbcSourceOptions.TABLE_PATH))
                            .query(connectorConfig.get(JdbcOptions.QUERY))
                            .partitionColumn(connectorConfig.get(JdbcOptions.PARTITION_COLUMN))
                            .partitionNumber(connectorConfig.get(JdbcOptions.PARTITION_NUM))
                            .partitionStart(connectorConfig.get(JdbcOptions.PARTITION_LOWER_BOUND))
                            .partitionEnd(connectorConfig.get(JdbcOptions.PARTITION_UPPER_BOUND))
                            .build();
            tableList = Collections.singletonList(tableProperty);
        }

        tableList.forEach(
                tableConfig -> {
                    if (tableConfig.getPartitionNumber() == null) {
                        tableConfig.setPartitionNumber(DEFAULT_PARTITION_NUMBER);
                    }
                    tableConfig.setUseSelectCount(
                            connectorConfig.get(JdbcSourceOptions.USE_SELECT_COUNT));
                    tableConfig.setSkipAnalyze(connectorConfig.get(JdbcSourceOptions.SKIP_ANALYZE));
                });

        if (tableList.size() > 1) {
            List<String> tableIds =
                    tableList.stream()
                            .map(JdbcSourceTableConfig::getTablePath)
                            .collect(Collectors.toList());
            Set<String> tableIdSet = new HashSet<>(tableIds);
            if (tableIdSet.size() < tableList.size() - 1) {
                throw new IllegalArgumentException(
                        "Please configure unique `table_path`, not allow null/duplicate table path: "
                                + tableIds);
            }
        }
        return tableList;
    }
}
