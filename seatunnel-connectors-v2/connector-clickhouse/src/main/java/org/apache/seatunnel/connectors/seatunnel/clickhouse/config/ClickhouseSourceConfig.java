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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;

import org.apache.commons.lang3.StringUtils;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@Builder(builderClassName = "Builder")
@Slf4j
public class ClickhouseSourceConfig implements Serializable {

    private static final long serialVersionUID = -5139627460951339176L;

    private String host;
    private String username;
    private String password;
    private String tablePath;
    private String filterQuery;
    private List<String> partitionList;
    private int batchSize;
    private int splitSize;
    private String sql;
    private Map<String, String> clickhouseConfig;
    private String serverTimeZone;
    private boolean isSqlStrategyRead;

    public static ClickhouseSourceConfig of(ReadonlyConfig config) {
        if (!config.getOptional(ClickhouseBaseOptions.TABLE_PATH).isPresent()
                && !config.getOptional(ClickhouseSourceOptions.SQL).isPresent()) {
            throw new IllegalArgumentException(
                    "`table_path` and `sql` parameter cannot be both empty.");
        }

        ClickhouseSourceConfig.Builder builder = ClickhouseSourceConfig.builder();
        builder.host(config.get(ClickhouseBaseOptions.HOST));
        builder.username(config.get(ClickhouseBaseOptions.USERNAME));
        builder.password(config.get(ClickhouseBaseOptions.PASSWORD));
        builder.tablePath(config.get(ClickhouseBaseOptions.TABLE_PATH));
        builder.filterQuery(config.get(ClickhouseSourceOptions.CLICKHOUSE_FILTER_QUERY));
        builder.partitionList(config.get(ClickhouseSourceOptions.CLICKHOUSE_PARTITION_LIST));
        builder.batchSize(config.get(ClickhouseSourceOptions.CLICKHOUSE_BATCH_SIZE));
        builder.splitSize(config.get(ClickhouseSourceOptions.CLICKHOUSE_SPLIT_SIZE));
        builder.sql(config.get(ClickhouseSourceOptions.SQL));
        builder.clickhouseConfig(config.get(ClickhouseBaseOptions.CLICKHOUSE_CONFIG));
        builder.isSqlStrategyRead(config.getOptional(ClickhouseSourceOptions.SQL).isPresent());

        return builder.build();
    }

    public TablePath getTableIdentifier() {
        if (StringUtils.isEmpty(tablePath)) {
            // Extract table identifier from SQL
            return ClickhouseUtil.extractTablePathFromSql(sql);
        }

        return TablePath.of(tablePath);
    }
}
