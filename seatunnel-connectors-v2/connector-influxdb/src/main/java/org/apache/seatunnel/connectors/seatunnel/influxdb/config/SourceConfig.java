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

package org.apache.seatunnel.connectors.seatunnel.influxdb.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.influxdb.client.InfluxDBClient;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorException;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Getter
public class SourceConfig extends InfluxDBConfig {

    private static final String QUERY_LIMIT = " limit 1";

    public static final Option<String> SQL =
            Options.key("sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server query sql");

    public static final Option<String> SQL_WHERE =
            Options.key("where")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server query sql where condition");

    public static final Option<String> SPLIT_COLUMN =
            Options.key("split_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb column which is used as split key");

    public static final Option<Integer> PARTITION_NUM =
            Options.key("partition_num")
                    .intType()
                    .defaultValue(0)
                    .withDescription("the influxdb server partition num");

    public static final Option<Long> UPPER_BOUND =
            Options.key("upper_bound")
                    .longType()
                    .noDefaultValue()
                    .withDescription("the influxdb server upper bound");

    public static final Option<Long> LOWER_BOUND =
            Options.key("lower_bound")
                    .longType()
                    .noDefaultValue()
                    .withDescription("the influxdb server lower bound");

    public static final Option<List<Map<String, Object>>> TABLE_CONFIGS =
            Options.key("tables_configs")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "Local file source configs, used to create multiple local file source.");

    public static final String DEFAULT_PARTITIONS = String.valueOf(PARTITION_NUM.defaultValue());
    private String sql;
    private int partitionNum = 0;
    private String splitKey;
    private long lowerBound;
    private long upperBound;

    private List<Integer> columnsIndex;
    private ReadonlyConfig config;

    private TablePath tablePath;

    private CatalogTable catalogTable;

    private SourceConfig(ReadonlyConfig config) {
        super(config);
    }

    public static SourceConfig loadConfig(ReadonlyConfig config) {
        SourceConfig sourceConfig = new SourceConfig(config);
        sourceConfig.config = config;
        sourceConfig.sql = config.get(SQL);

        if (config.getOptional(PARTITION_NUM).isPresent()) {
            sourceConfig.partitionNum = config.get(PARTITION_NUM);
        }

        if (config.getOptional(UPPER_BOUND).isPresent()) {
            sourceConfig.upperBound = config.get(UPPER_BOUND);
        }

        if (config.getOptional(LOWER_BOUND).isPresent()) {
            sourceConfig.lowerBound = config.get(LOWER_BOUND);
        }

        if (config.getOptional(SPLIT_COLUMN).isPresent()) {
            sourceConfig.splitKey = config.get(SPLIT_COLUMN);
        }

        try {
            sourceConfig.columnsIndex =
                    initColumnsIndex(
                            InfluxDBClient.getInfluxDB(sourceConfig), sourceConfig, config);
        } catch (Exception e) {
            throw new InfluxdbConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            "InfluxDB", PluginType.SOURCE, e));
        }
        CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(config);
        sourceConfig.tablePath =
                TablePath.of(sourceConfig.getDatabase(), extractTableName(sourceConfig.sql));
        sourceConfig.catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(catalogTable.getCatalogName(), sourceConfig.tablePath),
                        catalogTable.getTableSchema(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        config.get(TableSchemaOptions.TableIdentifierOptions.COMMENT));

        return sourceConfig;
    }

    static List<Integer> initColumnsIndex(
            InfluxDB influxdb, SourceConfig sourceConfig, ReadonlyConfig config) {
        // query one row to get column info
        String sql = sourceConfig.getSql();
        String query = sql + QUERY_LIMIT;
        // if sql contains tz(), can't be append QUERY_LIMIT at last . see bug #4231
        int start = containTzFunction(sql.toLowerCase());
        if (start > 0) {
            StringBuilder tmpSql = new StringBuilder(sql);
            tmpSql.insert(start - 1, QUERY_LIMIT).append(" ");
            query = tmpSql.toString();
        }

        try {
            QueryResult queryResult = influxdb.query(new Query(query, sourceConfig.getDatabase()));

            List<QueryResult.Series> serieList = queryResult.getResults().get(0).getSeries();
            List<String> fieldNames = new ArrayList<>(serieList.get(0).getColumns());
            SeaTunnelRowType typeInfo =
                    CatalogTableUtil.buildWithConfig(config.toConfig()).getSeaTunnelRowType();

            return Arrays.stream(typeInfo.getFieldNames())
                    .map(fieldNames::indexOf)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new InfluxdbConnectorException(
                    InfluxdbConnectorErrorCode.GET_COLUMN_INDEX_FAILED,
                    "Get column index of query result exception",
                    e);
        }
    }

    private static int containTzFunction(String sql) {
        Pattern pattern = Pattern.compile("tz\\(.*\\)");
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            int start = matcher.start();
            return start;
        }
        return -1;
    }

    public static String extractTableName(String query) {
        String patternString = "(?i)select\\s+.*?\\s+from\\s+([^\\s;]+)";
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(query);

        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }
}
