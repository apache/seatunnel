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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseTable;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.DistributedEngine;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.TypeConvertUtil;

import org.apache.commons.lang3.StringUtils;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseResponse;
import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.CLICKHOUSE_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.HOST;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.SERVER_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.TABLE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceOptions.CLICKHOUSE_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceOptions.CLICKHOUSE_FILTER_QUERY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceOptions.CLICKHOUSE_PARTITION_LIST;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceOptions.CLICKHOUSE_PART_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceOptions.SQL;

@AutoService(Factory.class)
public class ClickhouseSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Clickhouse";
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ClickhouseSourceConfig clickhouseSourceConfig =
                ClickhouseSourceConfig.of(context.getOptions());

        String sql = clickhouseSourceConfig.getSql();
        TablePath tablePath = TablePath.of(clickhouseSourceConfig.getTableIdentifier());
        List<ClickHouseNode> nodes =
                ClickhouseUtil.createNodes(
                        clickhouseSourceConfig.getHost(),
                        tablePath.getDatabaseName(),
                        clickhouseSourceConfig.getServerTimeZone(),
                        clickhouseSourceConfig.getUsername(),
                        clickhouseSourceConfig.getPassword(),
                        clickhouseSourceConfig.getClickhouseConfig());

        ClickHouseNode currentServer = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));
        Map<TablePath, ClickhouseSourceTable> clickhouseSourceTables = new HashMap<>();

        try (ClickhouseProxy proxy = new ClickhouseProxy(currentServer);
                ClickHouseResponse response =
                        proxy.getClickhouseConnection()
                                .query(
                                        generateQuerySql(
                                                sql,
                                                tablePath.getDatabaseName(),
                                                tablePath.getTableName()))
                                .executeAndWait()) {
            TableSchema.Builder builder = TableSchema.builder();
            List<ClickHouseColumn> columns = response.getColumns();
            columns.forEach(
                    column -> {
                        PhysicalColumn physicalColumn =
                                PhysicalColumn.of(
                                        column.getColumnName(),
                                        TypeConvertUtil.convert(column),
                                        (long) column.getEstimatedLength(),
                                        column.getScale(),
                                        column.isNullable(),
                                        null,
                                        null);
                        builder.column(physicalColumn);
                    });
            String catalogName = "clickhouse_catalog";
            CatalogTable catalogTable =
                    CatalogTable.of(
                            TableIdentifier.of(catalogName, tablePath.getDatabaseName(), "default"),
                            builder.build(),
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            "",
                            catalogName);

            ClickhouseTable clickhouseTable =
                    proxy.getClickhouseTable(
                            proxy.getClickhouseConnection(),
                            tablePath.getDatabaseName(),
                            tablePath.getTableName());

            List<Shard> clusterShardList =
                    getClusterShardList(clickhouseSourceConfig, nodes, proxy, clickhouseTable);

            ClickhouseSourceTable clickhouseSourceTable =
                    ClickhouseSourceTable.builder()
                            .tablePath(tablePath)
                            .clickhouseTable(clickhouseTable)
                            .originQuery(sql)
                            .filterQuery(clickhouseSourceConfig.getFilterQuery())
                            .partSize(clickhouseSourceConfig.getPartSize())
                            .batchSize(clickhouseSourceConfig.getBatchSize())
                            .partitionList(clickhouseSourceConfig.getPartitionList())
                            .clusterShardList(clusterShardList)
                            .isSqlStrategyRead(clickhouseSourceConfig.isSqlStrategyRead())
                            .build();

            clickhouseSourceTables.put(tablePath, clickhouseSourceTable);

            return () ->
                    (SeaTunnelSource<T, SplitT, StateT>)
                            new ClickhouseSource(
                                    nodes,
                                    catalogTable,
                                    clickhouseSourceTables,
                                    clickhouseSourceConfig);
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            factoryIdentifier(), PluginType.SOURCE, e.getMessage()));
        }
    }

    private String modifySQLToLimit1(String sql) {
        return String.format("SELECT * FROM (%s) s LIMIT 1", sql);
    }

    private String generateQuerySql(String sql, String database, String table) {
        if (StringUtils.isNotEmpty(sql)) {
            return modifySQLToLimit1(sql);
        }

        return String.format("SELECT * FROM %s.%s LIMIT 1", database, table);
    }

    private List<Shard> getClusterShardList(
            ClickhouseSourceConfig clickhouseSourceConfig,
            List<ClickHouseNode> nodes,
            ClickhouseProxy proxy,
            ClickhouseTable clickhouseTable) {
        String localTableEngine;
        List<Shard> clusterShardList;

        if (clickhouseTable.getDistributedEngine() != null) {
            DistributedEngine distributedEngine = clickhouseTable.getDistributedEngine();
            localTableEngine = distributedEngine.getTableEngine();

            clusterShardList =
                    proxy.getClusterShardList(
                            proxy.getClickhouseConnection(),
                            distributedEngine.getClusterName(),
                            distributedEngine.getDatabase(),
                            nodes.get(0).getPort(),
                            clickhouseSourceConfig.getUsername(),
                            clickhouseSourceConfig.getPassword(),
                            nodes.get(0).getOptions());
        } else {
            // if input is local table, generate shard list based on the input nodes
            clusterShardList = buildClusterShardForLocal(nodes);
            localTableEngine = clickhouseTable.getEngine();
        }

        if (StringUtils.isEmpty(clickhouseSourceConfig.getSql())
                && !localTableEngine.contains("MergeTree")) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.QUERY_TABLE_NOT_SUPPORT_NON_MERGE_TREE_TABLE,
                    "Query table mode not support non-MergeTree local table. Please specify sql in configuration");
        }

        return clusterShardList;
    }

    private List<Shard> buildClusterShardForLocal(List<ClickHouseNode> nodes) {
        List<Shard> shards = new ArrayList<>();
        IntStream.range(0, nodes.size())
                .forEach(
                        i -> {
                            ClickHouseNode node = nodes.get(i);
                            Shard shard = new Shard(i, 1, node);
                            shards.add(shard);
                        });

        return shards;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOST, USERNAME, PASSWORD)
                .optional(
                        TABLE_PATH,
                        CLICKHOUSE_CONFIG,
                        SERVER_TIME_ZONE,
                        SQL,
                        CLICKHOUSE_PART_SIZE,
                        CLICKHOUSE_PARTITION_LIST,
                        CLICKHOUSE_BATCH_SIZE,
                        CLICKHOUSE_FILTER_QUERY)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return ClickhouseSource.class;
    }
}
