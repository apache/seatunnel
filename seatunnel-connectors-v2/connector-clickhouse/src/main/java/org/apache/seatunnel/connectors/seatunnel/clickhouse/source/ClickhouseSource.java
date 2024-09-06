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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseCatalogConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSourceState;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.TypeConvertUtil;

import org.apache.commons.collections4.map.HashedMap;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseResponse;
import com.google.auto.service.AutoService;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.CLICKHOUSE_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SERVER_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SQL;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.TABLE_LIST;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.TABLE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.USERNAME;

@AutoService(SeaTunnelSource.class)
public class ClickhouseSource
        implements SeaTunnelSource<SeaTunnelRow, ClickhouseSourceSplit, ClickhouseSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private List<ClickHouseNode> servers;
    private SeaTunnelRowType rowTypeInfo;
    private Map<TablePath, ClickhouseCatalogConfig> tableClickhouseCatalogConfigMap =
            new HashedMap<>();

    private final String defaultTablePath = "default";

    @Override
    public String getPluginName() {
        return "Clickhouse";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        config =
                config.withFallback(
                        ConfigFactory.parseMap(
                                Collections.singletonMap(
                                        SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue())));

        Map<String, String> customConfig =
                CheckConfigUtil.isValidParam(config, CLICKHOUSE_CONFIG.key())
                        ? config.getObject(CLICKHOUSE_CONFIG.key()).entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                                Map.Entry::getKey,
                                                entry -> entry.getValue().unwrapped().toString()))
                        : null;

        servers =
                ClickhouseUtil.createNodes(
                        config.getString(HOST.key()),
                        config.getString(DATABASE.key()),
                        config.getString(SERVER_TIME_ZONE.key()),
                        config.getString(USERNAME.key()),
                        config.getString(PASSWORD.key()),
                        customConfig);

        ClickHouseNode currentServer =
                servers.get(ThreadLocalRandom.current().nextInt(servers.size()));

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);
        String sql = readonlyConfig.getOptional(SQL).orElse(null);

        if (readonlyConfig.getOptional(TABLE_LIST).isPresent()) {
            readonlyConfig.get(TABLE_LIST).stream()
                    .map(ReadonlyConfig::fromMap)
                    .forEach(
                            conf -> {
                                String confSql = conf.getOptional(SQL).get();
                                SeaTunnelRowType clickhouseRowType =
                                        getClickhouseRowType(currentServer, confSql);
                                TablePath tablePath =
                                        TablePath.of(conf.getOptional(TABLE_PATH).orElse(""));
                                CatalogTable catalogTable =
                                        createCatalogTable(clickhouseRowType, tablePath);

                                ClickhouseCatalogConfig clickhouseCatalogConfig =
                                        new ClickhouseCatalogConfig();
                                clickhouseCatalogConfig.setSql(confSql);
                                clickhouseCatalogConfig.setCatalogTable(catalogTable);
                                tableClickhouseCatalogConfigMap.put(
                                        tablePath, clickhouseCatalogConfig);
                            });
        } else {
            SeaTunnelRowType clickhouseRowType = getClickhouseRowType(currentServer, sql);
            CatalogTable catalogTable =
                    CatalogTableUtil.getCatalogTable(defaultTablePath, clickhouseRowType);

            ClickhouseCatalogConfig clickhouseCatalogConfig = new ClickhouseCatalogConfig();
            clickhouseCatalogConfig.setCatalogTable(catalogTable);
            clickhouseCatalogConfig.setSql(sql);
            tableClickhouseCatalogConfigMap.put(
                    TablePath.of(defaultTablePath), clickhouseCatalogConfig);
        }
    }

    private CatalogTable createCatalogTable(SeaTunnelRowType rowType, TablePath tablePath) {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            schemaBuilder.column(
                    PhysicalColumn.of(
                            rowType.getFieldName(i), rowType.getFieldType(i), 0, true, null, null));
        }
        return CatalogTable.of(
                TableIdentifier.of("", tablePath),
                schemaBuilder.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    public SeaTunnelRowType getClickhouseRowType(ClickHouseNode currentServer, String sql) {
        try (ClickHouseClient client = ClickHouseClient.newInstance(currentServer.getProtocol());
                ClickHouseResponse response =
                        client.connect(currentServer)
                                .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                                .query(String.format("SELECT * FROM (%s) s LIMIT 1", sql))
                                .executeAndWait()) {

            int columnSize = response.getColumns().size();
            String[] fieldNames = new String[columnSize];
            SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType[columnSize];

            for (int i = 0; i < columnSize; i++) {
                fieldNames[i] = response.getColumns().get(i).getColumnName();
                seaTunnelDataTypes[i] = TypeConvertUtil.convert(response.getColumns().get(i));
            }

            return new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
        } catch (ClickHouseException e) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, ExceptionUtils.getMessage(e)));
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return tableClickhouseCatalogConfigMap.entrySet().stream()
                .map(conf -> conf.getValue().getCatalogTable())
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, ClickhouseSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new ClickhouseSourceReader(servers, readerContext);
    }

    @Override
    public SourceSplitEnumerator<ClickhouseSourceSplit, ClickhouseSourceState> createEnumerator(
            SourceSplitEnumerator.Context<ClickhouseSourceSplit> enumeratorContext)
            throws Exception {
        return new ClickhouseSourceSplitEnumerator(
                enumeratorContext, tableClickhouseCatalogConfigMap);
    }

    @Override
    public SourceSplitEnumerator<ClickhouseSourceSplit, ClickhouseSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<ClickhouseSourceSplit> enumeratorContext,
            ClickhouseSourceState checkpointState)
            throws Exception {
        return new ClickhouseSourceSplitEnumerator(
                enumeratorContext, tableClickhouseCatalogConfigMap, checkpointState);
    }
}
