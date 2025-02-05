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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.catalog.ClickhouseCatalog;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.catalog.ClickhouseCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ReaderOption;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.ShardMetadata;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseTable;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKAggCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSinkState;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;

import com.clickhouse.client.ClickHouseNode;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.BULK_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.CLICKHOUSE_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.CUSTOM_SQL;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.PRIMARY_KEY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SHARDING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SPLIT_MODE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SUPPORT_UPSERT;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.USERNAME;

public class ClickhouseSink
        implements SeaTunnelSink<SeaTunnelRow, ClickhouseSinkState, CKCommitInfo, CKAggCommitInfo>,
                SupportSaveMode {

    private ReaderOption option;
    private CatalogTable catalogTable;

    private ReadonlyConfig readonlyConfig;

    public ClickhouseSink(CatalogTable catalogTable, ReadonlyConfig readonlyConfig) {
        this.catalogTable = catalogTable;
        this.readonlyConfig = readonlyConfig;
    }

    @Override
    public String getPluginName() {
        return "Clickhouse";
    }

    @Override
    public SinkWriter<SeaTunnelRow, CKCommitInfo, ClickhouseSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        List<ClickHouseNode> nodes = ClickhouseUtil.createNodes(readonlyConfig);
        Properties clickhouseProperties = new Properties();
        readonlyConfig
                .get(CLICKHOUSE_CONFIG)
                .forEach((key, value) -> clickhouseProperties.put(key, String.valueOf(value)));

        clickhouseProperties.put("user", readonlyConfig.get(USERNAME));
        clickhouseProperties.put("password", readonlyConfig.get(PASSWORD));
        ClickhouseProxy proxy = new ClickhouseProxy(nodes.get(0));

        Map<String, String> tableSchema = proxy.getClickhouseTableSchema(readonlyConfig.get(TABLE));
        String shardKey = null;
        String shardKeyType = null;
        ClickhouseTable table =
                proxy.getClickhouseTable(
                        proxy.getClickhouseConnection(),
                        readonlyConfig.get(DATABASE),
                        readonlyConfig.get(TABLE));
        if (readonlyConfig.get(SPLIT_MODE)) {
            if (!"Distributed".equals(table.getEngine())) {
                throw new ClickhouseConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "split mode only support table which engine is "
                                + "'Distributed' engine at now");
            }
            if (readonlyConfig.getOptional(SHARDING_KEY).isPresent()) {
                shardKey = readonlyConfig.get(SHARDING_KEY);
                shardKeyType = tableSchema.get(shardKey);
            }
        }
        ShardMetadata metadata =
                new ShardMetadata(
                        shardKey,
                        shardKeyType,
                        table.getSortingKey(),
                        readonlyConfig.get(DATABASE),
                        readonlyConfig.get(TABLE),
                        table.getEngine(),
                        readonlyConfig.get(SPLIT_MODE),
                        new Shard(1, 1, nodes.get(0)),
                        readonlyConfig.get(USERNAME),
                        readonlyConfig.get(PASSWORD));
        proxy.close();
        String[] primaryKeys = null;
        if (readonlyConfig.getOptional(PRIMARY_KEY).isPresent()) {
            String primaryKey = readonlyConfig.get(PRIMARY_KEY);
            if (primaryKey == null || primaryKey.trim().isEmpty()) {
                throw new ClickhouseConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT, "primary_key can not be empty");
            }
            if (shardKey != null && !Objects.equals(primaryKey, shardKey)) {
                throw new ClickhouseConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "sharding_key and primary_key must be consistent to ensure correct processing of cdc events");
            }
            primaryKeys = primaryKey.replaceAll("\\s+", "").split(",");
        }
        boolean supportUpsert = readonlyConfig.get(SUPPORT_UPSERT);
        boolean allowExperimentalLightweightDelete =
                readonlyConfig.get(ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE);

        ReaderOption option =
                ReaderOption.builder()
                        .shardMetadata(metadata)
                        .properties(clickhouseProperties)
                        .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                        .tableEngine(table.getEngine())
                        .tableSchema(tableSchema)
                        .bulkSize(readonlyConfig.get(BULK_SIZE))
                        .primaryKeys(primaryKeys)
                        .supportUpsert(supportUpsert)
                        .allowExperimentalLightweightDelete(allowExperimentalLightweightDelete)
                        .build();
        return new ClickhouseSinkWriter(option, context);
    }

    @Override
    public SinkWriter<SeaTunnelRow, CKCommitInfo, ClickhouseSinkState> restoreWriter(
            SinkWriter.Context context, List<ClickhouseSinkState> states) throws IOException {
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    @Override
    public Optional<Serializer<ClickhouseSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        TablePath tablePath = TablePath.of(readonlyConfig.get(DATABASE), readonlyConfig.get(TABLE));
        ClickhouseCatalog clickhouseCatalog =
                new ClickhouseCatalog(readonlyConfig, ClickhouseCatalogFactory.IDENTIFIER);
        SchemaSaveMode schemaSaveMode = readonlyConfig.get(ClickhouseConfig.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = readonlyConfig.get(ClickhouseConfig.DATA_SAVE_MODE);
        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode,
                        dataSaveMode,
                        clickhouseCatalog,
                        tablePath,
                        catalogTable,
                        readonlyConfig.get(CUSTOM_SQL)));
    }
}
