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
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ReaderOption;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.ShardMetadata;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKAggCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSinkState;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseTable;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;

import com.clickhouse.client.ClickHouseNode;
import com.google.auto.service.AutoService;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.BULK_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.CLICKHOUSE_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.PRIMARY_KEY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SHARDING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SPLIT_MODE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SUPPORT_UPSERT;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.USERNAME;

@AutoService(Factory.class)
public class ClickhouseSinkFactory implements TableSinkFactory {

    @Override
    public TableSink<SeaTunnelRow, ClickhouseSinkState, CKCommitInfo, CKAggCommitInfo> createSink(
            TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        List<ClickHouseNode> nodes = ClickhouseUtil.createNodes(readonlyConfig);
        Properties clickhouseProperties = new Properties();
        readonlyConfig
                .get(CLICKHOUSE_CONFIG)
                .forEach((key, value) -> clickhouseProperties.put(key, String.valueOf(value)));

        clickhouseProperties.put("user", readonlyConfig.get(USERNAME));
        clickhouseProperties.put("password", readonlyConfig.get(PASSWORD));
        ClickhouseProxy proxy = new ClickhouseProxy(nodes.get(0));
        try {
            Map<String, String> tableSchema =
                    proxy.getClickhouseTableSchema(readonlyConfig.get(TABLE));
            String shardKey = null;
            String shardKeyType = null;
            ClickhouseTable table =
                    proxy.getClickhouseTable(
                            readonlyConfig.get(DATABASE), readonlyConfig.get(TABLE));
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
                if (shardKey != null && !Objects.equals(primaryKey, shardKey)) {
                    throw new ClickhouseConnectorException(
                            CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                            "sharding_key and primary_key must be consistent to ensure correct processing of cdc events");
                }
                primaryKeys = new String[] {primaryKey};
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
            return () -> new ClickhouseSink(option);
        } finally {
            proxy.close();
        }
    }

    @Override
    public String factoryIdentifier() {
        return "Clickhouse";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOST, DATABASE, TABLE)
                .optional(
                        CLICKHOUSE_CONFIG,
                        BULK_SIZE,
                        SPLIT_MODE,
                        SHARDING_KEY,
                        PRIMARY_KEY,
                        SUPPORT_UPSERT,
                        ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE)
                .bundled(USERNAME, PASSWORD)
                .build();
    }
}
