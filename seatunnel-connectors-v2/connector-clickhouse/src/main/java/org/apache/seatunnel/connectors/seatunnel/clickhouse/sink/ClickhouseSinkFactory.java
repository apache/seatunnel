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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ReaderOption;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.ShardMetadata;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.ClickhouseProxy;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.ClickhouseSink;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseTable;
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
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SERVER_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SHARDING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SPLIT_MODE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SUPPORT_UPSERT;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.USERNAME;

@AutoService(Factory.class)
public class ClickhouseSinkFactory implements TableSinkFactory {
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

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        List<ClickHouseNode> nodes;
        Properties clickhouseProperties = new Properties();
        ShardMetadata metadata;

        boolean isCredential = config.getOptional(USERNAME).isPresent();

        if (config.getOptional(CLICKHOUSE_CONFIG).isPresent()) {
            clickhouseProperties.putAll(config.get(CLICKHOUSE_CONFIG));
        }

        if (isCredential) {
            nodes =
                    ClickhouseUtil.createNodes(
                            config.get(HOST),
                            config.get(DATABASE),
                            config.get(SERVER_TIME_ZONE),
                            config.get(USERNAME),
                            config.get(PASSWORD));

            clickhouseProperties.put("user", config.get(USERNAME));
            clickhouseProperties.put("password", config.get(PASSWORD));
        } else {
            nodes =
                    ClickhouseUtil.createNodes(
                            config.get(HOST),
                            config.get(DATABASE),
                            config.get(SERVER_TIME_ZONE),
                            null,
                            null);
        }

        ClickhouseProxy proxy = new ClickhouseProxy(nodes.get(0));
        Map<String, String> tableSchema = proxy.getClickhouseTableSchema(config.get(TABLE));
        String shardKey = null;
        String shardKeyType = null;
        ClickhouseTable table = proxy.getClickhouseTable(config.get(DATABASE), config.get(TABLE));
        if (config.get(SPLIT_MODE)) {
            if (!"Distributed".equals(table.getEngine())) {
                throw new ClickhouseConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "split mode only support table which engine is "
                                + "'Distributed' engine at now");
            }
            if (config.getOptional(SHARDING_KEY).isPresent()) {
                shardKey = config.get(SHARDING_KEY);
                shardKeyType = tableSchema.get(shardKey);
            }
        }

        if (isCredential) {
            metadata =
                    new ShardMetadata(
                            shardKey,
                            shardKeyType,
                            table.getSortingKey(),
                            config.get(DATABASE),
                            config.get(TABLE),
                            table.getEngine(),
                            config.get(SPLIT_MODE),
                            new Shard(1, 1, nodes.get(0)),
                            config.get(USERNAME),
                            config.get(PASSWORD));
        } else {
            metadata =
                    new ShardMetadata(
                            shardKey,
                            shardKeyType,
                            table.getSortingKey(),
                            config.get(DATABASE),
                            config.get(TABLE),
                            table.getEngine(),
                            config.get(SPLIT_MODE),
                            new Shard(1, 1, nodes.get(0)));
        }

        proxy.close();

        String[] primaryKeys = null;
        if (config.getOptional(PRIMARY_KEY).isPresent()) {
            String primaryKey = config.get(PRIMARY_KEY);
            if (shardKey != null && !Objects.equals(primaryKey, shardKey)) {
                throw new ClickhouseConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "sharding_key and primary_key must be consistent to ensure correct processing of cdc events");
            }
            primaryKeys = new String[] {primaryKey};
        }
        boolean supportUpsert = config.get(SUPPORT_UPSERT);
        boolean allowExperimentalLightweightDelete =
                config.get(ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE);
        ReaderOption readerOption =
                ReaderOption.builder()
                        .shardMetadata(metadata)
                        .properties(clickhouseProperties)
                        .tableEngine(table.getEngine())
                        .tableSchema(tableSchema)
                        .bulkSize(config.get(BULK_SIZE))
                        .primaryKeys(primaryKeys)
                        .supportUpsert(supportUpsert)
                        .allowExperimentalLightweightDelete(allowExperimentalLightweightDelete)
                        .seaTunnelRowType(context.getCatalogTable().getSeaTunnelRowType())
                        .build();

        return () -> new ClickhouseSink(readerOption);
    }
}
