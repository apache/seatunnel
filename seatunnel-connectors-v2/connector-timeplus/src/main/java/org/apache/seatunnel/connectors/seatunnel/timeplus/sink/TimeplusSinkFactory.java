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

package org.apache.seatunnel.connectors.seatunnel.timeplus.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.timeplus.config.ReaderOption;
import org.apache.seatunnel.connectors.seatunnel.timeplus.exception.TimeplusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.timeplus.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.timeplus.shard.ShardMetadata;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.client.TimeplusProxy;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.client.TimeplusSink;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.file.TimeplusTable;
import org.apache.seatunnel.connectors.seatunnel.timeplus.util.TimeplusUtil;

import com.google.auto.service.AutoService;
import com.timeplus.proton.client.ProtonNode;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.BULK_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.PRIMARY_KEY;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SERVER_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SHARDING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SPLIT_MODE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SUPPORT_UPSERT;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.TIMEPLUS_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.USERNAME;

@AutoService(Factory.class)
public class TimeplusSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Timeplus";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOST, DATABASE, TABLE)
                .optional(
                        TIMEPLUS_CONFIG,
                        BULK_SIZE,
                        SPLIT_MODE,
                        SHARDING_KEY,
                        PRIMARY_KEY,
                        SUPPORT_UPSERT,
                        ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE)
                .bundled(USERNAME, PASSWORD)
                .build();
    }

    /*
        public void prepare(Config config) throws PrepareFailException {
            Map<String, Object> defaultConfig =
                    ImmutableMap.<String, Object>builder()
                            .put(HOST.key(), HOST.defaultValue())
                            .put(DATABASE.key(), DATABASE.defaultValue())
                            .put(USERNAME.key(), USERNAME.defaultValue())
                            .put(PASSWORD.key(), PASSWORD.defaultValue())
                            .put(TABLE.key(), TABLE.defaultValue())
                            .put(BULK_SIZE.key(), BULK_SIZE.defaultValue())
                            .put(SPLIT_MODE.key(), SPLIT_MODE.defaultValue())
                            .put(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue())
                            .build();

            config = config.withFallback(ConfigFactory.parseMap(defaultConfig));

            CheckResult result = CheckConfigUtil.checkAllExists(config, HOST.key());

            boolean isCredential = config.hasPath(USERNAME.key()) || config.hasPath(PASSWORD.key());

            if (isCredential) {
                result = CheckConfigUtil.checkAllExists(config, USERNAME.key(), PASSWORD.key());
            }

            if (!result.isSuccess()) {
                throw new TimeplusConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format(
                                "PluginName: %s, PluginType: %s, Message: %s",
                                getPluginName(), PluginType.SINK, result.getMsg()));
            }

            List<ProtonNode> nodes;
            if (!isCredential) {
                nodes =
                        TimeplusUtil.createNodes(
                                config.getString(HOST.key()),
                                config.getString(DATABASE.key()),
                                config.getString(SERVER_TIME_ZONE.key()),
                                null,
                                null,
                                null);
            } else {
                nodes =
                        TimeplusUtil.createNodes(
                                config.getString(HOST.key()),
                                config.getString(DATABASE.key()),
                                config.getString(SERVER_TIME_ZONE.key()),
                                config.getString(USERNAME.key()),
                                config.getString(PASSWORD.key()),
                                null);
            }

            Properties tpProperties = new Properties();
            if (CheckConfigUtil.isValidParam(config, TIMEPLUS_CONFIG.key())) {
                config.getObject(TIMEPLUS_CONFIG.key())
                        .forEach(
                                (key, value) ->
                                        tpProperties.put(key, String.valueOf(value.unwrapped())));
            }

            if (isCredential) {
                tpProperties.put("user", config.getString(USERNAME.key()));
                tpProperties.put("password", config.getString(PASSWORD.key()));
            }

            TimeplusProxy proxy = new TimeplusProxy(nodes.get(0));
            // TODO: there could be no table setting to sync all tables
            Map<String, String> tableSchema =
                    proxy.getTimeplusTableSchema(config.getString(TABLE.key()));
            String shardKey = null;
            String shardKeyType = null;
            TimeplusTable table =
                    proxy.getTimeplusTable(
                            config.getString(DATABASE.key()), config.getString(TABLE.key()));
            if (config.getBoolean(SPLIT_MODE.key())) {
                if (!"Distributed".equals(table.getEngine())) {
                    throw new TimeplusConnectorException(
                            CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                            "split mode only support table which engine is "
                                    + "'Distributed' engine at now");
                }
                if (config.hasPath(SHARDING_KEY.key())) {
                    shardKey = config.getString(SHARDING_KEY.key());
                    shardKeyType = tableSchema.get(shardKey);
                }
            }
            ShardMetadata metadata;

            if (isCredential) {
                metadata =
                        new ShardMetadata(
                                shardKey,
                                shardKeyType,
                                table.getSortingKey(),
                                config.getString(DATABASE.key()),
                                config.getString(TABLE.key()),
                                table.getEngine(),
                                config.getBoolean(SPLIT_MODE.key()),
                                new Shard(1, 1, nodes.get(0)),
                                config.getString(USERNAME.key()),
                                config.getString(PASSWORD.key()));
            } else {
                metadata =
                        new ShardMetadata(
                                shardKey,
                                shardKeyType,
                                table.getSortingKey(),
                                config.getString(DATABASE.key()),
                                config.getString(TABLE.key()),
                                table.getEngine(),
                                config.getBoolean(SPLIT_MODE.key()),
                                new Shard(1, 1, nodes.get(0)));
            }

            proxy.close();

            String[] primaryKeys = null;
            if (config.hasPath(PRIMARY_KEY.key())) {
                String primaryKey = config.getString(PRIMARY_KEY.key());
                if (shardKey != null && !Objects.equals(primaryKey, shardKey)) {
                    throw new TimeplusConnectorException(
                            CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                            "sharding_key and primary_key must be consistent to ensure correct processing of cdc events");
                }
                primaryKeys = new String[] {primaryKey};
            }
            boolean supportUpsert = SUPPORT_UPSERT.defaultValue();
            if (config.hasPath(SUPPORT_UPSERT.key())) {
                supportUpsert = config.getBoolean(SUPPORT_UPSERT.key());
            }
            boolean allowExperimentalLightweightDelete =
                    ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE.defaultValue();
            if (config.hasPath(ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE.key())) {
                allowExperimentalLightweightDelete =
                        config.getBoolean(ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE.key());
            }
            this.option =
                    ReaderOption.builder()
                            .shardMetadata(metadata)
                            .properties(tpProperties)
                            .tableEngine(table.getEngine())
                            .tableSchema(tableSchema)
                            .bulkSize(config.getInt(BULK_SIZE.key()))
                            .primaryKeys(primaryKeys)
                            .supportUpsert(supportUpsert)
                            .allowExperimentalLightweightDelete(allowExperimentalLightweightDelete)
                            .build();
        }
    */
    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        List<ProtonNode> nodes;
        Properties tpProperties = new Properties();
        ShardMetadata metadata;

        boolean isCredential = config.getOptional(USERNAME).isPresent();

        if (config.getOptional(TIMEPLUS_CONFIG).isPresent()) {
            tpProperties.putAll(config.get(TIMEPLUS_CONFIG));
        }

        if (isCredential) {
            nodes =
                    TimeplusUtil.createNodes(
                            config.get(HOST),
                            config.get(DATABASE),
                            config.get(SERVER_TIME_ZONE),
                            config.get(USERNAME),
                            config.get(PASSWORD),
                            null);

            tpProperties.put("user", config.get(USERNAME));
            tpProperties.put("password", config.get(PASSWORD));
        } else {
            nodes =
                    TimeplusUtil.createNodes(
                            config.get(HOST),
                            config.get(DATABASE),
                            config.get(SERVER_TIME_ZONE),
                            null,
                            null,
                            null);
        }

        TimeplusProxy proxy = new TimeplusProxy(nodes.get(0));
        Map<String, String> tableSchema = proxy.getTimeplusTableSchema(config.get(TABLE));
        String shardKey = null;
        String shardKeyType = null;
        TimeplusTable table = proxy.getTimeplusTable(config.get(DATABASE), config.get(TABLE));
        if (config.get(SPLIT_MODE)) {
            if (!"Distributed".equals(table.getEngine())) {
                throw new TimeplusConnectorException(
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
                throw new TimeplusConnectorException(
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
                        .properties(tpProperties)
                        .tableEngine(table.getEngine())
                        .tableSchema(tableSchema)
                        .bulkSize(config.get(BULK_SIZE))
                        .primaryKeys(primaryKeys)
                        .supportUpsert(supportUpsert)
                        .allowExperimentalLightweightDelete(allowExperimentalLightweightDelete)
                        .seaTunnelRowType(context.getCatalogTable().getSeaTunnelRowType())
                        .build();

        return () -> new TimeplusSink(readerOption);
    }
}
