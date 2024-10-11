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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ReaderOption;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.ShardMetadata;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseTable;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKAggCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSinkState;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;

import com.clickhouse.client.ClickHouseNode;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

@AutoService(SeaTunnelSink.class)
public class ClickhouseSink
        implements SeaTunnelSink<SeaTunnelRow, ClickhouseSinkState, CKCommitInfo, CKAggCommitInfo> {

    private ReaderOption option;

    @Override
    public String getPluginName() {
        return "Clickhouse";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(config, HOST.key(), DATABASE.key(), TABLE.key());

        boolean isCredential = config.hasPath(USERNAME.key()) || config.hasPath(PASSWORD.key());

        if (isCredential) {
            result = CheckConfigUtil.checkAllExists(config, USERNAME.key(), PASSWORD.key());
        }

        if (!result.isSuccess()) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        Map<String, Object> defaultConfig =
                ImmutableMap.<String, Object>builder()
                        .put(BULK_SIZE.key(), BULK_SIZE.defaultValue())
                        .put(SPLIT_MODE.key(), SPLIT_MODE.defaultValue())
                        .put(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue())
                        .build();

        config = config.withFallback(ConfigFactory.parseMap(defaultConfig));

        List<ClickHouseNode> nodes;
        if (!isCredential) {
            nodes =
                    ClickhouseUtil.createNodes(
                            config.getString(HOST.key()),
                            config.getString(DATABASE.key()),
                            config.getString(SERVER_TIME_ZONE.key()),
                            null,
                            null,
                            null);
        } else {
            nodes =
                    ClickhouseUtil.createNodes(
                            config.getString(HOST.key()),
                            config.getString(DATABASE.key()),
                            config.getString(SERVER_TIME_ZONE.key()),
                            config.getString(USERNAME.key()),
                            config.getString(PASSWORD.key()),
                            null);
        }

        Properties clickhouseProperties = new Properties();
        if (CheckConfigUtil.isValidParam(config, CLICKHOUSE_CONFIG.key())) {
            config.getObject(CLICKHOUSE_CONFIG.key())
                    .forEach(
                            (key, value) ->
                                    clickhouseProperties.put(
                                            key, String.valueOf(value.unwrapped())));
        }

        if (isCredential) {
            clickhouseProperties.put("user", config.getString(USERNAME.key()));
            clickhouseProperties.put("password", config.getString(PASSWORD.key()));
        }

        ClickhouseProxy proxy = new ClickhouseProxy(nodes.get(0));
        Map<String, String> tableSchema =
                proxy.getClickhouseTableSchema(config.getString(TABLE.key()));
        String shardKey = null;
        String shardKeyType = null;
        ClickhouseTable table =
                proxy.getClickhouseTable(
                        config.getString(DATABASE.key()), config.getString(TABLE.key()));
        if (config.getBoolean(SPLIT_MODE.key())) {
            if (!"Distributed".equals(table.getEngine())) {
                throw new ClickhouseConnectorException(
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
                throw new ClickhouseConnectorException(
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
                        .properties(clickhouseProperties)
                        .tableEngine(table.getEngine())
                        .tableSchema(tableSchema)
                        .bulkSize(config.getInt(BULK_SIZE.key()))
                        .primaryKeys(primaryKeys)
                        .supportUpsert(supportUpsert)
                        .allowExperimentalLightweightDelete(allowExperimentalLightweightDelete)
                        .build();
    }

    @Override
    public SinkWriter<SeaTunnelRow, CKCommitInfo, ClickhouseSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
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
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.option.setSeaTunnelRowType(seaTunnelRowType);
    }
}
