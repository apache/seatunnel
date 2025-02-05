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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableMap;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseFileCopyMethod;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.FileReaderOption;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.ShardMetadata;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKFileAggCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKFileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSinkState;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;

import com.clickhouse.client.ClickHouseNode;
import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.CLICKHOUSE_LOCAL_PATH;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.COMPATIBLE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.COPY_METHOD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.FILE_FIELDS_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.FILE_TEMP_PATH;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.KEY_PATH;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.NODE_ADDRESS;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.NODE_FREE_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.NODE_PASS;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SERVER_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SHARDING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.USERNAME;

@AutoService(SeaTunnelSink.class)
public class ClickhouseFileSink
        implements SeaTunnelSink<
                SeaTunnelRow, ClickhouseSinkState, CKFileCommitInfo, CKFileAggCommitInfo> {

    private FileReaderOption readerOption;

    @Override
    public String getPluginName() {
        return "ClickhouseFile";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult checkResult =
                CheckConfigUtil.checkAllExists(
                        config,
                        HOST.key(),
                        TABLE.key(),
                        DATABASE.key(),
                        USERNAME.key(),
                        PASSWORD.key(),
                        CLICKHOUSE_LOCAL_PATH.key());
        if (!checkResult.isSuccess()) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, checkResult.getMsg()));
        }
        Map<String, Object> defaultConfigs =
                ImmutableMap.<String, Object>builder()
                        .put(COPY_METHOD.key(), COPY_METHOD.defaultValue().getName())
                        .put(NODE_FREE_PASSWORD.key(), NODE_FREE_PASSWORD.defaultValue())
                        .put(COMPATIBLE_MODE.key(), COMPATIBLE_MODE.defaultValue())
                        .put(FILE_TEMP_PATH.key(), FILE_TEMP_PATH.defaultValue())
                        .put(FILE_FIELDS_DELIMITER.key(), FILE_FIELDS_DELIMITER.defaultValue())
                        .put(KEY_PATH.key(), KEY_PATH.defaultValue())
                        .build();

        config = config.withFallback(ConfigFactory.parseMap(defaultConfigs));
        List<ClickHouseNode> nodes =
                ClickhouseUtil.createNodes(
                        config.getString(HOST.key()),
                        config.getString(DATABASE.key()),
                        config.getString(SERVER_TIME_ZONE.key()),
                        config.getString(USERNAME.key()),
                        config.getString(PASSWORD.key()),
                        null);

        ClickhouseProxy proxy = new ClickhouseProxy(nodes.get(0));
        Map<String, String> tableSchema =
                proxy.getClickhouseTableSchema(config.getString(TABLE.key()));
        ClickhouseTable table =
                proxy.getClickhouseTable(
                        proxy.getClickhouseConnection(),
                        config.getString(DATABASE.key()),
                        config.getString(TABLE.key()));
        String shardKey = null;
        String shardKeyType = null;
        if (config.hasPath(SHARDING_KEY.key())) {
            shardKey = config.getString(SHARDING_KEY.key());
            shardKeyType = tableSchema.get(shardKey);
        }
        ShardMetadata shardMetadata =
                new ShardMetadata(
                        shardKey,
                        shardKeyType,
                        config.getString(DATABASE.key()),
                        config.getString(TABLE.key()),
                        table.getEngine(),
                        true,
                        new Shard(1, 1, nodes.get(0)),
                        config.getString(USERNAME.key()),
                        config.getString(PASSWORD.key()));
        List<String> fields = new ArrayList<>(tableSchema.keySet());
        Map<String, String> nodeUser =
                config.getObjectList(NODE_PASS.key()).stream()
                        .collect(
                                Collectors.toMap(
                                        configObject ->
                                                configObject.toConfig().getString(NODE_ADDRESS),
                                        configObject ->
                                                configObject.toConfig().hasPath(USERNAME.key())
                                                        ? configObject
                                                                .toConfig()
                                                                .getString(USERNAME.key())
                                                        : "root"));
        Map<String, String> nodePassword =
                config.getObjectList(NODE_PASS.key()).stream()
                        .collect(
                                Collectors.toMap(
                                        configObject ->
                                                configObject.toConfig().getString(NODE_ADDRESS),
                                        configObject ->
                                                configObject.toConfig().getString(PASSWORD.key())));

        proxy.close();

        if (config.getString(FILE_FIELDS_DELIMITER.key()).length() != 1) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    FILE_FIELDS_DELIMITER.key() + " must be a single character");
        }
        this.readerOption =
                new FileReaderOption(
                        shardMetadata,
                        tableSchema,
                        fields,
                        config.getString(CLICKHOUSE_LOCAL_PATH.key()),
                        ClickhouseFileCopyMethod.from(config.getString(COPY_METHOD.key())),
                        nodeUser,
                        config.getBoolean(NODE_FREE_PASSWORD.key()),
                        nodePassword,
                        config.getBoolean(COMPATIBLE_MODE.key()),
                        config.getString(FILE_TEMP_PATH.key()),
                        config.getString(FILE_FIELDS_DELIMITER.key()),
                        config.getString(KEY_PATH.key()));
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.readerOption.setSeaTunnelRowType(seaTunnelRowType);
    }

    @Override
    public SinkWriter<SeaTunnelRow, CKFileCommitInfo, ClickhouseSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new ClickhouseFileSinkWriter(readerOption, context);
    }

    @Override
    public Optional<Serializer<CKFileCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkAggregatedCommitter<CKFileCommitInfo, CKFileAggCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.of(new ClickhouseFileSinkAggCommitter(this.readerOption));
    }

    @Override
    public Optional<Serializer<CKFileAggCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return SeaTunnelSink.super.getWriteCatalogTable();
    }
}
