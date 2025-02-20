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

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseFileCopyMethod;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.FileReaderOption;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.NodePassConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.ShardMetadata;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;

import com.clickhouse.client.ClickHouseNode;
import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.HOST;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.SERVER_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseFileSinkOptions.CLICKHOUSE_LOCAL_PATH;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseFileSinkOptions.COMPATIBLE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseFileSinkOptions.COPY_METHOD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseFileSinkOptions.FILE_FIELDS_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseFileSinkOptions.FILE_TEMP_PATH;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseFileSinkOptions.KEY_PATH;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseFileSinkOptions.NODE_ADDRESS;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseFileSinkOptions.NODE_FREE_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseFileSinkOptions.NODE_PASS;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.SHARDING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.TABLE;

@AutoService(Factory.class)
public class ClickhouseFileSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "ClickhouseFile";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOST, TABLE, DATABASE, USERNAME, PASSWORD, CLICKHOUSE_LOCAL_PATH)
                .optional(
                        COPY_METHOD,
                        SHARDING_KEY,
                        NODE_FREE_PASSWORD,
                        NODE_PASS,
                        COMPATIBLE_MODE,
                        FILE_FIELDS_DELIMITER,
                        FILE_TEMP_PATH,
                        KEY_PATH,
                        SERVER_TIME_ZONE)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();

        List<ClickHouseNode> nodes =
                ClickhouseUtil.createNodes(
                        readonlyConfig.get(HOST),
                        readonlyConfig.get(DATABASE),
                        readonlyConfig.get(SERVER_TIME_ZONE),
                        readonlyConfig.get(USERNAME),
                        readonlyConfig.get(PASSWORD),
                        null);

        ClickhouseProxy proxy = new ClickhouseProxy(nodes.get(0));
        Map<String, String> tableSchema = proxy.getClickhouseTableSchema(readonlyConfig.get(TABLE));
        ClickhouseTable table =
                proxy.getClickhouseTable(
                        proxy.getClickhouseConnection(),
                        readonlyConfig.get(DATABASE),
                        readonlyConfig.get(TABLE));
        String shardKey = null;
        String shardKeyType = null;
        if (readonlyConfig.getOptional(SHARDING_KEY).isPresent()) {
            shardKey = readonlyConfig.getOptional(SHARDING_KEY).get();
            shardKeyType = tableSchema.get(shardKey);
        }

        ShardMetadata shardMetadata =
                new ShardMetadata(
                        shardKey,
                        shardKeyType,
                        readonlyConfig.get(DATABASE),
                        readonlyConfig.get(TABLE),
                        table.getEngine(),
                        true,
                        new Shard(1, 1, nodes.get(0)),
                        readonlyConfig.get(USERNAME),
                        readonlyConfig.get(PASSWORD));
        List<String> fields = new ArrayList<>(tableSchema.keySet());

        Map<String, String> nodeUser =
                readonlyConfig.toConfig().getObjectList(NODE_PASS.key()).stream()
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
                readonlyConfig.get(NODE_PASS).stream()
                        .collect(
                                Collectors.toMap(
                                        NodePassConfig::getNodeAddress,
                                        NodePassConfig::getPassword));

        proxy.close();

        if (readonlyConfig.get(FILE_FIELDS_DELIMITER).length() != 1) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    FILE_FIELDS_DELIMITER.key() + " must be a single character");
        }
        FileReaderOption readerOption =
                new FileReaderOption(
                        shardMetadata,
                        tableSchema,
                        fields,
                        readonlyConfig.get(CLICKHOUSE_LOCAL_PATH),
                        ClickhouseFileCopyMethod.from(readonlyConfig.get(COPY_METHOD).getName()),
                        nodeUser,
                        readonlyConfig.get(NODE_FREE_PASSWORD),
                        nodePassword,
                        readonlyConfig.get(COMPATIBLE_MODE),
                        readonlyConfig.get(FILE_TEMP_PATH),
                        readonlyConfig.get(FILE_FIELDS_DELIMITER),
                        readonlyConfig.get(KEY_PATH));

        readerOption.setSeaTunnelRowType(catalogTable.getSeaTunnelRowType());
        return () -> new ClickhouseFileSink(readerOption);
    }
}
