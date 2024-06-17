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
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseFileCopyMethod;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.FileReaderOption;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.NodePassConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.ShardMetadata;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKFileAggCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKFileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSinkState;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseTable;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;

import com.clickhouse.client.ClickHouseNode;
import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.CLICKHOUSE_LOCAL_PATH;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.COMPATIBLE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.COPY_METHOD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.FILE_FIELDS_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.FILE_TEMP_PATH;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.NODE_FREE_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.NODE_PASS;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.SHARDING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.USERNAME;

@AutoService(Factory.class)
public class ClickhouseFileSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "ClickhouseFile";
    }

    @Override
    public TableSink<SeaTunnelRow, ClickhouseSinkState, CKFileCommitInfo, CKFileAggCommitInfo>
            createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        List<ClickHouseNode> nodes = ClickhouseUtil.createNodes(readonlyConfig);

        ClickhouseProxy proxy = new ClickhouseProxy(nodes.get(0));
        try {
            Map<String, String> tableSchema =
                    proxy.getClickhouseTableSchema(readonlyConfig.get(TABLE));
            ClickhouseTable table =
                    proxy.getClickhouseTable(
                            readonlyConfig.get(DATABASE), readonlyConfig.get(TABLE));
            String shardKey = null;
            String shardKeyType = null;
            if (readonlyConfig.getOptional(SHARDING_KEY).isPresent()) {
                shardKey = readonlyConfig.get(SHARDING_KEY);
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
                    readonlyConfig.get(NODE_PASS).stream()
                            .collect(
                                    Collectors.toMap(
                                            configObject -> configObject.getNodeAddress(),
                                            configObject -> readonlyConfig.get(USERNAME)));
            Map<String, String> nodePassword =
                    readonlyConfig.get(NODE_PASS).stream()
                            .collect(
                                    Collectors.toMap(
                                            NodePassConfig::getNodeAddress,
                                            NodePassConfig::getPassword));

            if (readonlyConfig.get(FILE_FIELDS_DELIMITER).length() != 1) {
                throw new ClickhouseConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        FILE_FIELDS_DELIMITER + " must be a single character");
            }

            FileReaderOption fileReaderOption =
                    new FileReaderOption(
                            shardMetadata,
                            tableSchema,
                            fields,
                            readonlyConfig.get(CLICKHOUSE_LOCAL_PATH),
                            ClickhouseFileCopyMethod.from(
                                    readonlyConfig.get(COPY_METHOD).getName()),
                            nodeUser,
                            readonlyConfig.get(NODE_FREE_PASSWORD),
                            nodePassword,
                            context.getCatalogTable().getSeaTunnelRowType(),
                            readonlyConfig.get(COMPATIBLE_MODE),
                            readonlyConfig.get(FILE_TEMP_PATH),
                            readonlyConfig.get(FILE_FIELDS_DELIMITER));
            return () -> new ClickhouseFileSink(fileReaderOption);
        } finally {
            proxy.close();
        }
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
                        FILE_TEMP_PATH)
                .build();
    }
}
