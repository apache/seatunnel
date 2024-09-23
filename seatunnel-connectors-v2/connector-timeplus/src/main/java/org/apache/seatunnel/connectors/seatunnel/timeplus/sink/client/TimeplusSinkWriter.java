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

package org.apache.seatunnel.connectors.seatunnel.timeplus.sink.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.timeplus.config.ReaderOption;
import org.apache.seatunnel.connectors.seatunnel.timeplus.exception.TimeplusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.timeplus.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.timeplus.shard.ShardMetadata;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.client.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.client.executor.JdbcBatchStatementExecutorBuilder;
import org.apache.seatunnel.connectors.seatunnel.timeplus.sink.file.TimeplusTable;
import org.apache.seatunnel.connectors.seatunnel.timeplus.state.TPCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.timeplus.state.TimeplusSinkState;
import org.apache.seatunnel.connectors.seatunnel.timeplus.tool.IntHolder;
import org.apache.seatunnel.connectors.seatunnel.timeplus.util.TimeplusUtil;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Strings;
import com.timeplus.proton.client.ProtonNode;
import com.timeplus.proton.jdbc.internal.ProtonConnectionImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SERVER_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SHARDING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.USERNAME;
import static org.icecream.IceCream.ic;

@Slf4j
public class TimeplusSinkWriter
        implements SinkWriter<SeaTunnelRow, TPCommitInfo, TimeplusSinkState>,
                SupportMultiTableSinkWriter<Void> {

    private final ReaderOption option;
    private ShardRouter shardRouter;
    private final transient TimeplusProxy proxy;
    private final Map<Shard, TimeplusBatchStatement> statementMap;

    public TimeplusSinkWriter(ReaderOption option, Context context, ReadonlyConfig config) {
        this.option = option;

        ProtonNode node =
                TimeplusUtil.createNodes(
                                config.get(HOST),
                                config.get(DATABASE),
                                config.get(SERVER_TIME_ZONE),
                                config.get(USERNAME),
                                config.get(PASSWORD),
                                null)
                        .get(0);

        this.proxy = new TimeplusProxy(node);
        TimeplusTable table = proxy.getTimeplusTable(config.get(DATABASE), option.getTableName());
        Map<String, String> tableSchema = proxy.getTimeplusTableSchema(option.getTableName());
        option.setTableSchema(tableSchema);
        String shardKey = config.get(SHARDING_KEY);
        ic("shardKey", shardKey);
        ShardMetadata metadata =
                new ShardMetadata(
                        shardKey,
                        tableSchema.get(shardKey),
                        table.getSortingKey(),
                        config.get(DATABASE),
                        option.getTableName(),
                        "Stream",
                        false,
                        new Shard(1, 1, node));
        option.setShardMetadata(metadata);
        this.shardRouter = new ShardRouter(proxy, option.getShardMetadata());
        this.statementMap = initStatementMap();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {

        Object shardKey = null;
        if (StringUtils.isNotEmpty(this.option.getShardMetadata().getShardKey())) {
            int i =
                    this.option
                            .getSeaTunnelRowType()
                            .indexOf(this.option.getShardMetadata().getShardKey());
            shardKey = element.getField(i);
        }
        TimeplusBatchStatement statement = statementMap.get(shardRouter.getShard(shardKey));
        JdbcBatchStatementExecutor tpStatement = statement.getJdbcBatchStatementExecutor();
        IntHolder sizeHolder = statement.getIntHolder();
        // add into batch
        addIntoBatch(element, tpStatement);
        sizeHolder.setValue(sizeHolder.getValue() + 1);
        // flush batch
        if (sizeHolder.getValue() >= option.getBulkSize()) {
            flush(tpStatement);
            sizeHolder.setValue(0);
        }
    }

    @Override
    public Optional<TPCommitInfo> prepareCommit() throws IOException {
        for (TimeplusBatchStatement batchStatement : statementMap.values()) {
            JdbcBatchStatementExecutor statement = batchStatement.getJdbcBatchStatementExecutor();
            IntHolder intHolder = batchStatement.getIntHolder();
            if (intHolder.getValue() > 0) {
                flush(statement);
                intHolder.setValue(0);
            }
        }
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        this.proxy.close();
        flush();
    }

    private void addIntoBatch(SeaTunnelRow row, JdbcBatchStatementExecutor tpStatement) {
        try {
            tpStatement.addToBatch(row);
        } catch (SQLException e) {
            throw new TimeplusConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "Add row data into batch error",
                    e);
        }
    }

    private void flush(JdbcBatchStatementExecutor tpStatement) {
        try {
            tpStatement.executeBatch();
        } catch (Exception e) {
            throw new TimeplusConnectorException(
                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                    "Timeplus execute batch statement error",
                    e);
        }
    }

    private void flush() {
        for (TimeplusBatchStatement batchStatement : statementMap.values()) {
            try (ProtonConnectionImpl needClosedConnection = batchStatement.getProtonConnection();
                    JdbcBatchStatementExecutor needClosedStatement =
                            batchStatement.getJdbcBatchStatementExecutor()) {
                IntHolder intHolder = batchStatement.getIntHolder();
                if (intHolder.getValue() > 0) {
                    flush(needClosedStatement);
                    intHolder.setValue(0);
                }
            } catch (SQLException e) {
                throw new TimeplusConnectorException(
                        CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                        "Failed to close prepared statement.",
                        e);
            }
        }
    }

    private Map<Shard, TimeplusBatchStatement> initStatementMap() {
        Map<Shard, TimeplusBatchStatement> result = new HashMap<>(Common.COLLECTION_SIZE);
        shardRouter
                .getShards()
                .forEach(
                        (weight, s) -> {
                            try {
                                ProtonConnectionImpl ProtonConnection =
                                        new ProtonConnectionImpl(
                                                s.getJdbcUrl(), this.option.getProperties());

                                String[] orderByKeys = null;
                                if (!Strings.isNullOrEmpty(shardRouter.getSortingKey())) {
                                    orderByKeys =
                                            Stream.of(shardRouter.getSortingKey().split(","))
                                                    .map(key -> StringUtils.trim(key))
                                                    .toArray(value -> new String[value]);
                                }
                                JdbcBatchStatementExecutor jdbcBatchStatementExecutor =
                                        new JdbcBatchStatementExecutorBuilder()
                                                .setTable(shardRouter.getShardTable())
                                                .setTableEngine(shardRouter.getShardTableEngine())
                                                .setRowType(option.getSeaTunnelRowType())
                                                .setPrimaryKeys(option.getPrimaryKeys())
                                                .setOrderByKeys(orderByKeys)
                                                .setTimeplusTableSchema(option.getTableSchema())
                                                .setAllowExperimentalLightweightDelete(
                                                        option
                                                                .isAllowExperimentalLightweightDelete())
                                                .setTimeplusServerEnableExperimentalLightweightDelete(
                                                        tpServerEnableExperimentalLightweightDelete(
                                                                ProtonConnection))
                                                .setSupportUpsert(option.isSupportUpsert())
                                                .build();
                                jdbcBatchStatementExecutor.prepareStatements(ProtonConnection);
                                IntHolder intHolder = new IntHolder();
                                TimeplusBatchStatement batchStatement =
                                        new TimeplusBatchStatement(
                                                ProtonConnection,
                                                jdbcBatchStatementExecutor,
                                                intHolder);
                                result.put(s, batchStatement);
                            } catch (SQLException e) {
                                throw new TimeplusConnectorException(
                                        CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                                        "Timeplus prepare statement error: " + e.getMessage(),
                                        e);
                            }
                        });
        return result;
    }

    private boolean tpServerEnableExperimentalLightweightDelete(
            ProtonConnectionImpl ProtonConnection) {
        if (!option.isAllowExperimentalLightweightDelete()) {
            return false;
        }
        String configKey = "allow_experimental_lightweight_delete";
        try (Statement stmt = ProtonConnection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("SHOW SETTINGS ILIKE '%" + configKey + "%'");
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                if (name.equalsIgnoreCase(configKey)) {
                    return resultSet.getBoolean("value");
                }
            }
            return false;
        } catch (SQLException e) {
            log.warn("Failed to get Timeplus server config: {}", configKey, e);
            return false;
        }
    }
}
