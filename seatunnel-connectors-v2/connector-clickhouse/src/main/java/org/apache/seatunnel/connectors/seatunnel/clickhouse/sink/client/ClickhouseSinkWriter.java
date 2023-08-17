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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ReaderOption;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.executor.JdbcBatchStatementExecutorBuilder;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSinkState;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.tool.IntHolder;

import org.apache.commons.lang3.StringUtils;

import com.clickhouse.jdbc.internal.ClickHouseConnectionImpl;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

@Slf4j
public class ClickhouseSinkWriter
        implements SinkWriter<SeaTunnelRow, CKCommitInfo, ClickhouseSinkState> {

    private final Context context;
    private final ReaderOption option;
    private final ShardRouter shardRouter;
    private final transient ClickhouseProxy proxy;
    private final Map<Shard, ClickhouseBatchStatement> statementMap;

    ClickhouseSinkWriter(ReaderOption option, Context context) {
        this.option = option;
        this.context = context;

        this.proxy = new ClickhouseProxy(option.getShardMetadata().getDefaultShard().getNode());
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
        ClickhouseBatchStatement statement = statementMap.get(shardRouter.getShard(shardKey));
        JdbcBatchStatementExecutor clickHouseStatement = statement.getJdbcBatchStatementExecutor();
        IntHolder sizeHolder = statement.getIntHolder();
        // add into batch
        addIntoBatch(element, clickHouseStatement);
        sizeHolder.setValue(sizeHolder.getValue() + 1);
        // flush batch
        if (sizeHolder.getValue() >= option.getBulkSize()) {
            flush(clickHouseStatement);
            sizeHolder.setValue(0);
        }
    }

    @Override
    public Optional<CKCommitInfo> prepareCommit() throws IOException {
        flush();
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        this.proxy.close();
        flush();
    }

    private void addIntoBatch(SeaTunnelRow row, JdbcBatchStatementExecutor clickHouseStatement) {
        try {
            clickHouseStatement.addToBatch(row);
        } catch (SQLException e) {
            throw new ClickhouseConnectorException(
                    CommonErrorCode.SQL_OPERATION_FAILED, "Add row data into batch error", e);
        }
    }

    private void flush(JdbcBatchStatementExecutor clickHouseStatement) {
        try {
            clickHouseStatement.executeBatch();
        } catch (Exception e) {
            throw new ClickhouseConnectorException(
                    CommonErrorCode.FLUSH_DATA_FAILED,
                    "Clickhouse execute batch statement error",
                    e);
        }
    }

    private void flush() {
        for (ClickhouseBatchStatement batchStatement : statementMap.values()) {
            try (ClickHouseConnectionImpl needClosedConnection =
                            batchStatement.getClickHouseConnection();
                    JdbcBatchStatementExecutor needClosedStatement =
                            batchStatement.getJdbcBatchStatementExecutor()) {
                IntHolder intHolder = batchStatement.getIntHolder();
                if (intHolder.getValue() > 0) {
                    flush(needClosedStatement);
                    intHolder.setValue(0);
                }
            } catch (SQLException e) {
                throw new ClickhouseConnectorException(
                        CommonErrorCode.SQL_OPERATION_FAILED,
                        "Failed to close prepared statement.",
                        e);
            }
        }
    }

    private Map<Shard, ClickhouseBatchStatement> initStatementMap() {
        Map<Shard, ClickhouseBatchStatement> result = new HashMap<>(Common.COLLECTION_SIZE);
        shardRouter
                .getShards()
                .forEach(
                        (weight, s) -> {
                            try {
                                ClickHouseConnectionImpl clickhouseConnection =
                                        new ClickHouseConnectionImpl(
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
                                                .setClickhouseTableSchema(option.getTableSchema())
                                                .setAllowExperimentalLightweightDelete(
                                                        option
                                                                .isAllowExperimentalLightweightDelete())
                                                .setClickhouseServerEnableExperimentalLightweightDelete(
                                                        clickhouseServerEnableExperimentalLightweightDelete(
                                                                clickhouseConnection))
                                                .setSupportUpsert(option.isSupportUpsert())
                                                .build();
                                jdbcBatchStatementExecutor.prepareStatements(clickhouseConnection);
                                IntHolder intHolder = new IntHolder();
                                ClickhouseBatchStatement batchStatement =
                                        new ClickhouseBatchStatement(
                                                clickhouseConnection,
                                                jdbcBatchStatementExecutor,
                                                intHolder);
                                result.put(s, batchStatement);
                            } catch (SQLException e) {
                                throw new ClickhouseConnectorException(
                                        CommonErrorCode.SQL_OPERATION_FAILED,
                                        "Clickhouse prepare statement error: " + e.getMessage(),
                                        e);
                            }
                        });
        return result;
    }

    private static boolean clickhouseServerEnableExperimentalLightweightDelete(
            ClickHouseConnectionImpl clickhouseConnection) {
        String configKey = "allow_experimental_lightweight_delete";
        try (Statement stmt = clickhouseConnection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("SHOW SETTINGS ILIKE '%" + configKey + "%'");
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                if (name.equalsIgnoreCase(configKey)) {
                    return resultSet.getBoolean("value");
                }
            }
            return false;
        } catch (SQLException e) {
            log.warn("Failed to get clickhouse server config: {}", configKey, e);
            return false;
        }
    }
}
