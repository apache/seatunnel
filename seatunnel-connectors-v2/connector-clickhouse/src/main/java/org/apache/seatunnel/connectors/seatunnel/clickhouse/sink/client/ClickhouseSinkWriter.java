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
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject.ArrayInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject.BigDecimalInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject.ClickhouseFieldInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject.DateInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject.DateTimeInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject.DoubleInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject.FloatInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject.IntInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject.LongInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject.MapInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject.StringInjectFunction;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSinkState;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.tool.IntHolder;

import com.clickhouse.jdbc.internal.ClickHouseConnectionImpl;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class ClickhouseSinkWriter implements SinkWriter<SeaTunnelRow, CKCommitInfo, ClickhouseSinkState> {

    private final Context context;
    private final ReaderOption option;
    private final ShardRouter shardRouter;
    private final transient ClickhouseProxy proxy;
    private final String prepareSql;
    private final Map<Shard, ClickhouseBatchStatement> statementMap;
    private final Map<String, ClickhouseFieldInjectFunction> fieldInjectFunctionMap;
    private static final ClickhouseFieldInjectFunction DEFAULT_INJECT_FUNCTION = new StringInjectFunction();

    private static final Pattern NULLABLE = Pattern.compile("Nullable\\((.*)\\)");
    private static final Pattern LOW_CARDINALITY = Pattern.compile("LowCardinality\\((.*)\\)");

    ClickhouseSinkWriter(ReaderOption option, Context context) {
        this.option = option;
        this.context = context;

        this.proxy = new ClickhouseProxy(option.getShardMetadata().getDefaultShard().getNode());
        this.fieldInjectFunctionMap = initFieldInjectFunctionMap();
        this.shardRouter = new ShardRouter(proxy, option.getShardMetadata());
        this.prepareSql = initPrepareSQL();
        this.statementMap = initStatementMap();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {

        Object shardKey = null;
        if (StringUtils.isNotEmpty(this.option.getShardMetadata().getShardKey())) {
            int i = this.option.getSeaTunnelRowType().indexOf(this.option.getShardMetadata().getShardKey());
            shardKey = element.getField(i);
        }
        ClickhouseBatchStatement statement = statementMap.get(shardRouter.getShard(shardKey));
        PreparedStatement clickHouseStatement = statement.getPreparedStatement();
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
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {

    }

    @Override
    public void close() throws IOException {
        this.proxy.close();
        for (ClickhouseBatchStatement batchStatement : statementMap.values()) {
            try (ClickHouseConnectionImpl needClosedConnection = batchStatement.getClickHouseConnection();
                 PreparedStatement needClosedStatement = batchStatement.getPreparedStatement()) {
                IntHolder intHolder = batchStatement.getIntHolder();
                if (intHolder.getValue() > 0) {
                    flush(needClosedStatement);
                    intHolder.setValue(0);
                }
            } catch (SQLException e) {
                throw new ClickhouseConnectorException(CommonErrorCode.SQL_OPERATION_FAILED, "Failed to close prepared statement.", e);
            }
        }
    }

    private void addIntoBatch(SeaTunnelRow row, PreparedStatement clickHouseStatement) {
        try {
            for (int i = 0; i < option.getFields().size(); i++) {
                String fieldName = option.getFields().get(i);
                Object fieldValue = row.getField(option.getSeaTunnelRowType().indexOf(fieldName));
                if (fieldValue == null) {
                    // field does not exist in row
                    // todo: do we need to transform to default value of each type
                    clickHouseStatement.setObject(i + 1, null);
                    continue;
                }
                String fieldType = option.getTableSchema().get(fieldName);
                fieldInjectFunctionMap
                    .getOrDefault(fieldType, DEFAULT_INJECT_FUNCTION)
                    .injectFields(clickHouseStatement, i + 1, fieldValue);
            }
            clickHouseStatement.addBatch();
        } catch (SQLException e) {
            throw new ClickhouseConnectorException(CommonErrorCode.SQL_OPERATION_FAILED, "Add row data into batch error", e);
        }
    }

    private void flush(PreparedStatement clickHouseStatement) {
        try {
            clickHouseStatement.executeBatch();
        } catch (Exception e) {
            throw new ClickhouseConnectorException(CommonErrorCode.FLUSH_DATA_FAILED, "Clickhouse execute batch statement error", e);
        }
    }

    private Map<Shard, ClickhouseBatchStatement> initStatementMap() {
        Map<Shard, ClickhouseBatchStatement> result = new HashMap<>(Common.COLLECTION_SIZE);
        shardRouter.getShards().forEach((weight, s) -> {
            try {
                ClickHouseConnectionImpl clickhouseConnection = new ClickHouseConnectionImpl(s.getJdbcUrl(),
                    this.option.getProperties());
                PreparedStatement preparedStatement = clickhouseConnection.prepareStatement(prepareSql);
                IntHolder intHolder = new IntHolder();
                ClickhouseBatchStatement batchStatement =
                    new ClickhouseBatchStatement(clickhouseConnection, preparedStatement, intHolder);
                result.put(s, batchStatement);
            } catch (SQLException e) {
                throw new ClickhouseConnectorException(CommonErrorCode.SQL_OPERATION_FAILED, "Clickhouse prepare statement error: " + e.getMessage(), e);
            }
        });
        return result;
    }

    private String initPrepareSQL() {
        String[] placeholder = new String[option.getFields().size()];
        Arrays.fill(placeholder, "?");

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
            shardRouter.getShardTable(),
            String.join(",", option.getFields()),
            String.join(",", placeholder));
    }

    private Map<String, ClickhouseFieldInjectFunction> initFieldInjectFunctionMap() {
        Map<String, ClickhouseFieldInjectFunction> result = new HashMap<>(Common.COLLECTION_SIZE);
        List<ClickhouseFieldInjectFunction> clickhouseFieldInjectFunctions;
        ClickhouseFieldInjectFunction defaultFunction = new StringInjectFunction();
        // get field type
        for (String field : this.option.getFields()) {
            clickhouseFieldInjectFunctions = Lists.newArrayList(
                new ArrayInjectFunction(),
                new MapInjectFunction(),
                new BigDecimalInjectFunction(),
                new DateInjectFunction(),
                new DateTimeInjectFunction(),
                new LongInjectFunction(),
                new DoubleInjectFunction(),
                new FloatInjectFunction(),
                new IntInjectFunction(),
                new StringInjectFunction()
            );
            ClickhouseFieldInjectFunction function = defaultFunction;
            String fieldType = this.option.getTableSchema().get(field);
            for (ClickhouseFieldInjectFunction clickhouseFieldInjectFunction : clickhouseFieldInjectFunctions) {
                if (clickhouseFieldInjectFunction.isCurrentFieldType(unwrapCommonPrefix(fieldType))) {
                    function = clickhouseFieldInjectFunction;
                    break;
                }
            }
            result.put(fieldType, function);
        }
        return result;
    }

    private String unwrapCommonPrefix(String fieldType) {
        Matcher nullMatcher = NULLABLE.matcher(fieldType);
        Matcher lowMatcher = LOW_CARDINALITY.matcher(fieldType);
        if (nullMatcher.matches()) {
            return nullMatcher.group(1);
        } else if (lowMatcher.matches()) {
            return lowMatcher.group(1);
        } else {
            return fieldType;
        }
    }

}
