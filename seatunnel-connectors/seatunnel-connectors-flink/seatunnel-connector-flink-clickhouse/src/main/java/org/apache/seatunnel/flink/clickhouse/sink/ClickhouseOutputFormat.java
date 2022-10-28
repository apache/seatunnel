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

package org.apache.seatunnel.flink.clickhouse.sink;

import static org.apache.seatunnel.flink.clickhouse.ConfigKey.BULK_SIZE;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.RETRY;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.RETRY_CODES;

import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.flink.clickhouse.pojo.IntHolder;
import org.apache.seatunnel.flink.clickhouse.pojo.Shard;
import org.apache.seatunnel.flink.clickhouse.pojo.ShardMetadata;
import org.apache.seatunnel.flink.clickhouse.sink.client.ClickhouseBatchStatement;
import org.apache.seatunnel.flink.clickhouse.sink.client.ClickhouseClient;
import org.apache.seatunnel.flink.clickhouse.sink.client.ShardRouter;
import org.apache.seatunnel.flink.clickhouse.sink.inject.ArrayInjectFunction;
import org.apache.seatunnel.flink.clickhouse.sink.inject.BigDecimalInjectFunction;
import org.apache.seatunnel.flink.clickhouse.sink.inject.ClickhouseFieldInjectFunction;
import org.apache.seatunnel.flink.clickhouse.sink.inject.DateInjectFunction;
import org.apache.seatunnel.flink.clickhouse.sink.inject.DateTimeInjectFunction;
import org.apache.seatunnel.flink.clickhouse.sink.inject.DoubleInjectFunction;
import org.apache.seatunnel.flink.clickhouse.sink.inject.FloatInjectFunction;
import org.apache.seatunnel.flink.clickhouse.sink.inject.IntInjectFunction;
import org.apache.seatunnel.flink.clickhouse.sink.inject.LongInjectFunction;
import org.apache.seatunnel.flink.clickhouse.sink.inject.StringInjectFunction;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.ClickHousePreparedStatementImpl;
import ru.yandex.clickhouse.ClickHouseStatement;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("magicnumber")
public class ClickhouseOutputFormat extends RichOutputFormat<Row> {

    private static final long serialVersionUID = -1L;

    private final Config config;
    private final List<String> fields;
    private final Map<String, String> tableSchema;
    private final ShardMetadata shardMetadata;
    private final int batchSize;

    // The below fields should be created by open function.
    private transient RetryUtils.RetryMaterial retryMaterial;
    private transient ShardRouter shardRouter;
    private transient ClickhouseClient clickhouseClient;
    private transient String prepareSql;
    private transient Map<Shard, ClickhouseBatchStatement> statementMap;
    private transient Map<String, ClickhouseFieldInjectFunction> fieldInjectFunctionMap;
    private static final ClickhouseFieldInjectFunction DEFAULT_INJECT_FUNCTION = new StringInjectFunction();

    public ClickhouseOutputFormat(Config config,
                                  ShardMetadata shardMetadata,
                                  List<String> fields,
                                  Map<String, String> tableSchema) {
        this.config = config;
        this.shardMetadata = shardMetadata;
        this.fields = fields;
        this.tableSchema = tableSchema;
        this.batchSize = config.getInt(BULK_SIZE);
    }

    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public void open(int taskNumber, int numTasks) {
        List<Integer> retryCodes = config.getIntList(RETRY_CODES);
        retryMaterial = new RetryUtils.RetryMaterial(config.getInt(RETRY), true, exception -> {
            if (exception instanceof SQLException) {
                SQLException sqlException = (SQLException) exception;
                return retryCodes.contains(sqlException.getErrorCode());
            }
            return false;
        });
        clickhouseClient = new ClickhouseClient(config);
        fieldInjectFunctionMap = initFieldInjectFunctionMap();
        shardRouter = new ShardRouter(clickhouseClient, shardMetadata);
        prepareSql = initPrepareSQL();
        statementMap = initStatementMap();
    }

    @Override
    public void writeRecord(Row row) {
        ClickhouseBatchStatement batchStatement = statementMap.get(shardRouter.getShard(row));
        ClickHousePreparedStatementImpl clickHouseStatement = batchStatement.getPreparedStatement();
        IntHolder sizeHolder = batchStatement.getIntHolder();
        // add into batch
        addIntoBatch(row, clickHouseStatement);
        sizeHolder.setValue(sizeHolder.getValue() + 1);
        // flush batch
        if (sizeHolder.getValue() >= batchSize) {
            flush(clickHouseStatement);
            sizeHolder.setValue(0);
        }
    }

    @Override
    public void close() {
        for (ClickhouseBatchStatement batchStatement : statementMap.values()) {
            try (ClickHouseConnectionImpl needClosedConnection = batchStatement.getClickHouseConnection();
                 ClickHousePreparedStatementImpl needClosedStatement = batchStatement.getPreparedStatement()) {
                IntHolder intHolder = batchStatement.getIntHolder();
                if (intHolder.getValue() > 0) {
                    flush(needClosedStatement);
                    intHolder.setValue(0);
                }
            } catch (SQLException e) {
                throw new RuntimeException("Failed to close prepared statement.", e);
            }
        }
    }

    private void addIntoBatch(Row row, ClickHousePreparedStatementImpl clickHouseStatement) {
        try {
            for (int i = 0; i < fields.size(); i++) {
                String fieldName = fields.get(i);
                Object fieldValue = row.getField(fieldName);
                if (fieldValue == null) {
                    // field does not exist in row
                    // todo: do we need to transform to default value of each type
                    clickHouseStatement.setObject(i + 1, null);
                    continue;
                }
                String fieldType = tableSchema.get(fieldName);
                fieldInjectFunctionMap
                    .getOrDefault(fieldType, DEFAULT_INJECT_FUNCTION)
                    .injectFields(clickHouseStatement, i + 1, fieldValue);
            }
            clickHouseStatement.addBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Add row data into batch error", e);
        }
    }

    private void flush(ClickHouseStatement clickHouseStatement) {
        RetryUtils.Execution<Void, Exception> execution = () -> {
            clickHouseStatement.executeBatch();
            return null;
        };
        try {
            RetryUtils.retryWithException(execution, retryMaterial);
        } catch (Exception e) {
            throw new RuntimeException("Clickhouse execute batch statement error", e);
        }
    }

    private String initPrepareSQL() {
        String[] placeholder = new String[fields.size()];
        Arrays.fill(placeholder, "?");

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
            shardRouter.getShardTable(),
            String.join(",", fields),
            String.join(",", placeholder));
    }

    private Map<Shard, ClickhouseBatchStatement> initStatementMap() {
        Map<Shard, ClickhouseBatchStatement> result = new HashMap<>(16);
        shardRouter.getShards().forEach((weight, shard) -> {
            try {
                ClickHouseConnectionImpl clickhouseConnection = clickhouseClient.getClickhouseConnection();
                ClickHousePreparedStatementImpl preparedStatement =
                    (ClickHousePreparedStatementImpl) clickhouseConnection.prepareStatement(prepareSql);
                IntHolder intHolder = new IntHolder();
                ClickhouseBatchStatement batchStatement =
                    new ClickhouseBatchStatement(clickhouseConnection, preparedStatement, intHolder);
                result.put(shard, batchStatement);
            } catch (SQLException e) {
                throw new RuntimeException("Clickhouse prepare statement error", e);
            }
        });
        return result;
    }

    private Map<String, ClickhouseFieldInjectFunction> initFieldInjectFunctionMap() {
        Map<String, ClickhouseFieldInjectFunction> result = new HashMap<>(16);
        List<ClickhouseFieldInjectFunction> clickhouseFieldInjectFunctions = Lists.newArrayList(
            new ArrayInjectFunction(),
            new BigDecimalInjectFunction(),
            new DateInjectFunction(),
            new DateTimeInjectFunction(),
            new DoubleInjectFunction(),
            new FloatInjectFunction(),
            new IntInjectFunction(),
            new LongInjectFunction(),
            new StringInjectFunction()
        );
        ClickhouseFieldInjectFunction defaultFunction = new StringInjectFunction();
        // get field type
        for (String field : fields) {
            ClickhouseFieldInjectFunction function = defaultFunction;
            String fieldType = tableSchema.get(field);
            for (ClickhouseFieldInjectFunction clickhouseFieldInjectFunction : clickhouseFieldInjectFunctions) {
                if (clickhouseFieldInjectFunction.isCurrentFieldType(fieldType)) {
                    function = clickhouseFieldInjectFunction;
                    break;
                }
            }
            result.put(fieldType, function);
        }
        return result;
    }
}
