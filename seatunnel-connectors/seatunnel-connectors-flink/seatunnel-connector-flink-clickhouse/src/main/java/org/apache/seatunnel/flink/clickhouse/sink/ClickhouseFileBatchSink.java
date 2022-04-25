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

import static org.apache.seatunnel.flink.clickhouse.ConfigKey.CLICKHOUSE_LOCAL_PATH;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.COPY_METHOD;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.DATABASE;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.FIELDS;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.HOST;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.PASSWORD;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.SHARDING_KEY;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.TABLE;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.USERNAME;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.clickhouse.pojo.ClickhouseFileCopyMethod;
import org.apache.seatunnel.flink.clickhouse.pojo.IntHolder;
import org.apache.seatunnel.flink.clickhouse.pojo.Shard;
import org.apache.seatunnel.flink.clickhouse.pojo.ShardMetadata;
import org.apache.seatunnel.flink.clickhouse.sink.client.ClickhouseClient;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import ru.yandex.clickhouse.ClickHouseConnection;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ClickhouseFileBatchSink extends ClickhouseBatchSink {

    private Config config;
    private ShardMetadata shardMetadata;
    private Map<String, String> tableSchema = new HashMap<>();
    private List<String> fields;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        CheckResult checkResult = CheckConfigUtil.checkAllExists(config, HOST, TABLE, DATABASE, USERNAME, PASSWORD, CLICKHOUSE_LOCAL_PATH);
        if (!checkResult.isSuccess()) {
            return checkResult;
        }
        Map<String, Object> defaultConfigs = ImmutableMap.<String, Object>builder()
            .put(COPY_METHOD, ClickhouseFileCopyMethod.SCP.getName())
            .build();

        config = config.withFallback(ConfigFactory.parseMap(defaultConfigs));
        return CheckResult.success();
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        ClickhouseClient clickhouseClient = new ClickhouseClient(config);
        String table = config.getString(TABLE);
        String database = config.getString(DATABASE);
        String[] hostAndPort = config.getString(HOST).split(":");
        try (ClickHouseConnection connection = clickhouseClient.getClickhouseConnection()) {
            tableSchema = clickhouseClient.getClickhouseTableSchema(connection, table);
            String shardKey = TypesafeConfigUtils.getConfig(this.config, SHARDING_KEY, "");
            String shardKeyType = tableSchema.get(shardKey);
            shardMetadata = new ShardMetadata(
                shardKey,
                shardKeyType,
                database,
                table,
                false, // we don't need to set splitMode in clickhouse file mode.
                new Shard(1, 1, 1, hostAndPort[0], hostAndPort[0], hostAndPort[1], database));

            if (this.config.hasPath(FIELDS)) {
                fields = this.config.getStringList(FIELDS);
                // check if the fields exist in schema
                for (String field : fields) {
                    if (!tableSchema.containsKey(field)) {
                        throw new RuntimeException("Field " + field + " does not exist in table " + table);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to connect to clickhouse server", e);
        }
    }

    @Nullable
    @Override
    public void outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        RowTypeInfo rowTypeInfo = (RowTypeInfo) dataSet.getType();
        String[] fieldNames = rowTypeInfo.getFieldNames();
        final IntHolder shardKeyIndexHolder = new IntHolder();
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(shardMetadata.getShardKey())) {
                shardKeyIndexHolder.setValue(i);
                break;
            }
        }
        final MapPartitionOperator<Row, Row> mapPartitionOperator = dataSet.partitionCustom(new Partitioner<String>() {
            // make sure the data belongs to each shard shuffle to the same partition
            @Override
            public int partition(String shardKey, int numPartitions) {
                return shardKey.hashCode() % numPartitions;
            }
        }, new KeySelector<Row, String>() {
            @Override
            public String getKey(Row value) {
                int shardKeyIndex = shardKeyIndexHolder.getValue();
                return Objects.toString(value.getField(shardKeyIndex));
            }
        }).mapPartition(new MapPartitionFunction<Row, Row>() {
            @Override
            public void mapPartition(Iterable<Row> values, Collector<Row> out) throws Exception {
                new ClickhouseFileOutputFormat(config, shardMetadata, fields).writeRecords(values);
            }
        });

        // This is just a dummy sink, since each flink job need to have a sink.
        mapPartitionOperator.output(new OutputFormat<Row>() {

            @Override
            public void configure(Configuration parameters) {
            }

            @Override
            public void open(int taskNumber, int numTasks) {
            }

            @Override
            public void writeRecord(Row record) {
            }

            @Override
            public void close() {
            }
        });
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public String getPluginName() {
        return "ClickhouseFile";
    }
}
