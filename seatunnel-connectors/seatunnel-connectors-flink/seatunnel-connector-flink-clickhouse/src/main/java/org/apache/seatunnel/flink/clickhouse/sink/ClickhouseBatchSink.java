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
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.DATABASE;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.FIELDS;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.HOST;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.PASSWORD;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.RETRY;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.RETRY_CODES;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.SHARDING_KEY;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.SPLIT_MODE;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.TABLE;
import static org.apache.seatunnel.flink.clickhouse.ConfigKey.USERNAME;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.flink.BaseFlinkSink;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSink;
import org.apache.seatunnel.flink.clickhouse.pojo.Shard;
import org.apache.seatunnel.flink.clickhouse.pojo.ShardMetadata;
import org.apache.seatunnel.flink.clickhouse.sink.client.ClickhouseClient;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("magicnumber")
@AutoService(BaseFlinkSink.class)
public class ClickhouseBatchSink implements FlinkBatchSink {

    private ShardMetadata shardMetadata;
    private Config config;

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
    public void outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        ClickhouseOutputFormat clickhouseOutputFormat = new ClickhouseOutputFormat(config, shardMetadata, fields, tableSchema);
        dataSet.output(clickhouseOutputFormat);
    }

    @Override
    public CheckResult checkConfig() {
        if (config.hasPath(USERNAME) && config.hasPath(PASSWORD)) {
            return CheckConfigUtil.checkAllExists(config, HOST, TABLE, DATABASE, USERNAME, PASSWORD);
        } else {
            return CheckConfigUtil.checkAllExists(config, HOST, TABLE, DATABASE);
        }
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        Map<String, Object> defaultConfig = ImmutableMap.<String, Object>builder()
            .put(BULK_SIZE, 20_000)
            .put(RETRY_CODES, new ArrayList<>())
            .put(RETRY, 1)
            .put(SPLIT_MODE, false)
            .build();

        config = config.withFallback(ConfigFactory.parseMap(defaultConfig));

        ClickhouseClient clickhouseClient = new ClickhouseClient(config);
        boolean splitMode = config.getBoolean(SPLIT_MODE);
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
                splitMode,
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

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public String getPluginName() {
        return "Clickhouse";
    }

}
