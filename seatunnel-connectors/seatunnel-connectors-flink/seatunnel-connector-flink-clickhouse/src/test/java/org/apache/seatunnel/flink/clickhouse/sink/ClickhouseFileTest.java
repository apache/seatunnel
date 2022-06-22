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

import static org.junit.Assert.assertEquals;

import org.apache.seatunnel.flink.clickhouse.sink.client.ClickhouseClient;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ClickhouseFileTest {

    @Test
    public void testLocalizationEngine() {
        String originalEngine = "ReplicatedMergeTree";
        String replicatedMergeTreeDDL = "CREATE TABLE default.replicatedMergeTreeTable (`shard_key` Int32, `order_id` String, " +
                "`user_name` String, `user_id` String, `order_time` DateTime, `bi_dt` String)" +
                " ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/default/replicatedMergeTreeTable', " +
                "'{replica}') PARTITION BY bi_dt ORDER BY (order_time, user_id)" +
                " SETTINGS index_granularity = 8192";
        String localizationDDL = "CREATE TABLE default.replicatedMergeTreeTable (`shard_key` Int32, `order_id` String, " +
                "`user_name` String, `user_id` String, `order_time` DateTime, `bi_dt` String)" +
                " ENGINE = MergeTree() PARTITION BY bi_dt ORDER BY (order_time, user_id)" +
                " SETTINGS index_granularity = 8192";
        Config config = getConfig();
        ClickhouseClient client = new ClickhouseClient(config);
        assertEquals(localizationDDL, client.localizationEngine(originalEngine, replicatedMergeTreeDDL));
    }

    public Config getConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("username", "test");
        configMap.put("password", "test");
        configMap.put("host", "localhost:8080");
        configMap.put("database", "test");
        return ConfigFactory.parseMap(configMap);
    }

}
