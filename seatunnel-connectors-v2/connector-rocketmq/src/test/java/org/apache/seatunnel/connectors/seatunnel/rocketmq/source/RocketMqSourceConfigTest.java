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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.StartMode;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RocketMqSourceConfigTest {

    private static Map<String, Object> baseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("name.srv.addr", "localhost:9876");
        return config;
    }

    private static Map<String, Object> schemaOf(String... nameTypePairs) {
        Map<String, Object> fields = new LinkedHashMap<>();
        for (int i = 0; i < nameTypePairs.length; i += 2) {
            fields.put(nameTypePairs[i], nameTypePairs[i + 1]);
        }
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", fields);
        return schema;
    }

    private static Map<String, Object> tableEntry(String topics, String... nameTypePairs) {
        Map<String, Object> entry = new HashMap<>();
        entry.put("topics", topics);
        entry.put("format", "json");
        entry.put("schema", schemaOf(nameTypePairs));
        return entry;
    }

    @Test
    void testMultiTableMode_tablesConfigs_tableName() {
        Map<String, Object> config = baseConfig();

        // topic_a: no explicit table name, should fall back to topic name
        Map<String, Object> entry1 = tableEntry("topic_a", "id", "bigint");

        // topic_b: explicit table name
        Map<String, Object> schemaWithTable = schemaOf("id", "bigint");
        schemaWithTable.put("table", "my_custom_table");
        Map<String, Object> entry2 = new HashMap<>();
        entry2.put("topics", "topic_b");
        entry2.put("format", "json");
        entry2.put("schema", schemaWithTable);

        config.put("tables_configs", Arrays.asList(entry1, entry2));

        RocketMqSourceConfig sourceConfig =
                new RocketMqSourceConfig(ReadonlyConfig.fromMap(config));

        CatalogTable tableA = sourceConfig.getTopicConfigs().get("topic_a").getCatalogTable();
        CatalogTable tableB = sourceConfig.getTopicConfigs().get("topic_b").getCatalogTable();
        assertEquals("topic_a", tableA.getTablePath().toString());
        assertEquals("my_custom_table", tableB.getTablePath().toString());
    }

    @Test
    void testMultiTableMode_perTableStartMode_consumeFromTimestamp() {
        Map<String, Object> config = baseConfig();

        long ts = System.currentTimeMillis() - 60_000;

        Map<String, Object> entry = tableEntry("topic_ts", "id", "bigint");
        entry.put("start.mode", "CONSUME_FROM_TIMESTAMP");
        entry.put("start.mode.timestamp", ts);

        List<Map<String, Object>> tablesConfigs = new ArrayList<>();
        tablesConfigs.add(entry);
        config.put("tables_configs", tablesConfigs);

        RocketMqSourceConfig sourceConfig =
                new RocketMqSourceConfig(ReadonlyConfig.fromMap(config));

        TopicTableConfig topicCfg = sourceConfig.getTopicConfigs().get("topic_ts");
        assertEquals(StartMode.CONSUME_FROM_TIMESTAMP, topicCfg.getStartMode());
        assertEquals(ts, topicCfg.getStartTimestamp());
    }

    @Test
    void testMultiTableMode_specificOffsets_mergedAcrossTables() {
        Map<String, Object> config = baseConfig();

        Map<String, Long> offsets1 = new HashMap<>();
        offsets1.put("topic_a-0", 100L);
        offsets1.put("topic_a-1", 200L);
        Map<String, Object> entry1 = tableEntry("topic_a", "id", "bigint");
        entry1.put("start.mode", "CONSUME_FROM_SPECIFIC_OFFSETS");
        entry1.put("start.mode.offsets", offsets1);

        Map<String, Long> offsets2 = new HashMap<>();
        offsets2.put("topic_b-0", 50L);
        Map<String, Object> entry2 = tableEntry("topic_b", "id", "bigint");
        entry2.put("start.mode", "CONSUME_FROM_SPECIFIC_OFFSETS");
        entry2.put("start.mode.offsets", offsets2);

        List<Map<String, Object>> tablesConfigs = new ArrayList<>();
        tablesConfigs.add(entry1);
        tablesConfigs.add(entry2);
        config.put("tables_configs", tablesConfigs);

        RocketMqSourceConfig sourceConfig =
                new RocketMqSourceConfig(ReadonlyConfig.fromMap(config));

        // Specific offsets from both tables should be merged into the same map in metadata
        assertNotNull(sourceConfig.getMetadata().getSpecificStartOffsets());
        assertEquals(3, sourceConfig.getMetadata().getSpecificStartOffsets().size());
    }
}
