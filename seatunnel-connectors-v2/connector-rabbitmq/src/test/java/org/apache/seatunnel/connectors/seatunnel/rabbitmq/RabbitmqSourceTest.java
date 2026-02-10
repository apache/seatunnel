/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.rabbitmq;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.source.RabbitmqSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RabbitmqSourceTest {

    @Test
    public void testSingleTableConfigParsing() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put("port", 5672);
        configMap.put("virtual_host", "/");
        configMap.put("queue_name", "single_queue");

        Map<String, Object> schema = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        fields.put("id", "int");
        schema.put("fields", fields);
        configMap.put("schema", schema);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        RabbitmqSource source = new RabbitmqSource(config);

        List<CatalogTable> tables = source.getProducedCatalogTables();

        Assertions.assertEquals(1, tables.size());
        Assertions.assertEquals(
                "single_queue",
                tables.get(0).getOptions().get(RabbitmqSourceOptions.QUEUE_NAME.key()));
    }

    @Test
    public void testMultiTableConfigParsing() {
        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("host", "localhost");
        rootMap.put("port", 5672);
        rootMap.put("virtual_host", "/");

        List<Map<String, Object>> tablesConfigs = new ArrayList<>();

        // Table 1
        Map<String, Object> table1 = new HashMap<>();
        table1.put("queue_name", "queue_users");

        Map<String, Object> schema1 = new HashMap<>();
        Map<String, Object> fields1 = new HashMap<>();
        fields1.put("id", "int");
        schema1.put("fields", fields1);
        table1.put("schema", schema1);
        tablesConfigs.add(table1);

        // Table 2
        Map<String, Object> table2 = new HashMap<>();
        table2.put("queue_name", "queue_orders");

        Map<String, Object> schema2 = new HashMap<>();
        Map<String, Object> fields2 = new HashMap<>();
        fields2.put("order_id", "int");
        schema2.put("fields", fields2);
        table2.put("schema", schema2);
        tablesConfigs.add(table2);

        rootMap.put("tables_configs", tablesConfigs);

        ReadonlyConfig config = ReadonlyConfig.fromMap(rootMap);
        RabbitmqSource source = new RabbitmqSource(config);

        List<CatalogTable> tables = source.getProducedCatalogTables();

        Assertions.assertEquals(2, tables.size());

        String queue1 = tables.get(0).getOptions().get(RabbitmqSourceOptions.QUEUE_NAME.key());
        String queue2 = tables.get(1).getOptions().get(RabbitmqSourceOptions.QUEUE_NAME.key());

        List<String> queues = new ArrayList<>();
        queues.add(queue1);
        queues.add(queue2);

        Assertions.assertTrue(queues.contains("queue_users"));
        Assertions.assertTrue(queues.contains("queue_orders"));
    }
}
