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

package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class JdbcConnectionConfigTest {

    @Test
    public void testBatchIntervalMsDefaultValue() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        configMap.put("driver", "com.mysql.cj.jdbc.Driver");

        JdbcConnectionConfig config = JdbcConnectionConfig.of(ReadonlyConfig.fromMap(configMap));

        Assertions.assertEquals(0L, config.getBatchIntervalMs());
    }

    @Test
    public void testBatchIntervalMsExplicitValue() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        configMap.put("driver", "com.mysql.cj.jdbc.Driver");
        configMap.put("batch_interval_ms", 1000L);

        JdbcConnectionConfig config = JdbcConnectionConfig.of(ReadonlyConfig.fromMap(configMap));

        Assertions.assertEquals(1000L, config.getBatchIntervalMs());
    }

    @Test
    public void testBatchIntervalMsWithBatchSizeOne() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:mysql://localhost:3306/test");
        configMap.put("driver", "com.mysql.cj.jdbc.Driver");
        configMap.put("batch_size", 1);
        configMap.put("batch_interval_ms", 1000L);

        JdbcConnectionConfig config = JdbcConnectionConfig.of(ReadonlyConfig.fromMap(configMap));

        Assertions.assertEquals(1000L, config.getBatchIntervalMs());
        Assertions.assertEquals(1, config.getBatchSize());
    }

    @Test
    public void testBatchIntervalMsBuilder() {
        JdbcConnectionConfig config =
                JdbcConnectionConfig.builder()
                        .url("jdbc:mysql://localhost:3306/test")
                        .driverName("com.mysql.cj.jdbc.Driver")
                        .batchIntervalMs(2000L)
                        .build();

        Assertions.assertEquals(2000L, config.getBatchIntervalMs());
    }
}
