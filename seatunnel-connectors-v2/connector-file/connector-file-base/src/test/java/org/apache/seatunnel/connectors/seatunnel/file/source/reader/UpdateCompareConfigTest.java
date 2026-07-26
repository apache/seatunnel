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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class UpdateCompareConfigTest {

    @Test
    void shouldRejectPointLookupParallelismOutsideRange() throws Exception {
        for (int invalid : new int[] {0, 65}) {
            Map<String, Object> config = updateConfig();
            config.put("update_compare_parallelism", invalid);
            try (BinaryReadStrategy strategy = new BinaryReadStrategy()) {
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () -> strategy.setPluginConfig(ConfigFactory.parseMap(config)));
            }
        }
    }

    @Test
    void shouldRejectNegativeBulkThreshold() throws Exception {
        Map<String, Object> config = updateConfig();
        config.put("update_compare_bulk_threshold", -1);
        try (BinaryReadStrategy strategy = new BinaryReadStrategy()) {
            Assertions.assertThrows(
                    FileConnectorException.class,
                    () -> strategy.setPluginConfig(ConfigFactory.parseMap(config)));
        }
    }

    @Test
    void shouldAcceptDefaultsAndBoundaryValues() throws Exception {
        Map<String, Object> config = updateConfig();
        config.put("update_compare_parallelism", 64);
        config.put("update_compare_bulk_threshold", 0);
        try (BinaryReadStrategy strategy = new BinaryReadStrategy()) {
            Assertions.assertDoesNotThrow(
                    () -> strategy.setPluginConfig(ConfigFactory.parseMap(config)));
            Assertions.assertEquals(64, strategy.updateCompareParallelism);
            Assertions.assertEquals(0, strategy.updateCompareBulkThreshold);
        }
    }

    @Test
    void shouldDisableAutomaticBulkComparisonByDefault() throws Exception {
        try (BinaryReadStrategy strategy = new BinaryReadStrategy()) {
            Assertions.assertDoesNotThrow(
                    () -> strategy.setPluginConfig(ConfigFactory.parseMap(updateConfig())));
            Assertions.assertEquals(0, strategy.updateCompareBulkThreshold);
        }
    }

    private static Map<String, Object> updateConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("path", "/source");
        config.put("file_format_type", "binary");
        config.put("sync_mode", "update");
        config.put("target_path", "/target");
        return config;
    }
}
