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

package org.apache.seatunnel.connectors.seatunnel.prometheus;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class PrometheusSinkConfigTest {

    @Test
    void shouldApplyBatchSizeDefaultWhenNotConfigured() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", "http://localhost:9090/api/v1/write");
        map.put("key_label", "c_map");
        map.put("key_value", "c_double");

        PrometheusSinkConfig config = PrometheusSinkConfig.loadConfig(ReadonlyConfig.fromMap(map));

        Assertions.assertEquals(
                PrometheusSinkOptions.BATCH_SIZE.defaultValue().intValue(), config.getBatchSize());
    }

    @Test
    void shouldUseConfiguredBatchSize() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", "http://localhost:9090/api/v1/write");
        map.put("key_label", "c_map");
        map.put("key_value", "c_double");
        map.put("batch_size", 5);

        PrometheusSinkConfig config = PrometheusSinkConfig.loadConfig(ReadonlyConfig.fromMap(map));

        Assertions.assertEquals(5, config.getBatchSize());
    }
}
