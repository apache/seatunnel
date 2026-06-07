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

package org.apache.seatunnel.connectors.seatunnel.edgesocket;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.source.EdgeSocketSource;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.source.EdgeSocketSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class EdgeSocketFactoryTest {
    @Test
    void optionRule() {
        Assertions.assertNotNull(new EdgeSocketSourceFactory().optionRule());
    }

    @Test
    void optionRuleContainsBackpressureOptions() {
        OptionRule rule = new EdgeSocketSourceFactory().optionRule();
        Set<String> optionalKeys =
                rule.getOptionalOptions().stream()
                        .map(opt -> opt.key())
                        .collect(Collectors.toSet());
        Assertions.assertTrue(
                optionalKeys.contains(
                        EdgeSocketSourceOptions.QUEUE_BACKPRESSURE_WATERMARK_RATIO.key()),
                "optionRule must include queue_backpressure_watermark_ratio");
        Assertions.assertTrue(
                optionalKeys.contains(EdgeSocketSourceOptions.QUEUE_FULL_RETRY_AFTER_MS.key()),
                "optionRule must include queue_full_retry_after_ms");
    }

    @Test
    void shouldUseNullableEndpointAndQueueCapacity() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(EdgeSocketCommonOptions.PORT.key(), 10001);
        configMap.put(EdgeSocketSourceOptions.TOKEN.key(), "token");

        EdgeSocketConfig config = new EdgeSocketConfig(ReadonlyConfig.fromMap(configMap));
        Assertions.assertNull(config.getEndpoint());
        Assertions.assertEquals(1024, config.getLocalQueueCapacity());
    }

    @Test
    void shouldCreateDefaultValueSchemaWhenSchemaNotConfigured() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(EdgeSocketCommonOptions.PORT.key(), 10001);
        configMap.put(EdgeSocketSourceOptions.TOKEN.key(), "token");

        EdgeSocketSource source = new EdgeSocketSource(ReadonlyConfig.fromMap(configMap));
        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());
        CatalogTable catalogTable = source.getProducedCatalogTables().get(0);
        Assertions.assertArrayEquals(
                new String[] {"value"}, catalogTable.getTableSchema().getFieldNames());
    }

    @Test
    void shouldUseConfiguredSchemaWhenSchemaProvided() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(EdgeSocketCommonOptions.PORT.key(), 10001);
        configMap.put(EdgeSocketSourceOptions.TOKEN.key(), "token");
        configMap.put(
                ConnectorCommonOptions.SCHEMA.key(),
                Collections.singletonMap("fields", Collections.singletonMap("name", "string")));

        EdgeSocketSource source = new EdgeSocketSource(ReadonlyConfig.fromMap(configMap));
        CatalogTable catalogTable = source.getProducedCatalogTables().get(0);
        Assertions.assertArrayEquals(
                new String[] {"name"}, catalogTable.getTableSchema().getFieldNames());
    }

    @Test
    void shouldSupportConfiguredEndpoint() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(EdgeSocketCommonOptions.PORT.key(), 10001);
        configMap.put(EdgeSocketSourceOptions.TOKEN.key(), "token");
        configMap.put(EdgeSocketCommonOptions.ENDPOINT.key(), "edge.lb.example.com:10091");

        EdgeSocketConfig config = new EdgeSocketConfig(ReadonlyConfig.fromMap(configMap));
        Assertions.assertEquals("edge.lb.example.com:10091", config.getEndpoint());
    }

    @Test
    void shouldRejectInvalidEndpoint() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(EdgeSocketCommonOptions.PORT.key(), 10001);
        configMap.put(EdgeSocketSourceOptions.TOKEN.key(), "token");
        configMap.put(EdgeSocketCommonOptions.ENDPOINT.key(), "edge.lb.example.com");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new EdgeSocketConfig(ReadonlyConfig.fromMap(configMap)));
    }
}
