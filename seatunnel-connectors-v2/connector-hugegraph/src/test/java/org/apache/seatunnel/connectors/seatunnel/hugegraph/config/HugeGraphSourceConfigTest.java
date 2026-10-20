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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HugeGraphSourceConfigTest {

    @Test
    void testDefaultValues() {
        HugeGraphSourceConfig config =
                HugeGraphSourceConfig.of(ReadonlyConfig.fromMap(baseConfig()), schema());

        assertEquals("person", config.getLabel());
        assertEquals(MappingConfig.LabelType.VERTEX, config.getLabelType());
        assertEquals(HugeGraphSourceOptions.PAGE_SIZE.defaultValue(), config.getPageSize());
        assertEquals("127.0.0.1", config.getConnectionConfig().getHost());
    }

    @Test
    void testEdgeLabelTypeAndPageSize() {
        Map<String, Object> configMap = baseConfig();
        configMap.put("label_type", "EDGE");
        configMap.put("page_size", 5000);

        HugeGraphSourceConfig config =
                HugeGraphSourceConfig.of(ReadonlyConfig.fromMap(configMap), schema());

        assertEquals(MappingConfig.LabelType.EDGE, config.getLabelType());
        assertEquals(5000, config.getPageSize());
    }

    @Test
    void testPageSizeRange() {
        Map<String, Object> configMap = baseConfig();
        configMap.put("page_size", 99);

        assertThrows(
                HugeGraphConnectorException.class,
                () -> HugeGraphSourceConfig.of(ReadonlyConfig.fromMap(configMap), schema()));
    }

    @Test
    void testSplitSizeBelowMinimumRejected() {
        // A tiny split_size shatters the keyspace into a huge number of shards (one split each, all
        // persisted into every checkpoint) — reject anything below the HugeGraph minimum shard
        // size.
        Map<String, Object> configMap = baseConfig();
        configMap.put("split_size", 1024L);

        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () ->
                                HugeGraphSourceConfig.of(
                                        ReadonlyConfig.fromMap(configMap), schema()));
        assertTrue(ex.getMessage().contains("split_size"));
    }

    @Test
    void testSplitSizeAtMinimumAccepted() {
        Map<String, Object> configMap = baseConfig();
        configMap.put("split_size", HugeGraphSourceOptions.MIN_SPLIT_SIZE);

        HugeGraphSourceConfig config =
                HugeGraphSourceConfig.of(ReadonlyConfig.fromMap(configMap), schema());
        assertEquals(HugeGraphSourceOptions.MIN_SPLIT_SIZE, config.getSplitSize());
    }

    @Test
    void testSchemaRequired() {
        // null schema is rejected
        assertThrows(
                HugeGraphConnectorException.class,
                () -> HugeGraphSourceConfig.of(ReadonlyConfig.fromMap(baseConfig()), null));
    }

    @Test
    void testEmptyFieldsAllowedForPropertyLessLabel() {
        // A property-less label (e.g. a pure relationship edge) has zero declared fields and is
        // exported as just the reserved columns — this must be accepted, not rejected.
        HugeGraphSourceConfig config =
                HugeGraphSourceConfig.of(
                        ReadonlyConfig.fromMap(baseConfig()),
                        new SeaTunnelRowType(new String[] {}, new SeaTunnelDataType<?>[] {}));
        assertEquals(0, config.getSchema().getTotalFields());
    }

    @Test
    void testGraphSpaceIsHonored() {
        Map<String, Object> configMap = baseConfig();
        configMap.put("graph_space", "my_space");

        HugeGraphSourceConfig config =
                HugeGraphSourceConfig.of(ReadonlyConfig.fromMap(configMap), schema());
        assertEquals("my_space", config.getConnectionConfig().getGraphSpace());
    }

    @Test
    void testGraphSpaceDefaultsToDefault() {
        HugeGraphSourceConfig config =
                HugeGraphSourceConfig.of(ReadonlyConfig.fromMap(baseConfig()), schema());
        assertEquals("DEFAULT", config.getConnectionConfig().getGraphSpace());
    }

    @Test
    void testReadAllSetsFlagAndLabelsWithoutSchema() {
        HugeGraphSourceConfig config =
                HugeGraphSourceConfig.ofReadAll(
                        ReadonlyConfig.fromMap(baseConfig()), Arrays.asList("person", "software"));

        assertTrue(config.isReadAllLabels());
        assertNull(config.getLabel());
        assertNull(config.getSchema());
        assertEquals(Arrays.asList("person", "software"), config.getLabels());
    }

    @Test
    void testReadAllRejectsEmptyLabels() {
        assertThrows(
                HugeGraphConnectorException.class,
                () ->
                        HugeGraphSourceConfig.ofReadAll(
                                ReadonlyConfig.fromMap(baseConfig()), Collections.emptyList()));
    }

    private Map<String, Object> baseConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "127.0.0.1");
        configMap.put("port", 8080);
        configMap.put("graph_name", "hugegraph");
        configMap.put("label", "person");
        return configMap;
    }

    private SeaTunnelRowType schema() {
        return new SeaTunnelRowType(
                new String[] {"name"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
    }
}
