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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphOperations;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.PageResult;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Shard;
import org.apache.hugegraph.structure.graph.Vertex;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HugeGraphSourceFactoryTest {

    @Test
    void optionRuleMakesLabelOptionalForReadAll() {
        // 'label' must be optional so it can be omitted to read all labels of label_type.
        OptionRule rule = new HugeGraphSourceFactory().optionRule();
        assertTrue(rule.getOptionalOptions().contains(HugeGraphSourceOptions.LABEL));
    }

    @Test
    void testVertexReservedFields() {
        SeaTunnelRowType rowType =
                HugeGraphSourceFactory.prependReservedFields(
                        propertyRowType(), MappingConfig.LabelType.VERTEX);

        assertArrayEquals(new String[] {"~id", "~label", "name", "age"}, rowType.getFieldNames());
    }

    @Test
    void testEdgeReservedFields() {
        SeaTunnelRowType rowType =
                HugeGraphSourceFactory.prependReservedFields(
                        propertyRowType(), MappingConfig.LabelType.EDGE);

        assertArrayEquals(
                new String[] {
                    "~id",
                    "~label",
                    "~source_id",
                    "~source_label",
                    "~target_id",
                    "~target_label",
                    "name",
                    "age"
                },
                rowType.getFieldNames());
    }

    @Test
    void rejectsReservedColumnNameInSchemaFields() {
        // Declaring a reserved column (~id) in schema.fields used to silently duplicate the column
        // and later fail with a misleading "label has no property ~id"; it must fail fast with a
        // message that names the offending column.
        SeaTunnelRowType withReserved =
                new SeaTunnelRowType(
                        new String[] {"~id", "name"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});
        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () ->
                                HugeGraphSourceFactory.prependReservedFields(
                                        withReserved, MappingConfig.LabelType.VERTEX));
        assertTrue(ex.getMessage().contains("~id"));
        assertTrue(ex.getMessage().contains("schema.fields"));
    }

    @Test
    void rejectsReservedEdgeEndpointColumnInSchemaFields() {
        SeaTunnelRowType withReserved =
                new SeaTunnelRowType(
                        new String[] {"~source_id", "weight"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.DOUBLE_TYPE});
        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () ->
                                HugeGraphSourceFactory.prependReservedFields(
                                        withReserved, MappingConfig.LabelType.EDGE));
        assertTrue(ex.getMessage().contains("~source_id"));
    }

    @Test
    void rejectsFilterWithParallelismGreaterThanOne() {
        Map<String, Object> options = new HashMap<>();
        options.put("parallelism", 2);
        options.put("filter", Collections.singletonMap("country", "US"));
        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () ->
                                HugeGraphSourceFactory.checkFilterParallelism(
                                        ReadonlyConfig.fromMap(options)));
        assertTrue(ex.getMessage().contains("filter"));
        assertTrue(ex.getMessage().contains("parallelism"));
    }

    @Test
    void allowsParallelismGreaterThanOneWithoutFilter() {
        Map<String, Object> options = new HashMap<>();
        options.put("parallelism", 4);
        assertDoesNotThrow(
                () ->
                        HugeGraphSourceFactory.checkFilterParallelism(
                                ReadonlyConfig.fromMap(options)));
    }

    @Test
    void allowsFilterWithParallelismOneOrUnset() {
        Map<String, Object> one = new HashMap<>();
        one.put("parallelism", 1);
        one.put("filter", Collections.singletonMap("country", "US"));
        assertDoesNotThrow(
                () -> HugeGraphSourceFactory.checkFilterParallelism(ReadonlyConfig.fromMap(one)));

        Map<String, Object> unset = new HashMap<>();
        unset.put("filter", Collections.singletonMap("country", "US"));
        assertDoesNotThrow(
                () -> HugeGraphSourceFactory.checkFilterParallelism(ReadonlyConfig.fromMap(unset)));
    }

    @Test
    void optionRuleDeclaresRetryBackoffMax() {
        assertTrue(
                new HugeGraphSourceFactory()
                        .optionRule()
                        .getOptionalOptions()
                        .contains(
                                org.apache.seatunnel.connectors.seatunnel.hugegraph.config
                                        .HugeGraphOptions.RETRY_BACKOFF_MAX_MS),
                "retry_backoff_max_ms missing from source optionRule");
    }

    @Test
    void discoversPropertyRowTypeFromServerSortedByName() {
        FakeClient client = new FakeClient();
        client.vertexProperties = new HashSet<>(Arrays.asList("name", "age", "tags"));
        client.propertyTypes.put("name", DataType.TEXT);
        client.propertyTypes.put("age", DataType.INT);
        client.propertyTypes.put("tags", DataType.TEXT);
        client.propertyCardinalities.put("tags", Cardinality.LIST);

        SeaTunnelRowType rowType =
                HugeGraphSourceFactory.discoverPropertyRowType(
                        client, "person", MappingConfig.LabelType.VERTEX);

        // sorted by name: age, name, tags
        assertArrayEquals(new String[] {"age", "name", "tags"}, rowType.getFieldNames());
        assertEquals(BasicType.INT_TYPE, rowType.getFieldType(0));
        assertEquals(BasicType.STRING_TYPE, rowType.getFieldType(1));
        assertEquals(ArrayType.STRING_ARRAY_TYPE, rowType.getFieldType(2));
    }

    @Test
    void discoverFailsWhenLabelMissing() {
        FakeClient client = new FakeClient();
        // vertexProperties stays null -> label does not exist
        assertThrows(
                HugeGraphConnectorException.class,
                () ->
                        HugeGraphSourceFactory.discoverPropertyRowType(
                                client, "ghost", MappingConfig.LabelType.VERTEX));
    }

    @Test
    void discoversEmptyRowTypeForPropertylessLabel() {
        FakeClient client = new FakeClient();
        client.edgeProperties = new HashSet<>();

        SeaTunnelRowType rowType =
                HugeGraphSourceFactory.discoverPropertyRowType(
                        client, "rel", MappingConfig.LabelType.EDGE);

        assertEquals(0, rowType.getTotalFields());
    }

    private SeaTunnelRowType propertyRowType() {
        return new SeaTunnelRowType(
                new String[] {"name", "age"},
                new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
    }

    private static class FakeClient implements HugeGraphOperations {
        private final Map<String, DataType> propertyTypes = new HashMap<>();
        private final Map<String, Cardinality> propertyCardinalities = new HashMap<>();
        private Set<String> vertexProperties;
        private Set<String> edgeProperties;

        @Override
        public Set<String> getVertexLabelPropertiesOrNull(String label) {
            return vertexProperties;
        }

        @Override
        public Set<String> getEdgeLabelPropertiesOrNull(String label) {
            return edgeProperties;
        }

        @Override
        public List<String> listVertexLabels() {
            return Collections.emptyList();
        }

        @Override
        public List<String> listEdgeLabels() {
            return Collections.emptyList();
        }

        @Override
        public DataType getPropertyDataType(String propertyName) {
            return propertyTypes.get(propertyName);
        }

        @Override
        public Cardinality getPropertyCardinality(String propertyName) {
            return propertyCardinalities.getOrDefault(propertyName, Cardinality.SINGLE);
        }

        @Override
        public PageResult<Vertex> listVertices(
                String label, Map<String, Object> filter, String page, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PageResult<Edge> listEdges(
                String label, Map<String, Object> filter, String page, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Shard> vertexShards(long splitSize) {
            return Collections.emptyList();
        }

        @Override
        public List<Shard> edgeShards(long splitSize) {
            return Collections.emptyList();
        }

        @Override
        public PageResult<Vertex> scanVertices(Shard shard, String page, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PageResult<Edge> scanEdges(Shard shard, String page, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
