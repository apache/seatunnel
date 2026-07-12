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

package org.apache.seatunnel.e2e.connector.hugegraph;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig.LabelType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig.SourceTargetConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.sink.HugeGraphSinkWriter;

import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class HugeGraphIT {

    private static final String HUGE_GRAPH_IMAGE = "hugegraph/hugegraph:1.5.0";
    private static final String GRAPH_NAME = "hugegraph";
    private static final String VERTEX_LABEL = "person";
    private static final SeaTunnelRowType VERTEX_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"name", "age"},
                    new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
    private static HugeClient hugeClient;

    @Container
    private static final GenericContainer<?> HUGE_GRAPH_CONTAINER =
            new GenericContainer<>(DockerImageName.parse(HUGE_GRAPH_IMAGE))
                    .withExposedPorts(8080)
                    .waitingFor(Wait.forHttp("/graphs").forPort(8080).forStatusCode(200))
                    .withStartupTimeout(Duration.ofMinutes(3));

    @BeforeAll
    public static void setup() {
        String host = HUGE_GRAPH_CONTAINER.getHost();
        Integer port = HUGE_GRAPH_CONTAINER.getMappedPort(8080);
        String url = String.format("http://%s:%d", host, port);
        hugeClient = HugeClient.builder(url, GRAPH_NAME).build();
        setupSchema();
    }

    @AfterAll
    public static void cleanup() {
        if (hugeClient != null) {
            hugeClient.close();
        }
    }

    @BeforeEach
    public void clearGraph() {
        hugeClient.graphs().clearGraph(GRAPH_NAME, "I'm sure to delete all data");
        setupSchema();
    }

    private static void setupSchema() {
        hugeClient.schema().propertyKey("name").asText().ifNotExist().create();
        hugeClient.schema().propertyKey("age").asInt().ifNotExist().create();
        hugeClient.schema().propertyKey("weight").asDouble().ifNotExist().create();

        hugeClient
                .schema()
                .vertexLabel(VERTEX_LABEL)
                .idStrategy(IdStrategy.PRIMARY_KEY)
                .primaryKeys("name")
                .properties("name", "age")
                .nullableKeys("age")
                .ifNotExist()
                .create();

        hugeClient
                .schema()
                .edgeLabel("knows")
                .sourceLabel(VERTEX_LABEL)
                .targetLabel(VERTEX_LABEL)
                .properties("weight")
                .nullableKeys("weight")
                .ifNotExist()
                .create();
    }

    private HugeGraphSinkWriter createSinkWriter(
            MappingConfig mappingConfig, SeaTunnelRowType rowType) throws IOException {
        return createSinkWriter(Collections.singletonList(mappingConfig), rowType, false, 100);
    }

    private HugeGraphSinkWriter createSinkWriter(
            List<MappingConfig> mappingConfigs,
            SeaTunnelRowType rowType,
            boolean deleteVertexWithEdges,
            int batchSize)
            throws IOException {
        HugeGraphConnectionConfig connectionConfig = new HugeGraphConnectionConfig();
        connectionConfig.setHost(HUGE_GRAPH_CONTAINER.getHost());
        connectionConfig.setPort(HUGE_GRAPH_CONTAINER.getMappedPort(8080));
        connectionConfig.setGraphName(GRAPH_NAME);

        HugeGraphSinkConfig config = new HugeGraphSinkConfig();
        config.setConnectionConfig(connectionConfig);
        config.setBatchSize(batchSize);
        config.setBatchIntervalMs(0);
        config.setMaxRetries(0);
        config.setRetryBackoffMs(0);
        config.setMappings(mappingConfigs);
        config.setDeleteVertexWithEdges(deleteVertexWithEdges);
        return new HugeGraphSinkWriter(config, rowType);
    }

    @Test
    public void testVertexInsert() throws IOException {
        MappingConfig mapping = new MappingConfig();
        mapping.setType(LabelType.VERTEX);
        mapping.setLabel(VERTEX_LABEL);
        mapping.setIdStrategy(IdStrategy.PRIMARY_KEY);
        mapping.setIdFields(Collections.singletonList("name"));
        mapping.setProperties(Arrays.asList("name", "age"));

        HugeGraphSinkWriter writer = createSinkWriter(mapping, VERTEX_ROW_TYPE);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"marko", 29});
        row.setRowKind(RowKind.INSERT);
        writer.write(row);
        writer.close();

        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "marko");
        List<Vertex> vertices = hugeClient.graph().listVertices(VERTEX_LABEL, properties, 10);
        assertEquals(1, vertices.size());
        assertEquals(29, vertices.get(0).property("age"));
    }

    @Test
    public void testVertexUpdate() throws IOException {
        Vertex vadas = new Vertex(VERTEX_LABEL);
        vadas.property("name", "vadas");
        vadas.property("age", 27);
        hugeClient.graph().addVertex(vadas);

        MappingConfig mapping = new MappingConfig();
        mapping.setType(LabelType.VERTEX);
        mapping.setLabel(VERTEX_LABEL);
        mapping.setIdStrategy(IdStrategy.PRIMARY_KEY);
        mapping.setIdFields(Collections.singletonList("name"));
        mapping.setProperties(Arrays.asList("name", "age"));

        HugeGraphSinkWriter writer = createSinkWriter(mapping, VERTEX_ROW_TYPE);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"vadas", 28});
        row.setRowKind(RowKind.UPDATE_AFTER);
        writer.write(row);
        writer.close();

        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "vadas");
        List<Vertex> vertices = hugeClient.graph().listVertices(VERTEX_LABEL, properties, 10);
        assertEquals(1, vertices.size());
        assertEquals(28, vertices.get(0).property("age"));
    }

    @Test
    public void testVertexDelete() throws IOException {
        Vertex josh = new Vertex(VERTEX_LABEL);
        josh.property("name", "josh");
        josh.property("age", 32);
        hugeClient.graph().addVertex(josh);

        MappingConfig mapping = new MappingConfig();
        mapping.setType(LabelType.VERTEX);
        mapping.setLabel(VERTEX_LABEL);
        mapping.setIdStrategy(IdStrategy.PRIMARY_KEY);
        mapping.setIdFields(Collections.singletonList("name"));
        mapping.setProperties(Arrays.asList("name", "age"));

        HugeGraphSinkWriter writer = createSinkWriter(mapping, VERTEX_ROW_TYPE);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"josh", 32});
        row.setRowKind(RowKind.DELETE);
        writer.write(row);
        writer.close();

        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "josh");
        List<Vertex> vertices = hugeClient.graph().listVertices(VERTEX_LABEL, properties, 10);
        Assertions.assertTrue(vertices.isEmpty(), "Vertex should have been deleted");
    }

    @Test
    public void testEdgeInsert() throws IOException {
        Vertex marko = new Vertex(VERTEX_LABEL).property("name", "marko").property("age", 29);
        Vertex david = new Vertex(VERTEX_LABEL).property("name", "david").property("age", 30);
        hugeClient.graph().addVertex(marko);
        hugeClient.graph().addVertex(david);

        SeaTunnelRowType edgeRowType =
                new SeaTunnelRowType(
                        new String[] {"src_name", "tgt_name", "weight"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.DOUBLE_TYPE
                        });

        MappingConfig mapping = new MappingConfig();
        mapping.setType(LabelType.EDGE);
        mapping.setLabel("knows");

        SourceTargetConfig sourceConfig = new SourceTargetConfig();
        sourceConfig.setLabel(VERTEX_LABEL);
        sourceConfig.setIdFields(Collections.singletonList("src_name"));
        mapping.setSourceConfig(sourceConfig);

        SourceTargetConfig targetConfig = new SourceTargetConfig();
        targetConfig.setLabel(VERTEX_LABEL);
        targetConfig.setIdFields(Collections.singletonList("tgt_name"));
        mapping.setTargetConfig(targetConfig);

        mapping.setProperties(Arrays.asList("weight"));
        Map<String, String> fieldMap = new HashMap<>();
        fieldMap.put("src_name", "name");
        fieldMap.put("tgt_name", "name");
        mapping.setFieldMapping(fieldMap);

        HugeGraphSinkWriter writer = createSinkWriter(mapping, edgeRowType);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"marko", "david", 1.5});
        row.setRowKind(RowKind.INSERT);
        writer.write(row);
        writer.close();

        List<Edge> edges = hugeClient.graph().listEdges("knows");
        assertEquals(1, edges.size());
        assertEquals(1.5, edges.get(0).property("weight"));
    }

    @Test
    public void testEdgeDelete() throws IOException {
        Vertex marko = new Vertex(VERTEX_LABEL).property("name", "marko").property("age", 29);
        Vertex david = new Vertex(VERTEX_LABEL).property("name", "david").property("age", 30);
        marko = hugeClient.graph().addVertex(marko);
        david = hugeClient.graph().addVertex(david);

        Edge edge = new Edge("knows").source(marko).target(david).property("weight", 2.0);
        hugeClient.graph().addEdge(edge);
        assertEquals(1, hugeClient.graph().listEdges("knows").size());

        SeaTunnelRowType edgeRowType =
                new SeaTunnelRowType(
                        new String[] {"src_name", "tgt_name"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

        MappingConfig mapping = new MappingConfig();
        mapping.setType(LabelType.EDGE);
        mapping.setLabel("knows");

        SourceTargetConfig sourceConfig = new SourceTargetConfig();
        sourceConfig.setLabel(VERTEX_LABEL);
        sourceConfig.setIdFields(Collections.singletonList("src_name"));
        mapping.setSourceConfig(sourceConfig);

        SourceTargetConfig targetConfig = new SourceTargetConfig();
        targetConfig.setLabel(VERTEX_LABEL);
        targetConfig.setIdFields(Collections.singletonList("tgt_name"));
        mapping.setTargetConfig(targetConfig);

        Map<String, String> fieldMap = new HashMap<>();
        fieldMap.put("src_name", "name");
        fieldMap.put("tgt_name", "name");
        mapping.setFieldMapping(fieldMap);

        HugeGraphSinkWriter writer = createSinkWriter(mapping, edgeRowType);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"marko", "david"});
        row.setRowKind(RowKind.DELETE);
        writer.write(row);
        writer.close();

        Assertions.assertTrue(hugeClient.graph().listEdges("knows").isEmpty());
    }

    private List<MappingConfig> buildMultiMappingConfigs() {
        MappingConfig vertexMapping = new MappingConfig();
        vertexMapping.setType(LabelType.VERTEX);
        vertexMapping.setLabel(VERTEX_LABEL);
        vertexMapping.setIdStrategy(IdStrategy.PRIMARY_KEY);
        vertexMapping.setIdFields(Collections.singletonList("v_name"));
        vertexMapping.setProperties(Arrays.asList("v_name", "v_age"));
        Map<String, String> vertexFm = new HashMap<>();
        vertexFm.put("v_name", "name");
        vertexFm.put("v_age", "age");
        vertexMapping.setFieldMapping(vertexFm);

        MappingConfig edgeMapping = new MappingConfig();
        edgeMapping.setType(LabelType.EDGE);
        edgeMapping.setLabel("knows");
        SourceTargetConfig srcCfg = new SourceTargetConfig();
        srcCfg.setLabel(VERTEX_LABEL);
        srcCfg.setIdFields(Collections.singletonList("src"));
        edgeMapping.setSourceConfig(srcCfg);
        SourceTargetConfig tgtCfg = new SourceTargetConfig();
        tgtCfg.setLabel(VERTEX_LABEL);
        tgtCfg.setIdFields(Collections.singletonList("tgt"));
        edgeMapping.setTargetConfig(tgtCfg);
        edgeMapping.setProperties(Collections.singletonList("weight"));
        Map<String, String> edgeFm = new HashMap<>();
        edgeFm.put("src", "name");
        edgeFm.put("tgt", "name");
        edgeMapping.setFieldMapping(edgeFm);

        return Arrays.asList(vertexMapping, edgeMapping);
    }

    private static final SeaTunnelRowType MULTI_MAPPING_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"v_name", "v_age", "src", "tgt", "weight"},
                    new SeaTunnelDataType<?>[] {
                        BasicType.STRING_TYPE, BasicType.INT_TYPE,
                        BasicType.STRING_TYPE, BasicType.STRING_TYPE,
                        BasicType.DOUBLE_TYPE
                    });

    @Test
    public void testMultiMappingDeleteWithoutCascade() throws IOException {
        Vertex alice = new Vertex(VERTEX_LABEL).property("name", "Alice").property("age", 30);
        Vertex bob = new Vertex(VERTEX_LABEL).property("name", "Bob").property("age", 25);
        alice = hugeClient.graph().addVertex(alice);
        bob = hugeClient.graph().addVertex(bob);
        Edge edge = new Edge("knows").source(alice).target(bob).property("weight", 1.0);
        hugeClient.graph().addEdge(edge);
        assertEquals(1, hugeClient.graph().listEdges("knows").size());

        HugeGraphSinkWriter writer =
                createSinkWriter(buildMultiMappingConfigs(), MULTI_MAPPING_ROW_TYPE, false, 100);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"Alice", 30, "Alice", "Bob", 1.0});
        row.setRowKind(RowKind.DELETE);
        writer.write(row);
        writer.close();

        assertEquals(
                0,
                hugeClient.graph().listEdges("knows").size(),
                "Edge should have been deleted before vertex");
        Map<String, Object> props = new HashMap<>();
        props.put("name", "Alice");
        Assertions.assertTrue(
                hugeClient.graph().listVertices(VERTEX_LABEL, props, 10).isEmpty(),
                "Vertex Alice should have been deleted");
        props.put("name", "Bob");
        assertEquals(
                1,
                hugeClient.graph().listVertices(VERTEX_LABEL, props, 10).size(),
                "Vertex Bob should still exist");
    }

    @Test
    public void testMultiMappingDeleteWithCascade() throws IOException {
        Vertex alice = new Vertex(VERTEX_LABEL).property("name", "Alice").property("age", 30);
        Vertex bob = new Vertex(VERTEX_LABEL).property("name", "Bob").property("age", 25);
        alice = hugeClient.graph().addVertex(alice);
        bob = hugeClient.graph().addVertex(bob);
        Edge edge = new Edge("knows").source(alice).target(bob).property("weight", 1.0);
        hugeClient.graph().addEdge(edge);
        assertEquals(1, hugeClient.graph().listEdges("knows").size());

        HugeGraphSinkWriter writer =
                createSinkWriter(buildMultiMappingConfigs(), MULTI_MAPPING_ROW_TYPE, true, 100);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"Alice", 30, "Alice", "Bob", 1.0});
        row.setRowKind(RowKind.DELETE);
        writer.write(row);
        writer.close();

        assertEquals(
                0,
                hugeClient.graph().listEdges("knows").size(),
                "Edge should have been deleted");
        Map<String, Object> props = new HashMap<>();
        props.put("name", "Alice");
        Assertions.assertTrue(
                hugeClient.graph().listVertices(VERTEX_LABEL, props, 10).isEmpty(),
                "Vertex Alice should have been deleted (cascade as safety net)");
        props.put("name", "Bob");
        assertEquals(
                1,
                hugeClient.graph().listVertices(VERTEX_LABEL, props, 10).size(),
                "Vertex Bob should still exist");
    }
}
