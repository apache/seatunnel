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

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig.SourceTargetConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.sink.HugeGraphSinkWriter;

import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.exception.ServerException;
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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
public class HugeGraphIT {

    private static final String HUGE_GRAPH_IMAGE = "hugegraph/hugegraph:latest";
    private static final String GRAPH_NAME = "hugegraph";
    private static final String VERTEX_LABEL_PERSON = "person_for_test";
    private static final String VERTEX_LABEL_ALL_TYPES = "vertex_all_types_for_test";
    private static final SeaTunnelRowType SEATUNNEL_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"name", "age"},
                    new SeaTunnelDataType<?>[] {
                        org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE,
                        org.apache.seatunnel.api.table.type.BasicType.INT_TYPE
                    });
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
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
        // Clear all vertices and edges before each test using GraphsManager.clearGraph()
        try {
            hugeClient.graphs().clearGraph(GRAPH_NAME, "I'm sure to delete all data");
            // After clearing, need to recreate schema
            setupSchema();
        } catch (Exception e) {
            // Ignore errors during clear
        }
    }

    private static void setupSchema() {
        hugeClient.schema().propertyKey("name").asText().ifNotExist().create();
        hugeClient.schema().propertyKey("age").asInt().ifNotExist().create();
        hugeClient
                .schema()
                .vertexLabel(VERTEX_LABEL_PERSON)
                .idStrategy(IdStrategy.PRIMARY_KEY)
                .primaryKeys("name")
                .properties("name", "age")
                .ifNotExist()
                .create();

        hugeClient.schema().propertyKey("duration").asFloat().ifNotExist().create();
        hugeClient
                .schema()
                .edgeLabel("knows")
                .sourceLabel(VERTEX_LABEL_PERSON)
                .targetLabel(VERTEX_LABEL_PERSON)
                .properties("duration")
                .ifNotExist()
                .create();

        // New schema for all types vertex
        hugeClient.schema().propertyKey("id_field").asText().ifNotExist().create();
        hugeClient.schema().propertyKey("prop_string").asText().ifNotExist().create();
        hugeClient.schema().propertyKey("prop_long").asLong().ifNotExist().create();
        hugeClient.schema().propertyKey("prop_double").asDouble().ifNotExist().create();
        hugeClient.schema().propertyKey("prop_boolean").asBoolean().ifNotExist().create();
        hugeClient.schema().propertyKey("prop_date").asDate().ifNotExist().create();

        hugeClient
                .schema()
                .vertexLabel(VERTEX_LABEL_ALL_TYPES)
                .idStrategy(IdStrategy.CUSTOMIZE_STRING)
                .properties(
                        "id_field",
                        "prop_string",
                        "prop_long",
                        "prop_double",
                        "prop_boolean",
                        "prop_date")
                .ifNotExist()
                .create();

        hugeClient.schema().propertyKey("lang").asText().ifNotExist().create();

        hugeClient
                .schema()
                .vertexLabel("person_pk_for_edge")
                .idStrategy(IdStrategy.PRIMARY_KEY)
                .primaryKeys("name")
                .properties("name")
                .ifNotExist()
                .create();

        hugeClient
                .schema()
                .vertexLabel("software_cs_for_edge")
                .idStrategy(IdStrategy.CUSTOMIZE_STRING)
                .properties("lang")
                .ifNotExist()
                .create();

        hugeClient
                .schema()
                .edgeLabel("transfer")
                .sourceLabel("person_pk_for_edge")
                .targetLabel("software_cs_for_edge")
                .properties("prop_string", "prop_long", "prop_double", "prop_boolean", "prop_date")
                .ifNotExist()
                .create();
    }

    private HugeGraphSinkWriter createSinkWriter(
            SchemaConfig schemaConfig, SeaTunnelRowType rowType) throws IOException {
        HugeGraphSinkConfig config = new HugeGraphSinkConfig();
        config.setHost(HUGE_GRAPH_CONTAINER.getHost());
        config.setPort(HUGE_GRAPH_CONTAINER.getMappedPort(8080));
        config.setGraphName(GRAPH_NAME);
        config.setSchemaConfig(schemaConfig);
        return new HugeGraphSinkWriter(config, rowType);
    }

    @Test
    public void testInsert() throws IOException {
        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setType(SchemaConfig.LabelType.VERTEX);
        schemaConfig.setLabel(VERTEX_LABEL_PERSON);
        schemaConfig.setIdStrategy(IdStrategy.PRIMARY_KEY);
        schemaConfig.setIdFields(Collections.singletonList("name"));

        try {
            HugeGraphSinkWriter writer = createSinkWriter(schemaConfig, SEATUNNEL_ROW_TYPE);
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {"marko", 29});
            row.setRowKind(RowKind.INSERT);
            writer.write(row);
            writer.close();
        } finally {

        }

        // Verify using REST API
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "marko");
        List<Vertex> vertices =
                hugeClient.graph().listVertices(VERTEX_LABEL_PERSON, properties, 10);
        assertEquals(1, vertices.size());
        assertEquals(29, vertices.get(0).property("age"));
    }

    @Test
    public void testEdgeInsert() throws IOException {
        // 1. Insert source and target vertices
        Vertex marko =
                new Vertex(VERTEX_LABEL_PERSON).property("name", "marko").property("age", 29);
        Vertex david =
                new Vertex(VERTEX_LABEL_PERSON).property("name", "david").property("age", 30);
        hugeClient.graph().addVertex(marko);
        hugeClient.graph().addVertex(david);

        // 2. Define edge row type
        SeaTunnelRowType edgeRowType =
                new SeaTunnelRowType(
                        new String[] {"src_name", "tgt_name", "duration"},
                        new SeaTunnelDataType<?>[] {
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.FLOAT_TYPE
                        });

        // 3. Configure SchemaConfig for edge
        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setType(SchemaConfig.LabelType.EDGE);
        schemaConfig.setLabel("knows");

        SourceTargetConfig sourceConfig = new SourceTargetConfig();
        sourceConfig.setLabel(VERTEX_LABEL_PERSON);
        sourceConfig.setIdFields(Collections.singletonList("src_name"));

        SourceTargetConfig targetConfig = new SourceTargetConfig();
        targetConfig.setLabel(VERTEX_LABEL_PERSON);
        targetConfig.setIdFields(Collections.singletonList("tgt_name"));

        schemaConfig.setSourceConfig(sourceConfig);
        schemaConfig.setTargetConfig(targetConfig);

        MappingConfig mappingConfig = new MappingConfig();
        Map<String, String> map = new HashMap<>();
        map.put("duration", "duration");
        map.put("src_name", "name");
        map.put("tgt_name", "name");
        mappingConfig.setFieldMapping(map);
        schemaConfig.setMapping(mappingConfig);

        try {
            // 4. Create writer with new row type
            HugeGraphSinkWriter writer = createSinkWriter(schemaConfig, edgeRowType);
            // 5. Create and write row
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {"marko", "david", 1.5});
            row.setRowKind(RowKind.INSERT);
            writer.write(row);
            writer.close();
        } finally {
        }

        // 6. Verify edge creation
        List<Edge> edges = hugeClient.graph().listEdges("knows");
        assertEquals(1, edges.size());
        Edge createdEdge = edges.get(0);
        assertEquals(1.5, createdEdge.property("duration"));

        // Also verify source and target
        Vertex sourceVertex = hugeClient.graph().getVertex(createdEdge.sourceId());
        Vertex targetVertex = hugeClient.graph().getVertex(createdEdge.targetId());
        assertEquals("marko", sourceVertex.property("name"));
        assertEquals("david", targetVertex.property("name"));

        // 7. Verify the frequency setting
        try {
            HugeGraphSinkWriter writer = createSinkWriter(schemaConfig, edgeRowType);
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {"marko", "david", 11.0});
            row.setRowKind(RowKind.INSERT);
            writer.write(row);
            writer.close();
        } finally {
        }

        List<Edge> edges_overwrite = hugeClient.graph().listEdges("knows");
        assertEquals(1, edges_overwrite.size());
        Edge createdEdge_overwrite = edges_overwrite.get(0);
        assertEquals(11.0, createdEdge_overwrite.property("duration"));
    }

    @Test
    public void testUpdate() throws IOException {
        // First, insert a vertex using REST API
        Vertex vadas = new Vertex(VERTEX_LABEL_PERSON);
        vadas.property("name", "vadas");
        vadas.property("age", 27);
        hugeClient.graph().addVertex(vadas);

        MappingConfig mappingConfig = new MappingConfig();
        Map<String, String> map = new HashMap<>();
        map.put("name", "name");
        map.put("age", "age");
        mappingConfig.setFieldMapping(map);
        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setType(SchemaConfig.LabelType.VERTEX);
        schemaConfig.setLabel(VERTEX_LABEL_PERSON);
        schemaConfig.setIdStrategy(IdStrategy.PRIMARY_KEY);
        schemaConfig.setIdFields(Collections.singletonList("name"));
        schemaConfig.setMapping(mappingConfig);

        try {
            HugeGraphSinkWriter writer = createSinkWriter(schemaConfig, SEATUNNEL_ROW_TYPE);
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {"vadas", 28});
            row.setRowKind(RowKind.UPDATE_AFTER);
            writer.write(row);
            writer.close();
        } finally {
        }

        // Verify using REST API
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "vadas");
        List<Vertex> vertices =
                hugeClient.graph().listVertices(VERTEX_LABEL_PERSON, properties, 10);
        assertEquals(1, vertices.size());
        assertEquals(28, vertices.get(0).property("age"));
    }

    @Test
    public void testEdgeDelete() throws IOException {
        // 1. Insert vertices and an edge to be deleted
        Vertex marko =
                new Vertex(VERTEX_LABEL_PERSON).property("name", "marko").property("age", 29);
        Vertex david =
                new Vertex(VERTEX_LABEL_PERSON).property("name", "david").property("age", 30);
        marko = hugeClient.graph().addVertex(marko);
        david = hugeClient.graph().addVertex(david);

        Edge edge = new Edge("knows").source(marko).target(david).property("duration", 12.3);
        hugeClient.graph().addEdge(edge);

        // Verify it exists first and there
        assertEquals(1, hugeClient.graph().listEdges("knows").size());

        // 2. Define edge row type (only source/target fields needed for identification)
        SeaTunnelRowType edgeRowType =
                new SeaTunnelRowType(
                        new String[] {"src_name", "tgt_name"},
                        new SeaTunnelDataType<?>[] {
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE
                        });

        // 3. Configure SchemaConfig for edge deletion
        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setType(SchemaConfig.LabelType.EDGE);
        schemaConfig.setLabel("knows");

        SourceTargetConfig sourceConfig = new SourceTargetConfig();
        sourceConfig.setLabel(VERTEX_LABEL_PERSON);
        sourceConfig.setIdFields(Collections.singletonList("src_name"));
        SourceTargetConfig targetConfig = new SourceTargetConfig();
        targetConfig.setLabel(VERTEX_LABEL_PERSON);
        targetConfig.setIdFields(Collections.singletonList("tgt_name"));
        schemaConfig.setSourceConfig(sourceConfig);
        schemaConfig.setTargetConfig(targetConfig);

        MappingConfig mappingConfig = new MappingConfig();
        Map<String, String> map = new HashMap<>();
        map.put("duration", "duration");
        map.put("src_name", "name");
        map.put("tgt_name", "name");
        mappingConfig.setFieldMapping(map);
        schemaConfig.setMapping(mappingConfig);

        try {
            // 4. Create writer
            HugeGraphSinkWriter writer = createSinkWriter(schemaConfig, edgeRowType);
            // 5. Create and write DELETE row
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {"marko", "david"});
            row.setRowKind(RowKind.DELETE);
            writer.write(row);
            writer.close();
        } finally {
        }

        // 6. Verify edge is deleted
        Assertions.assertTrue(hugeClient.graph().listEdges("knows").isEmpty());
    }

    @Test
    public void testDelete() throws IOException {
        // First, insert a vertex using REST API
        Vertex josh = new Vertex(VERTEX_LABEL_PERSON);
        josh.property("name", "josh");
        josh.property("age", 32);
        hugeClient.graph().addVertex(josh);

        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setType(SchemaConfig.LabelType.VERTEX);
        schemaConfig.setLabel(VERTEX_LABEL_PERSON);
        schemaConfig.setIdStrategy(IdStrategy.PRIMARY_KEY);
        schemaConfig.setIdFields(Collections.singletonList("name"));

        try {
            HugeGraphSinkWriter writer = createSinkWriter(schemaConfig, SEATUNNEL_ROW_TYPE);
            // The row only needs to contain the ID fields for a delete operation
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {"josh", 32});
            row.setRowKind(RowKind.DELETE);
            writer.write(row);
            writer.close();
        } finally {
        }

        // Verify using REST API
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "josh");
        List<Vertex> vertices =
                hugeClient.graph().listVertices(VERTEX_LABEL_PERSON, properties, 10);
        Assertions.assertTrue(vertices.isEmpty(), "Vertex should have been deleted");
    }

    @Test
    public void testVertexWithCustomizedIdAndAllTypes() throws IOException {
        // 1. Define RowType for vertex with various data types
        SeaTunnelRowType allTypesRowType =
                new SeaTunnelRowType(
                        new String[] {
                            "id_field",
                            "prop_string",
                            "prop_long",
                            "prop_double",
                            "prop_boolean",
                            "prop_date_1"
                        },
                        new SeaTunnelDataType<?>[] {
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.DOUBLE_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.BOOLEAN_TYPE,
                            org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        // 2. Configure SchemaConfig for the new vertex type
        MappingConfig mappingConfig = new MappingConfig();
        Map<String, String> map = new HashMap<>();
        map.put("prop_date_1", "prop_date");
        mappingConfig.setFieldMapping(map); // 'id_field' will be used as the custom ID
        mappingConfig.setTimeZone("UTC");

        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setType(SchemaConfig.LabelType.VERTEX);
        schemaConfig.setLabel(VERTEX_LABEL_ALL_TYPES);
        schemaConfig.setIdStrategy(IdStrategy.CUSTOMIZE_STRING);
        schemaConfig.setIdFields(Collections.singletonList("id_field"));
        schemaConfig.setMapping(mappingConfig);

        // 3. INSERT operation
        HugeGraphSinkWriter writer = createSinkWriter(schemaConfig, allTypesRowType);
        LocalDateTime insertDate = LocalDateTime.of(2023, 1, 1, 12, 0, 0);
        Object[] insertData =
                new Object[] {"custom_id_1", "hello", 2147483648L, 123.45, true, insertDate};
        SeaTunnelRow insertRow = new SeaTunnelRow(insertData);
        insertRow.setRowKind(RowKind.INSERT);
        writer.write(insertRow);
        writer.close();

        // 4. Verify INSERT
        System.out.println(hugeClient.graph().getVertex("custom_id_1"));
        Vertex insertedVertex = hugeClient.graph().getVertex("custom_id_1");
        Assertions.assertNotNull(insertedVertex);
        assertEquals(VERTEX_LABEL_ALL_TYPES, insertedVertex.label());
        assertEquals("hello", insertedVertex.property("prop_string"));
        assertEquals(2147483648L, insertedVertex.property("prop_long"));
        assertEquals(123.45, insertedVertex.property("prop_double"));
        assertEquals(true, insertedVertex.property("prop_boolean"));
        // The date is serialized as a long (timestamp)
        Date expectedDate = Date.from(insertDate.atZone(ZoneOffset.UTC).toInstant());
        LocalDateTime insertDateTime =
                LocalDateTime.parse((String) insertedVertex.property("prop_date"), formatter);
        long insertTimeStampUtc = insertDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        Assertions.assertEquals(expectedDate.getTime(), insertTimeStampUtc);

        // 5. UPDATE operation
        writer = createSinkWriter(schemaConfig, allTypesRowType);
        LocalDateTime updateDate = LocalDateTime.of(2024, 2, 2, 1, 1, 1);
        Object[] updateData =
                new Object[] {"custom_id_1", "world", 2000000L, 543.21, false, updateDate};
        SeaTunnelRow updateRow = new SeaTunnelRow(updateData);
        updateRow.setRowKind(RowKind.UPDATE_AFTER);
        writer.write(updateRow);
        writer.close();

        // 6. Verify UPDATE
        System.out.println(hugeClient.graph().getVertex("custom_id_1"));
        Vertex updatedVertex = hugeClient.graph().getVertex("custom_id_1");
        Assertions.assertNotNull(updatedVertex);
        assertEquals("world", updatedVertex.property("prop_string"));
        assertEquals(2000000L, ((Number) updatedVertex.property("prop_long")).longValue());
        assertEquals(543.21, updatedVertex.property("prop_double"));
        assertEquals(false, updatedVertex.property("prop_boolean"));

        Date expectedUpdateDate = Date.from(updateDate.atZone(ZoneOffset.UTC).toInstant());
        LocalDateTime updatedDateTime =
                LocalDateTime.parse((String) updatedVertex.property("prop_date"), formatter);
        long updatedTimeStampMillisUtc = updatedDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        Assertions.assertEquals(expectedUpdateDate.getTime(), updatedTimeStampMillisUtc);

        // 7. DELETE operation
        writer = createSinkWriter(schemaConfig, allTypesRowType);
        // For delete, only the ID field is required.
        Object[] deleteData = new Object[] {"custom_id_1", null, null, null, null, null};
        SeaTunnelRow deleteRow = new SeaTunnelRow(deleteData);
        deleteRow.setRowKind(RowKind.DELETE);
        writer.write(deleteRow);
        writer.close();

        // 8. Verify DELETE
        ServerException serverException =
                assertThrows(
                        ServerException.class,
                        () -> {
                            hugeClient.graph().getVertex("custom_id_1");
                        });

        String expectedErrorMessage = "Vertex 'custom_id_1' does not exist";
        assertEquals(expectedErrorMessage, serverException.getMessage());
    }

    @Test
    public void testEdgeWithComplexTypesAndIdStrategies() throws IOException {
        // 1. Insert source and target vertices
        Vertex person = new Vertex("person_pk_for_edge").property("name", "person1");
        hugeClient.graph().addVertex(person);

        Vertex software = new Vertex("software_cs_for_edge");
        software.id("software1");
        software.property("lang", "java");
        hugeClient.graph().addVertex(software);

        // 2. Define edge row type with all properties
        SeaTunnelRowType edgeRowType =
                new SeaTunnelRowType(
                        new String[] {
                            "src_name",
                            "tgt_id",
                            "prop_string",
                            "prop_long",
                            "prop_double",
                            "prop_boolean",
                            "prop_date"
                        },
                        new SeaTunnelDataType<?>[] {
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.DOUBLE_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.BOOLEAN_TYPE,
                            org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        // 3. Configure SchemaConfig for edge
        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setType(SchemaConfig.LabelType.EDGE);
        schemaConfig.setLabel("transfer");

        SourceTargetConfig sourceConfig = new SourceTargetConfig();
        sourceConfig.setLabel("person_pk_for_edge");
        sourceConfig.setIdFields(Collections.singletonList("src_name"));

        SourceTargetConfig targetConfig = new SourceTargetConfig();
        targetConfig.setLabel("software_cs_for_edge");
        targetConfig.setIdFields(Collections.singletonList("tgt_id"));

        schemaConfig.setSourceConfig(sourceConfig);
        schemaConfig.setTargetConfig(targetConfig);

        MappingConfig mappingConfig = new MappingConfig();
        Map<String, String> map = new HashMap<>();
        map.put("src_name", "name");
        map.put("tgt_id", "lang");
        mappingConfig.setFieldMapping(map);
        schemaConfig.setMapping(mappingConfig);

        // 4. INSERT operation
        HugeGraphSinkWriter writer = createSinkWriter(schemaConfig, edgeRowType);
        LocalDateTime insertDate = LocalDateTime.of(2023, 1, 1, 12, 0, 0);
        Object[] insertData =
                new Object[] {
                    "person1", "software1", "transfer_v1", 100L, 123.45, true, insertDate
                };
        SeaTunnelRow insertRow = new SeaTunnelRow(insertData);
        insertRow.setRowKind(RowKind.INSERT);
        writer.write(insertRow);
        writer.close();

        // 5. Verify INSERT
        System.out.println(hugeClient.graph().listEdges("transfer"));
        List<Edge> edges = hugeClient.graph().listEdges("transfer");
        assertEquals(1, edges.size());
        Edge createdEdge = edges.get(0);
        assertEquals("transfer_v1", createdEdge.property("prop_string"));
        assertEquals(100L, ((Number) createdEdge.property("prop_long")).longValue());
        assertEquals(123.45, createdEdge.property("prop_double"));
        assertEquals(true, createdEdge.property("prop_boolean"));

        // Verify source and target
        Vertex sourceVertex = hugeClient.graph().getVertex(createdEdge.sourceId());
        Vertex targetVertex = hugeClient.graph().getVertex(createdEdge.targetId());
        assertEquals("person1", sourceVertex.property("name"));
        assertEquals("software1", targetVertex.id());

        // 6. UPDATE operation
        writer = createSinkWriter(schemaConfig, edgeRowType);
        LocalDateTime updateDate = LocalDateTime.of(2024, 2, 2, 1, 1, 1);
        Object[] updateData =
                new Object[] {
                    "person1", "software1", "transfer_v2", 200L, 543.21, false, updateDate
                };
        SeaTunnelRow updateRow = new SeaTunnelRow(updateData);
        updateRow.setRowKind(RowKind.UPDATE_AFTER);
        writer.write(updateRow);
        writer.close();

        // 7. Verify UPDATE
        System.out.println(hugeClient.graph().listEdges("transfer"));
        edges = hugeClient.graph().listEdges("transfer");
        assertEquals(1, edges.size());
        Edge updatedEdge = edges.get(0);
        assertEquals("transfer_v2", updatedEdge.property("prop_string"));
        assertEquals(200L, ((Number) updatedEdge.property("prop_long")).longValue());
        assertEquals(543.21, updatedEdge.property("prop_double"));
        assertEquals(false, updatedEdge.property("prop_boolean"));

        // 8. DELETE operation
        SeaTunnelRowType edgeDeleteRowType =
                new SeaTunnelRowType(
                        new String[] {"src_name", "tgt_id"},
                        new SeaTunnelDataType<?>[] {
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE,
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE
                        });

        writer = createSinkWriter(schemaConfig, edgeDeleteRowType);
        Object[] deleteData = new Object[] {"person1", "software1"};
        SeaTunnelRow deleteRow = new SeaTunnelRow(deleteData);
        deleteRow.setRowKind(RowKind.DELETE);
        writer.write(deleteRow);
        writer.close();

        // 9. Verify DELETE
        Assertions.assertTrue(hugeClient.graph().listEdges("transfer").isEmpty());
    }
}
