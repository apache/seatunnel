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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class HugeGraphSourceIT extends TestSuiteBase implements TestResource {

    // Pinned to 1.7.0 to match the graph-space-aware client (REST paths are
    // /graphspaces/{graphspace}/graphs/{graph}/...); a <1.7.0 server would 404 those paths.
    private static final String HUGE_GRAPH_IMAGE = "hugegraph/hugegraph:1.7.0";
    private static final String HUGE_GRAPH_HOST = "hugegraph-host";
    private static final int HUGE_GRAPH_PORT = 8080;
    private static final String GRAPH_NAME = "hugegraph";
    private static final String VERTEX_LABEL = "person";
    private static final String EDGE_LABEL = "knows";
    private static final String GADGET_LABEL = "gadget";

    private GenericContainer<?> hugeGraphContainer;
    private HugeClient hugeClient;

    @BeforeAll
    @Override
    public void startUp() {
        hugeGraphContainer =
                new GenericContainer<>(DockerImageName.parse(HUGE_GRAPH_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HUGE_GRAPH_HOST)
                        .withExposedPorts(HUGE_GRAPH_PORT)
                        .waitingFor(
                                Wait.forHttp("/graphs").forPort(HUGE_GRAPH_PORT).forStatusCode(200))
                        .withStartupTimeout(Duration.ofMinutes(3));
        Startables.deepStart(Stream.of(hugeGraphContainer)).join();

        String url =
                String.format(
                        "http://%s:%d",
                        hugeGraphContainer.getHost(),
                        hugeGraphContainer.getMappedPort(HUGE_GRAPH_PORT));
        hugeClient = HugeClient.builder(url, GRAPH_NAME).build();
        setupSchema();
    }

    @TestTemplate
    public void testVertexSourceToAssert(TestContainer container)
            throws IOException, InterruptedException {
        clearGraph();
        for (int i = 0; i < 150; i++) {
            addVertex("person-" + i, 29);
        }
        awaitTotalVertexCount(150);

        Container.ExecResult execResult =
                container.executeJob("/hugegraph/hugegraph_vertex_to_assert.conf");

        Assertions.assertEquals(0, execResult.getExitCode(), buildFailureMessage(execResult));
    }

    @TestTemplate
    public void testVertexCheckpointedMultiPageScan(TestContainer container)
            throws IOException, InterruptedException {
        clearGraph();
        // 250 vertices with page_size=100 => 3 pages, so the opaque page token is snapshotted at
        // checkpoints mid-scan (on Zeta). Assert verifies the full scan is read exactly once.
        for (int i = 0; i < 250; i++) {
            addVertex("ckpt-person-" + i, 29);
        }
        awaitTotalVertexCount(250);

        Container.ExecResult execResult =
                container.executeJob("/hugegraph/hugegraph_vertex_checkpoint_to_assert.conf");

        Assertions.assertEquals(0, execResult.getExitCode(), buildFailureMessage(execResult));
    }

    @TestTemplate
    public void testVertexSinkJob(TestContainer container)
            throws IOException, InterruptedException {
        clearGraph();

        Container.ExecResult execResult =
                container.executeJob("/hugegraph/fake_to_hugegraph_vertex.conf");

        Assertions.assertEquals(0, execResult.getExitCode(), buildFailureMessage(execResult));
        awaitTotalVertexCount(100);
        List<Vertex> vertices =
                hugeClient
                        .graph()
                        .listVertices(VERTEX_LABEL, java.util.Collections.emptyMap(), 101);
        Assertions.assertTrue(
                vertices.stream()
                        .allMatch(
                                vertex ->
                                        VERTEX_LABEL.equals(vertex.label())
                                                && vertex.property("name") != null
                                                && vertex.property("age") != null));
    }

    @TestTemplate
    public void testEdgeSinkJob(TestContainer container) throws IOException, InterruptedException {
        clearGraph();

        Container.ExecResult execResult =
                container.executeJob("/hugegraph/fake_to_hugegraph_edge.conf");

        Assertions.assertEquals(0, execResult.getExitCode(), buildFailureMessage(execResult));
        awaitEdgeCount(50);
        Assertions.assertTrue(
                hugeClient.graph().listEdges(EDGE_LABEL).stream()
                        .allMatch(
                                edge ->
                                        EDGE_LABEL.equals(edge.label())
                                                && edge.property("weight") != null));
    }

    @TestTemplate
    public void testMultiMappingFanOutJob(TestContainer container)
            throws IOException, InterruptedException {
        clearGraph();

        Container.ExecResult execResult =
                container.executeJob("/hugegraph/fake_to_hugegraph_multi_mapping.conf");

        Assertions.assertEquals(0, execResult.getExitCode(), buildFailureMessage(execResult));
        awaitLabelVertexCount("person", 25);
        awaitEdgeCount("knows", 25);
    }

    @TestTemplate
    public void testSizeTriggeredFlushWithEdgeFirstMappingOrder(TestContainer container)
            throws IOException, InterruptedException {
        clearGraph();

        Container.ExecResult execResult =
                container.executeJob("/hugegraph/fake_to_hugegraph_multi_mapping_edge_first.conf");

        Assertions.assertEquals(0, execResult.getExitCode(), buildFailureMessage(execResult));
        awaitLabelVertexCount("person", 25);
        awaitEdgeCount("knows", 25);
    }

    @TestTemplate
    public void testEdgeSourceToAssert(TestContainer container)
            throws IOException, InterruptedException {
        clearGraph();
        Vertex marko = addVertex("marko", 29);
        Vertex vadas = addVertex("vadas", 27);
        Edge edge = new Edge(EDGE_LABEL).source(marko).target(vadas).property("weight", 1.5D);
        hugeClient.graph().addEdge(edge);
        awaitVertexCount("marko", 1);
        awaitVertexCount("vadas", 1);
        awaitEdgeCount(1);

        Container.ExecResult execResult =
                container.executeJob("/hugegraph/hugegraph_edge_to_assert.conf");

        Assertions.assertEquals(0, execResult.getExitCode(), buildFailureMessage(execResult));
    }

    @TestTemplate
    public void testByteAndObjectColumnsAreReadable(TestContainer container)
            throws IOException, InterruptedException {
        clearGraph();
        // A BYTE (code) and an OBJECT (meta) property: before the fix these fell through the type
        // converter's default branch and failed schema validation, so the whole label read errored.
        hugeClient
                .graph()
                .addVertex(
                        new Vertex(GADGET_LABEL)
                                .property("name", "g1")
                                .property("code", (byte) 7)
                                .property("meta", "info-1"));
        awaitLabelVertexCount(GADGET_LABEL, 1);

        Container.ExecResult execResult =
                container.executeJob("/hugegraph/hugegraph_byte_object_to_assert.conf");

        Assertions.assertEquals(0, execResult.getExitCode(), buildFailureMessage(execResult));
    }

    private Vertex addVertex(String name, int age) {
        return hugeClient
                .graph()
                .addVertex(new Vertex(VERTEX_LABEL).property("name", name).property("age", age));
    }

    private void clearGraph() {
        clearGraphWithoutSchema();
        setupSchema();
        awaitSchemaReady();
    }

    private void clearGraphWithoutSchema() {
        // The server can close its REST connection while completing a previous graph mutation.
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(Duration.ofSeconds(2))
                .atMost(Duration.ofMinutes(2))
                .untilAsserted(
                        () ->
                                hugeClient
                                        .graphs()
                                        .clearGraph(GRAPH_NAME, "I'm sure to delete all data"));
    }

    private void setupSchema() {
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
                .edgeLabel(EDGE_LABEL)
                .sourceLabel(VERTEX_LABEL)
                .targetLabel(VERTEX_LABEL)
                .properties("weight")
                .nullableKeys("weight")
                .ifNotExist()
                .create();

        // BYTE + OBJECT property columns exercise the two type-converter branches that previously
        // fell through to the default and blocked the whole label read.
        hugeClient.schema().propertyKey("code").asByte().ifNotExist().create();
        hugeClient.schema().propertyKey("meta").dataType(DataType.OBJECT).ifNotExist().create();
        hugeClient
                .schema()
                .vertexLabel(GADGET_LABEL)
                .idStrategy(IdStrategy.PRIMARY_KEY)
                .primaryKeys("name")
                .properties("name", "code", "meta")
                .nullableKeys("code", "meta")
                .ifNotExist()
                .create();
    }

    private void awaitSchemaReady() {
        awaitCondition(
                () ->
                        hugeClient.schema().getVertexLabel(VERTEX_LABEL) != null
                                && hugeClient.schema().getVertexLabel(GADGET_LABEL) != null
                                && hugeClient.schema().getEdgeLabel(EDGE_LABEL) != null
                                && hugeClient.schema().getPropertyKey("name") != null
                                && hugeClient.schema().getPropertyKey("age") != null
                                && hugeClient.schema().getPropertyKey("weight") != null
                                && hugeClient.schema().getPropertyKey("code") != null
                                && hugeClient.schema().getPropertyKey("meta") != null,
                "HugeGraph schema is not ready");
    }

    private void awaitVertexCount(String name, int expectedCount) {
        awaitCondition(
                () -> {
                    Map<String, Object> properties = new HashMap<>();
                    properties.put("name", name);
                    List<Vertex> vertices =
                            hugeClient.graph().listVertices(VERTEX_LABEL, properties, 10);
                    return vertices.size() == expectedCount;
                },
                String.format("Vertex data for name=%s is not ready", name));
    }

    private void awaitTotalVertexCount(int expectedCount) {
        awaitLabelVertexCount(VERTEX_LABEL, expectedCount);
    }

    private void awaitLabelVertexCount(String label, int expectedCount) {
        awaitCondition(
                () ->
                        hugeClient
                                        .graph()
                                        .listVertices(
                                                label,
                                                java.util.Collections.emptyMap(),
                                                expectedCount + 1)
                                        .size()
                                == expectedCount,
                String.format("Expected %s vertices for label %s", expectedCount, label));
    }

    private void awaitEdgeCount(int expectedCount) {
        awaitEdgeCount(EDGE_LABEL, expectedCount);
    }

    private void awaitEdgeCount(String label, int expectedCount) {
        awaitCondition(
                () -> hugeClient.graph().listEdges(label).size() == expectedCount,
                String.format("Expected %s edges for label %s", expectedCount, label));
    }

    private void awaitCondition(Check check, String timeoutMessage) {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                if (check.ok()) {
                    return;
                }
            } catch (Exception ignored) {
                // HugeGraph metadata/data can be eventually visible right after clear/create.
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted while waiting HugeGraph state", interruptedException);
            }
        }
        throw new IllegalStateException(timeoutMessage);
    }

    @FunctionalInterface
    private interface Check {
        boolean ok() throws Exception;
    }

    private String buildFailureMessage(Container.ExecResult execResult) {
        return String.format(
                "Seatunnel job failed with exitCode=%s, stdout=%s, stderr=%s",
                execResult.getExitCode(), execResult.getStdout(), execResult.getStderr());
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (hugeClient != null) {
            hugeClient.close();
        }
        if (hugeGraphContainer != null) {
            hugeGraphContainer.close();
        }
    }
}
