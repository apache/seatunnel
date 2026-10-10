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

package org.apache.seatunnel.engine.server.rest.lineage;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.Edge;
import org.apache.seatunnel.engine.core.job.ExecutionAddress;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.VertexInfo;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.dag.DAGUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class JobLineageProjectorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String SECRET = "sentinel-secret-value";

    @Test
    void projectsSortedWhitelistedGraph() {
        Map<Long, VertexInfo> vertices = new LinkedHashMap<>();
        vertices.put(3L, vertex(3L, PluginType.SINK, "pipeline-1 [sink]", "warehouse.orders"));
        vertices.put(1L, vertex(1L, PluginType.SOURCE, "pipeline-1 [source]", "sales.orders"));
        vertices.put(2L, vertex(2L, PluginType.TRANSFORM, "pipeline-1 [transform]"));
        Map<Integer, List<Edge>> edges = new HashMap<>();
        edges.put(1, Arrays.asList(new Edge(2L, 3L), new Edge(1L, 2L)));

        String json = projectToString(dag(733584788375093248L, vertices, edges));

        Assertions.assertEquals(
                "{\"schemaVersion\":1,\"jobId\":\"733584788375093248\",\"graphKind\":\"EXECUTION\","
                        + "\"idScope\":\"JOB\",\"nodes\":["
                        + "{\"id\":\"1\",\"kind\":\"SOURCE\",\"name\":\"pipeline-1 [source]\","
                        + "\"tablePaths\":[\"sales.orders\"],\"datasetMetadata\":\"REPORTED\"},"
                        + "{\"id\":\"2\",\"kind\":\"TRANSFORM\",\"name\":\"pipeline-1 [transform]\","
                        + "\"tablePaths\":[],\"datasetMetadata\":\"NOT_APPLICABLE\"},"
                        + "{\"id\":\"3\",\"kind\":\"SINK\",\"name\":\"pipeline-1 [sink]\","
                        + "\"tablePaths\":[\"warehouse.orders\"],\"datasetMetadata\":\"REPORTED\"}],"
                        + "\"edges\":["
                        + "{\"pipelineId\":1,\"sourceNodeId\":\"1\",\"targetNodeId\":\"2\"},"
                        + "{\"pipelineId\":1,\"sourceNodeId\":\"2\",\"targetNodeId\":\"3\"}],"
                        + "\"warnings\":[]}",
                json);
    }

    @Test
    void projectsSnapshotsGeneratedByTheEngine() throws IOException {
        long jobId = 733584788375093248L;
        LogicalDag logicalDag =
                TestUtils.createTestLogicalPlan("fake_to_console.conf", "lineage", jobId);
        JobImmutableInformation jobInformation =
                new JobImmutableInformation(
                        jobId,
                        "lineage",
                        new DefaultSerializationServiceBuilder().build(),
                        logicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        // All engine callers build the execution (physical) DAG info.
        JobDAGInfo dagInfo =
                DAGUtils.getJobDAGInfo(
                        logicalDag,
                        jobInformation,
                        new EngineConfig(),
                        true,
                        new ExecutionAddress(SECRET, 5801),
                        new HashSet<>());
        String json = projectToString(dagInfo);
        JsonNode root = MAPPER.readTree(json);

        Assertions.assertFalse(json.contains(SECRET));
        Assertions.assertEquals(Long.toString(jobId), root.get("jobId").asText());
        Assertions.assertEquals(4, root.get("nodes").size());
        Assertions.assertEquals(
                Arrays.asList("SOURCE", "SINK", "SOURCE", "SINK"),
                Arrays.asList(
                        root.get("nodes").get(0).get("kind").asText(),
                        root.get("nodes").get(1).get("kind").asText(),
                        root.get("nodes").get(2).get("kind").asText(),
                        root.get("nodes").get(3).get("kind").asText()));
        Assertions.assertEquals(
                "[\"fake2\"]", root.get("nodes").get(2).get("tablePaths").toString());
        Assertions.assertEquals(
                Arrays.asList("1:1>2", "2:3>4"),
                Arrays.asList(
                        edgeKey(root.get("edges").get(0)), edgeKey(root.get("edges").get(1))));
        Assertions.assertEquals(0, root.get("warnings").size());
        Assertions.assertEquals(json, projectToString(dagInfo));
    }

    @Test
    void projectsTransformFromPhysicalDagInfo() throws IOException {
        long jobId = 733584788375093248L;
        LogicalDag logicalDag =
                TestUtils.createTestLogicalPlan("lineage_transform.conf", "lineage", jobId);
        JobImmutableInformation jobInformation =
                new JobImmutableInformation(
                        jobId,
                        "lineage",
                        new DefaultSerializationServiceBuilder().build(),
                        logicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        JobDAGInfo physicalInfo =
                DAGUtils.getJobDAGInfo(
                        logicalDag,
                        jobInformation,
                        new EngineConfig(),
                        true,
                        null,
                        Collections.emptySet());
        JsonNode root = MAPPER.readTree(projectToString(physicalInfo));

        Assertions.assertEquals(Long.toString(jobId), root.get("jobId").asText());
        Assertions.assertEquals(3, root.get("nodes").size(), root.toString());
        Assertions.assertEquals(2, root.get("edges").size(), root.toString());
        JsonNode source = root.get("nodes").get(0);
        JsonNode transform = root.get("nodes").get(1);
        JsonNode sink = root.get("nodes").get(2);
        Assertions.assertEquals("SOURCE", source.get("kind").asText());
        Assertions.assertEquals("TRANSFORM", transform.get("kind").asText());
        Assertions.assertTrue(
                transform
                        .get("name")
                        .asText()
                        .contains("Transform[0]-FilterRowKind->Transform[1]-FilterRowKind"));
        Assertions.assertEquals("NOT_APPLICABLE", transform.get("datasetMetadata").asText());
        Assertions.assertEquals(0, transform.get("tablePaths").size());
        Assertions.assertEquals("SINK", sink.get("kind").asText());
        Assertions.assertEquals(
                Arrays.asList(
                        "1:" + source.get("id").asText() + ">" + transform.get("id").asText(),
                        "1:" + transform.get("id").asText() + ">" + sink.get("id").asText()),
                Arrays.asList(
                        edgeKey(root.get("edges").get(0)), edgeKey(root.get("edges").get(1))));
        Assertions.assertEquals(0, root.get("warnings").size());
        Assertions.assertEquals(
                root.toString(),
                MAPPER.readTree(
                                projectToString(
                                        DAGUtils.getJobDAGInfo(
                                                logicalDag,
                                                jobInformation,
                                                new EngineConfig(),
                                                true,
                                                null,
                                                Collections.emptySet())))
                        .toString());
    }

    @Test
    void outputIsIndependentOfInputOrder() {
        Map<Long, VertexInfo> forward = new LinkedHashMap<>();
        Map<Long, VertexInfo> reverse = new LinkedHashMap<>();
        for (long id = 1; id <= 20; id++) {
            forward.put(id, vertex(id, id == 1 ? PluginType.SOURCE : PluginType.SINK, "v" + id));
        }
        for (long id = 20; id >= 1; id--) {
            reverse.put(id, forward.get(id));
        }
        List<Edge> edges = new ArrayList<>();
        for (long id = 2; id <= 20; id++) {
            edges.add(new Edge(1L, id));
        }
        List<Edge> reversedEdges = new ArrayList<>(edges);
        Collections.reverse(reversedEdges);

        Assertions.assertEquals(
                projectToString(dag(7L, forward, Collections.singletonMap(1, edges))),
                projectToString(dag(7L, reverse, Collections.singletonMap(1, reversedEdges))));
    }

    @Test
    void keepsPipelineMembershipAndDeduplicatesIdenticalEdges() throws IOException {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(1L, vertex(1L, PluginType.SOURCE, "source", "db.a"));
        vertices.put(2L, vertex(2L, PluginType.SINK, "sink-1", "db.b"));
        vertices.put(3L, vertex(3L, PluginType.SINK, "sink-2", "db.c"));
        Map<Integer, List<Edge>> edges = new HashMap<>();
        edges.put(2, Arrays.asList(new Edge(1L, 3L), new Edge(1L, 2L), new Edge(1L, 2L)));
        edges.put(1, Collections.singletonList(new Edge(1L, 2L)));

        JsonNode edgesNode =
                MAPPER.readTree(projectToString(dag(1L, vertices, edges))).get("edges");

        Assertions.assertEquals(3, edgesNode.size());
        Assertions.assertEquals(
                Arrays.asList("1:1>2", "2:1>2", "2:1>3"),
                Arrays.asList(
                        edgeKey(edgesNode.get(0)),
                        edgeKey(edgesNode.get(1)),
                        edgeKey(edgesNode.get(2))));
    }

    @Test
    void reportsMissingDatasetMetadataWithoutDroppingTopology() throws IOException {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(
                1L,
                new VertexInfo(
                        1L,
                        PluginType.SOURCE,
                        "source",
                        Arrays.asList(
                                TablePath.of("db.b"),
                                null,
                                TablePath.DEFAULT,
                                TablePath.of("db.a"),
                                TablePath.of("db.b"))));
        vertices.put(2L, vertex(2L, PluginType.TRANSFORM, "transform", "ignored.path"));
        vertices.put(3L, new VertexInfo(3L, PluginType.SINK, null, null));
        Map<Integer, List<Edge>> edges =
                Collections.singletonMap(1, Arrays.asList(new Edge(1L, 2L), new Edge(2L, 3L)));

        JsonNode root = MAPPER.readTree(projectToString(dag(1L, vertices, edges)));

        JsonNode source = root.get("nodes").get(0);
        Assertions.assertEquals("[\"db.a\",\"db.b\"]", source.get("tablePaths").toString());
        Assertions.assertEquals("PARTIAL", source.get("datasetMetadata").asText());
        JsonNode transform = root.get("nodes").get(1);
        Assertions.assertEquals(0, transform.get("tablePaths").size());
        Assertions.assertEquals("NOT_APPLICABLE", transform.get("datasetMetadata").asText());
        JsonNode sink = root.get("nodes").get(2);
        Assertions.assertEquals("", sink.get("name").asText());
        Assertions.assertEquals("UNAVAILABLE", sink.get("datasetMetadata").asText());
        Assertions.assertEquals(2, root.get("edges").size());
        Assertions.assertEquals(
                "[{\"code\":\"DATASET_METADATA_PARTIAL\",\"nodeId\":\"1\"},"
                        + "{\"code\":\"DATASET_METADATA_UNAVAILABLE\",\"nodeId\":\"3\"}]",
                root.get("warnings").toString());
    }

    @Test
    void writesLargeIdentifiersAsDecimalStrings() throws IOException {
        long source = Long.MAX_VALUE - 1;
        long sink = Long.MAX_VALUE;
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(source, vertex(source, PluginType.SOURCE, "source", "db.a"));
        vertices.put(sink, vertex(sink, PluginType.SINK, "sink", "db.b"));

        JsonNode root =
                MAPPER.readTree(
                        projectToString(
                                dag(
                                        Long.MAX_VALUE,
                                        vertices,
                                        Collections.singletonMap(
                                                Integer.MAX_VALUE,
                                                Collections.singletonList(
                                                        new Edge(source, sink))))));

        Assertions.assertEquals(Long.toString(Long.MAX_VALUE), root.get("jobId").asText());
        Assertions.assertEquals(Long.toString(source), root.get("nodes").get(0).get("id").asText());
        Assertions.assertEquals(
                Long.toString(sink), root.get("edges").get(0).get("targetNodeId").asText());
    }

    @Test
    void neverWritesFieldsOutsideTheWhitelist() throws IOException {
        Map<String, Object> envOptions = new HashMap<>();
        envOptions.put("password", SECRET);
        JobDAGInfo dagInfo =
                new JobDAGInfo(
                        1L,
                        envOptions,
                        Collections.singletonMap(1, Collections.singletonList(new Edge(1L, 2L))),
                        twoVertices(),
                        new ExecutionAddress(SECRET, 5801),
                        new HashSet<>(
                                Collections.singletonList(new ExecutionAddress(SECRET, 5802))));

        String json = projectToString(dagInfo);
        JsonNode root = MAPPER.readTree(json);

        Assertions.assertFalse(json.contains(SECRET));
        Assertions.assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                "schemaVersion",
                                "jobId",
                                "graphKind",
                                "idScope",
                                "nodes",
                                "edges",
                                "warnings")),
                fieldNames(root));
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList("id", "kind", "name", "tablePaths", "datasetMetadata")),
                fieldNames(root.get("nodes").get(0)));
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList("pipelineId", "sourceNodeId", "targetNodeId")),
                fieldNames(root.get("edges").get(0)));
    }

    @Test
    void doesNotModifyTheSnapshot() {
        JobDAGInfo dagInfo =
                dag(
                        1L,
                        twoVertices(),
                        Collections.singletonMap(
                                1, Arrays.asList(new Edge(1L, 2L), new Edge(1L, 2L))));
        String before = dagInfo.toString();

        projectToString(dagInfo);

        Assertions.assertEquals(before, dagInfo.toString());
    }

    @Test
    void ordersWarningsByCodeThenNodeId() throws IOException {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(1L, vertex(1L, PluginType.SOURCE, "source"));
        vertices.put(
                2L,
                new VertexInfo(
                        2L,
                        PluginType.SINK,
                        "sink",
                        Arrays.asList(TablePath.of("db.b"), TablePath.DEFAULT)));
        vertices.put(3L, vertex(3L, PluginType.SINK, "sink-2", "db.c", "db.d"));
        vertices.put(10L, vertex(10L, PluginType.SINK, "sink-3"));
        Map<Integer, List<Edge>> edges =
                Collections.singletonMap(
                        1, Arrays.asList(new Edge(1L, 2L), new Edge(1L, 3L), new Edge(1L, 10L)));

        JsonNode root = MAPPER.readTree(projectToString(dag(1L, vertices, edges)));

        Assertions.assertEquals(
                "REPORTED", root.get("nodes").get(2).get("datasetMetadata").asText());
        Assertions.assertEquals(
                "[\"db.c\",\"db.d\"]", root.get("nodes").get(2).get("tablePaths").toString());
        Assertions.assertEquals(
                "[{\"code\":\"DATASET_METADATA_PARTIAL\",\"nodeId\":\"2\"},"
                        + "{\"code\":\"DATASET_METADATA_UNAVAILABLE\",\"nodeId\":\"1\"},"
                        + "{\"code\":\"DATASET_METADATA_UNAVAILABLE\",\"nodeId\":\"10\"}]",
                root.get("warnings").toString());
    }

    @Test
    void acceptsAnyNonNullJobId() throws IOException {
        for (long jobId : new long[] {0L, -1L, Long.MIN_VALUE}) {
            JsonNode root =
                    MAPPER.readTree(
                            projectToString(
                                    dag(
                                            jobId,
                                            twoVertices(),
                                            Collections.singletonMap(1, edgeList(1L, 2L)))));
            Assertions.assertEquals(Long.toString(jobId), root.get("jobId").asText());
        }
    }

    @Test
    void requiresLimits() {
        NullPointerException e =
                Assertions.assertThrows(
                        NullPointerException.class,
                        () ->
                                JobLineageProjector.project(
                                        dag(1L, twoVertices(), Collections.emptyMap()), null));
        Assertions.assertEquals("limits", e.getMessage());
    }

    @Test
    void rejectsSnapshotsWithoutUsableTopology() {
        assertUnavailable(null);
        assertUnavailable(
                new JobDAGInfo(null, null, Collections.emptyMap(), twoVertices(), null, null));
        assertUnavailable(dag(1L, new HashMap<>(), Collections.emptyMap()));
        assertUnavailable(new JobDAGInfo(1L, null, null, twoVertices(), null, null));
        assertUnavailable(dag(1L, twoVertices(), Collections.singletonMap(1, null)));
        assertUnavailable(dag(1L, twoVertices(), Collections.singletonMap(null, edgeList(1L, 2L))));
    }

    @Test
    void rejectsInconsistentVertices() {
        Map<Long, VertexInfo> mismatchedKey = new HashMap<>();
        mismatchedKey.put(1L, vertex(2L, PluginType.SOURCE, "source"));
        assertUnavailable(dag(1L, mismatchedKey, Collections.emptyMap()));

        Map<Long, VertexInfo> missingType = new HashMap<>();
        missingType.put(1L, new VertexInfo(1L, null, "source", null));
        assertUnavailable(dag(1L, missingType, Collections.emptyMap()));

        Map<Long, VertexInfo> nullVertex = new HashMap<>();
        nullVertex.put(1L, null);
        assertUnavailable(dag(1L, nullVertex, Collections.emptyMap()));
    }

    @Test
    void rejectsInvalidEdges() {
        assertUnavailable(dag(1L, twoVertices(), Collections.singletonMap(1, edgeList(null, 2L))));
        assertUnavailable(dag(1L, twoVertices(), Collections.singletonMap(1, edgeList(1L, 9L))));
        assertUnavailable(dag(1L, twoVertices(), Collections.singletonMap(1, edgeList(1L, null))));
        assertUnavailable(dag(1L, twoVertices(), Collections.singletonMap(1, edgeList(9L, 2L))));
        assertUnavailable(dag(1L, twoVertices(), Collections.singletonMap(1, edgeList(1L, 1L))));
        assertUnavailable(
                dag(
                        1L,
                        twoVertices(),
                        Collections.singletonMap(1, Collections.singletonList(null))));
    }

    @Test
    void rejectsCyclesAcrossPipelines() {
        Map<Long, VertexInfo> vertices = twoVertices();
        vertices.put(3L, vertex(3L, PluginType.TRANSFORM, "transform"));
        Map<Integer, List<Edge>> edges = new HashMap<>();
        edges.put(1, Arrays.asList(new Edge(1L, 3L), new Edge(3L, 2L)));
        edges.put(2, Collections.singletonList(new Edge(2L, 3L)));

        assertUnavailable(dag(1L, vertices, edges));
    }

    @Test
    void rejectsCyclesWithinOnePipeline() {
        Map<Long, VertexInfo> vertices = twoVertices();
        vertices.put(3L, vertex(3L, PluginType.TRANSFORM, "transform"));
        Map<Integer, List<Edge>> edges =
                Collections.singletonMap(
                        1, Arrays.asList(new Edge(1L, 3L), new Edge(3L, 2L), new Edge(2L, 3L)));

        assertUnavailable(dag(1L, vertices, edges));
    }

    @Test
    void reportsDefaultOnlyPathAsUnavailable() throws IOException {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(
                1L,
                new VertexInfo(
                        1L,
                        PluginType.SOURCE,
                        "source",
                        Collections.singletonList(TablePath.DEFAULT)));

        JsonNode root = MAPPER.readTree(projectToString(dag(1L, vertices, Collections.emptyMap())));

        Assertions.assertEquals(0, root.get("nodes").get(0).get("tablePaths").size());
        Assertions.assertEquals(
                "UNAVAILABLE", root.get("nodes").get(0).get("datasetMetadata").asText());
        Assertions.assertEquals(
                "[{\"code\":\"DATASET_METADATA_UNAVAILABLE\",\"nodeId\":\"1\"}]",
                root.get("warnings").toString());
    }

    @Test
    void acceptsLongChainsWithoutRecursion() {
        int length = 10_000;
        Map<Long, VertexInfo> vertices = new HashMap<>();
        List<Edge> edges = new ArrayList<>();
        for (long id = 1; id <= length; id++) {
            vertices.put(id, vertex(id, PluginType.TRANSFORM, "t"));
            if (id > 1) {
                edges.add(new Edge(id - 1, id));
            }
        }

        Assertions.assertDoesNotThrow(
                () ->
                        JobLineageProjector.project(
                                dag(1L, vertices, Collections.singletonMap(1, edges)),
                                JobLineageLimits.DEFAULT));
    }

    @Test
    void enforcesStructuralLimitsBeforeDeduplication() {
        JobDAGInfo dagInfo =
                dag(
                        1L,
                        twoVertices(),
                        Collections.singletonMap(
                                1, Arrays.asList(new Edge(1L, 2L), new Edge(1L, 2L))));

        Assertions.assertDoesNotThrow(
                () -> JobLineageProjector.project(dagInfo, limits(2, 2, 2, 64, 4096)));
        assertTooLarge(dagInfo, limits(1, 2, 2, 64, 4096));
        assertTooLarge(dagInfo, limits(2, 1, 2, 64, 4096));
        assertTooLarge(dagInfo, limits(2, 2, 1, 64, 4096));
    }

    @Test
    void countsAllTablePathsOnOneVertex() {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(1L, vertex(1L, PluginType.SOURCE, "source", "db.a", "db.b", "db.c"));
        JobDAGInfo dagInfo = dag(1L, vertices, Collections.emptyMap());

        Assertions.assertDoesNotThrow(
                () -> JobLineageProjector.project(dagInfo, limits(1, 1, 3, 64, 4096)));
        assertTooLarge(dagInfo, limits(1, 1, 2, 64, 4096));
    }

    @Test
    void enforcesStringLimitInUtf8Bytes() {
        // Each character below is three bytes in UTF-8.
        String exact = repeat("表", 4);
        String oneOver = exact + "a";

        Assertions.assertDoesNotThrow(
                () ->
                        JobLineageProjector.project(
                                singleSource(exact, "db.t"), limits(10, 10, 10, 12, 4096)));
        assertTooLarge(singleSource(oneOver, "db.t"), limits(10, 10, 10, 12, 4096));
        assertTooLarge(singleSource("s", "db." + oneOver), limits(10, 10, 10, 12, 4096));
    }

    @Test
    void enforcesResponseLimitIncludingEscaping() {
        JobDAGInfo dagInfo = singleSource(repeat("\"", 30), "db.t");
        int size = JobLineageProjector.project(dagInfo, JobLineageLimits.DEFAULT).length;

        Assertions.assertEquals(
                size, JobLineageProjector.project(dagInfo, limits(10, 10, 10, 64, size)).length);
        assertTooLarge(dagInfo, limits(10, 10, 10, 64, size - 1));
    }

    @Test
    void enforcesResponseLimitInTheMiddleOfALargeDocument() {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        for (long id = 1; id <= 2_000; id++) {
            vertices.put(id, vertex(id, PluginType.SOURCE, "source-" + id, "db.t" + id));
        }
        JobDAGInfo dagInfo = dag(1L, vertices, Collections.emptyMap());
        int size = JobLineageProjector.project(dagInfo, JobLineageLimits.DEFAULT).length;

        Assertions.assertTrue(size > 64 * 1024);
        assertTooLarge(dagInfo, limits(2_000, 1, 2_000, 64, size - 1));
        assertTooLarge(dagInfo, limits(2_000, 1, 2_000, 64, size / 2));
    }

    @Test
    void rejectsOneLargeVertexBeforeRetainingAllPaths() {
        int reported = 1_000;
        int allowedReads = 10;
        // Fails the test if the projector reads past the point where the byte limit is exceeded.
        List<TablePath> paths =
                new AbstractList<TablePath>() {
                    @Override
                    public TablePath get(int index) {
                        if (index >= allowedReads) {
                            throw new AssertionError("read path " + index + " after the limit");
                        }
                        return TablePath.of("db.t" + repeat("x", 40) + index);
                    }

                    @Override
                    public int size() {
                        return reported;
                    }
                };
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(1L, new VertexInfo(1L, PluginType.SOURCE, "source", paths));

        assertTooLarge(dag(1L, vertices, Collections.emptyMap()), limits(1, 1, reported, 64, 200));
    }

    @Test
    void rejectsRetainedStringsLargerThanTheResponseLimitBeforeSerializing() {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(1L, vertex(1L, PluginType.SOURCE, repeat("n", 60), "db." + repeat("p", 60)));

        // Each string fits the per-string limit, but together they exceed the response limit.
        assertTooLarge(dag(1L, vertices, Collections.emptyMap()), limits(1, 1, 1, 64, 100));
    }

    private static JobDAGInfo singleSource(String name, String path) {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(1L, vertex(1L, PluginType.SOURCE, name, path));
        return dag(1L, vertices, Collections.emptyMap());
    }

    private static Map<Long, VertexInfo> twoVertices() {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(1L, vertex(1L, PluginType.SOURCE, "source", "db.a"));
        vertices.put(2L, vertex(2L, PluginType.SINK, "sink", "db.b"));
        return vertices;
    }

    private static VertexInfo vertex(long id, PluginType type, String name, String... paths) {
        List<TablePath> tablePaths = new ArrayList<>();
        for (String path : paths) {
            tablePaths.add(TablePath.of(path));
        }
        return new VertexInfo(id, type, name, tablePaths);
    }

    private static JobDAGInfo dag(
            long jobId, Map<Long, VertexInfo> vertices, Map<Integer, List<Edge>> edges) {
        return new JobDAGInfo(jobId, Collections.emptyMap(), edges, vertices, null, null);
    }

    private static List<Edge> edgeList(Long source, Long target) {
        return Collections.singletonList(new Edge(source, target));
    }

    private static JobLineageLimits limits(
            int nodes, int edges, int paths, int stringBytes, int responseBytes) {
        return new JobLineageLimits(nodes, edges, paths, stringBytes, responseBytes);
    }

    private static String projectToString(JobDAGInfo dagInfo) {
        return new String(
                JobLineageProjector.project(dagInfo, JobLineageLimits.DEFAULT),
                StandardCharsets.UTF_8);
    }

    private static void assertUnavailable(JobDAGInfo dagInfo) {
        JobLineageException e =
                Assertions.assertThrows(
                        JobLineageException.class,
                        () -> JobLineageProjector.project(dagInfo, JobLineageLimits.DEFAULT));
        Assertions.assertEquals(JobLineageException.Reason.LINEAGE_UNAVAILABLE, e.getReason());
    }

    private static void assertTooLarge(JobDAGInfo dagInfo, JobLineageLimits limits) {
        JobLineageException e =
                Assertions.assertThrows(
                        JobLineageException.class,
                        () -> JobLineageProjector.project(dagInfo, limits));
        Assertions.assertEquals(JobLineageException.Reason.LINEAGE_GRAPH_TOO_LARGE, e.getReason());
    }

    private static String edgeKey(JsonNode edge) {
        return edge.get("pipelineId").asInt()
                + ":"
                + edge.get("sourceNodeId").asText()
                + ">"
                + edge.get("targetNodeId").asText();
    }

    private static Set<String> fieldNames(JsonNode node) {
        Set<String> names = new HashSet<>();
        Iterator<String> it = node.fieldNames();
        while (it.hasNext()) {
            names.add(it.next());
        }
        return names;
    }

    private static String repeat(String value, int times) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < times; i++) {
            builder.append(value);
        }
        return builder.toString();
    }
}
