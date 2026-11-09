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
package org.apache.seatunnel.engine.server.dag;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableMap;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSink;
import org.apache.seatunnel.connectors.seatunnel.fake.source.FakeSource;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.Edge;
import org.apache.seatunnel.engine.core.job.ExecutionAddress;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class DAGUtilsTest {

    private final JobContext jobContext = new JobContext(1L);
    private final Map<Long, LogicalVertex> vertices = new HashMap<>();
    private final LogicalDag logicalDag = new LogicalDag(new JobConfig(), new IdGenerator());

    @Test
    void mapsSourceToSinkEdge() {
        source(1);
        sink(2);
        edge(1, 2);

        Assertions.assertEquals(
                Collections.singleton(edges(1, 2)), new HashSet<>(logicalPipelines()));
    }

    @Test
    void mapsEdgesInsideAChainedTransformVertex() {
        source(1);
        transform(2);
        transform(3);
        sink(4);
        edge(1, 2);
        edge(2, 3);
        edge(3, 4);

        Assertions.assertEquals(
                Collections.singleton(edges(1, 2, 2, 3, 3, 4)), new HashSet<>(logicalPipelines()));
    }

    @Test
    void mapsSharedEdgesToEverySplitPipeline() {
        // Two sources into one transform: the union split runs 3 -> 4 in both pipelines.
        source(1);
        source(2);
        transform(3);
        sink(4);
        edge(1, 3);
        edge(2, 3);
        edge(3, 4);

        Assertions.assertEquals(
                set(edges(1, 3, 3, 4), edges(2, 3, 3, 4)), new HashSet<>(logicalPipelines()));
    }

    @Test
    void doesNotAssignAShortcutEdgeToAPipelineWithoutIt() {
        // 2 -> 4 and 2 -> 3 -> 4: the split pipelines both contain 2 and 4, but only one has 2 ->
        // 4.
        source(1);
        transform(2);
        transform(3);
        sink(4);
        edge(1, 2);
        edge(2, 4);
        edge(2, 3);
        edge(3, 4);

        Assertions.assertEquals(
                set(edges(1, 2, 2, 4), edges(1, 2, 2, 3, 3, 4)), new HashSet<>(logicalPipelines()));
    }

    @Test
    void keepsFanOutToSeveralSinksInOnePipeline() {
        source(1);
        sink(2);
        sink(3);
        edge(1, 2);
        edge(1, 3);

        Assertions.assertEquals(
                Collections.singleton(edges(1, 2, 1, 3)), new HashSet<>(logicalPipelines()));
    }

    @Test
    void mapsUnconnectedPipelinesSeparately() {
        source(1);
        sink(2);
        source(3);
        sink(4);
        edge(1, 2);
        edge(3, 4);

        Assertions.assertEquals(set(edges(1, 2), edges(3, 4)), new HashSet<>(logicalPipelines()));
    }

    @Test
    void reportsLogicalVerticesForTheLogicalFlavour() {
        source(1);
        transform(2);
        sink(3);
        edge(1, 2);
        edge(2, 3);

        JobDAGInfo dagInfo = dagInfo(false);

        Assertions.assertEquals(set(1L, 2L, 3L), dagInfo.getVertexInfoMap().keySet());
        Assertions.assertEquals(
                Collections.singleton(edges(1, 2, 2, 3)),
                new HashSet<>(dagInfo.getPipelineEdges().values()));
    }

    @Test
    void leavesThePhysicalFlavourChained() {
        source(1);
        transform(2);
        transform(3);
        sink(4);
        edge(1, 2);
        edge(2, 3);
        edge(3, 4);

        JobDAGInfo dagInfo = dagInfo(true);

        Assertions.assertEquals(3, dagInfo.getVertexInfoMap().size());
        Assertions.assertEquals(2, dagInfo.getPipelineEdges().values().iterator().next().size());
    }

    @Test
    void mapsEveryEdgeOfAParsedJob() {
        // Two FakeSources into one Console sink: the parser builds the plan and the union split
        // gives one pipeline per source.
        LogicalDag parsed = TestUtils.createTestLogicalPlan("fake_to_console.conf", "test", 1L);
        Set<Edge> expected =
                parsed.getEdges().stream()
                        .map(e -> new Edge(e.getInputVertexId(), e.getTargetVertexId()))
                        .collect(Collectors.toSet());

        JobDAGInfo dagInfo = dagInfo(parsed, false);

        Assertions.assertEquals(2, dagInfo.getPipelineEdges().size());
        Assertions.assertEquals(
                expected,
                dagInfo.getPipelineEdges().values().stream()
                        .flatMap(List::stream)
                        .collect(Collectors.toSet()));
    }

    private Collection<List<Edge>> logicalPipelines() {
        return dagInfo(false).getPipelineEdges().values();
    }

    private JobDAGInfo dagInfo(boolean physical) {
        return dagInfo(logicalDag, physical);
    }

    private static JobDAGInfo dagInfo(LogicalDag logicalDag, boolean physical) {
        JobImmutableInformation information =
                new JobImmutableInformation(
                        1L,
                        "test",
                        new DefaultSerializationServiceBuilder().build(),
                        logicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());
        return DAGUtils.getJobDAGInfo(
                logicalDag,
                information,
                new EngineConfig(),
                physical,
                new ExecutionAddress("localhost", 5801),
                new HashSet<>());
    }

    private void source(long id) {
        FakeSource fakeSource =
                new FakeSource(
                        ReadonlyConfig.fromConfig(
                                ConfigFactory.parseMap(
                                        Collections.singletonMap(
                                                "schema",
                                                Collections.singletonMap(
                                                        "fields", ImmutableMap.of("id", "int"))))));
        fakeSource.setJobContext(jobContext);
        add(
                new SourceAction<>(
                        id,
                        "Source[" + id + "]",
                        fakeSource,
                        Collections.emptySet(),
                        Collections.emptySet()));
    }

    private void transform(long id) {
        add(
                new TransformAction(
                        id,
                        "Transform[" + id + "]",
                        new StubTransform(),
                        Collections.emptySet(),
                        Collections.emptySet()));
    }

    private void sink(long id) {
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("default", TablePath.DEFAULT),
                        TableSchema.builder()
                                .columns(
                                        Collections.singletonList(
                                                PhysicalColumn.of(
                                                        "id",
                                                        BasicType.INT_TYPE,
                                                        11L,
                                                        0,
                                                        true,
                                                        null,
                                                        "")))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "");
        ConsoleSink consoleSink = new ConsoleSink(table, ReadonlyConfig.fromMap(new HashMap<>()));
        consoleSink.setJobContext(jobContext);
        add(
                new SinkAction<>(
                        id,
                        "Sink[" + id + "]",
                        consoleSink,
                        Collections.emptySet(),
                        Collections.emptySet()));
    }

    private void add(Action action) {
        action.setParallelism(1);
        LogicalVertex vertex = new LogicalVertex(action.getId(), action, 1);
        vertices.put(action.getId(), vertex);
        logicalDag.addLogicalVertex(vertex);
    }

    private void edge(long input, long target) {
        logicalDag.addEdge(new LogicalEdge(vertices.get(input), vertices.get(target)));
    }

    private static List<Edge> edges(long... pairs) {
        List<Edge> edges =
                Arrays.stream(pairsOf(pairs))
                        .map(pair -> new Edge(pair[0], pair[1]))
                        .collect(Collectors.toList());
        edges.sort(
                (a, b) ->
                        a.getInputVertexId().equals(b.getInputVertexId())
                                ? a.getTargetVertexId().compareTo(b.getTargetVertexId())
                                : a.getInputVertexId().compareTo(b.getInputVertexId()));
        return edges;
    }

    private static long[][] pairsOf(long[] values) {
        long[][] pairs = new long[values.length / 2][];
        for (int i = 0; i < pairs.length; i++) {
            pairs[i] = new long[] {values[2 * i], values[2 * i + 1]};
        }
        return pairs;
    }

    @SafeVarargs
    private static <T> Set<T> set(T... values) {
        return new HashSet<>(Arrays.asList(values));
    }

    private static class StubTransform implements SeaTunnelTransform<Object> {
        @Override
        public CatalogTable getProducedCatalogTable() {
            return null;
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.emptyList();
        }

        @Override
        public String getPluginName() {
            return "stub";
        }
    }
}
