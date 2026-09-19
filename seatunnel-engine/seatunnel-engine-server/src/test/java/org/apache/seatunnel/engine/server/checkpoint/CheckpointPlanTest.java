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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableMap;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSink;
import org.apache.seatunnel.connectors.seatunnel.fake.source.FakeSource;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.server.QueueType;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupAction;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupDescriptor;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupProjectionField;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupSideSpec;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDagGenerator;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.dag.physical.ChannelAttemptId;
import org.apache.seatunnel.engine.server.dag.physical.InputPortDescriptor;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.PlanUtils;
import org.apache.seatunnel.engine.server.task.DynamicLookupCoordinatorTask;
import org.apache.seatunnel.engine.server.task.SourceSeaTunnelTask;
import org.apache.seatunnel.engine.server.task.TransformSeaTunnelTask;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.map.IMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class CheckpointPlanTest extends AbstractSeaTunnelServerTest {

    @Test
    public void testGenerateCheckpointPlan() {
        final IdGenerator idGenerator = new IdGenerator();
        JobConfig config = new JobConfig();
        config.setName("test");
        config.getEnvOptions().put("engine.observability.split_sink_io", true);
        final LogicalDag logicalDag = new LogicalDag(config, idGenerator);
        fillVirtualVertex(idGenerator, logicalDag, 2);
        fillVirtualVertex(idGenerator, logicalDag, 3);

        JobImmutableInformation jobInfo =
                new JobImmutableInformation(
                        1,
                        "Test",
                        nodeEngine.getSerializationService(),
                        logicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        IMap<Object, Object> runningJobState =
                nodeEngine.getHazelcastInstance().getMap("testRunningJobState");
        IMap<Object, Long[]> runningJobStateTimestamp =
                nodeEngine.getHazelcastInstance().getMap("testRunningJobStateTimestamp");

        Map<Integer, CheckpointPlan> checkpointPlans =
                PlanUtils.fromLogicalDAG(
                                logicalDag,
                                nodeEngine,
                                jobInfo,
                                System.currentTimeMillis(),
                                Executors.newCachedThreadPool(),
                                server.getClassLoaderService(),
                                instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME),
                                runningJobState,
                                runningJobStateTimestamp,
                                QueueType.BLOCKINGQUEUE,
                                new EngineConfig())
                        .f1();
        Assertions.assertNotNull(checkpointPlans);
        Assertions.assertEquals(2, checkpointPlans.size());
        // enum(1) + reader(2) + writer(2)
        Assertions.assertEquals(5, checkpointPlans.get(1).getPipelineSubtasks().size());
        // enum
        Assertions.assertEquals(1, checkpointPlans.get(1).getStartingSubtasks().size());
        // enum + reader
        Assertions.assertEquals(2, checkpointPlans.get(1).getPipelineActions().size());
        // enum(1) + reader(3) + writer(3)
        Assertions.assertEquals(7, checkpointPlans.get(2).getPipelineSubtasks().size());
        // enum
        Assertions.assertEquals(1, checkpointPlans.get(2).getStartingSubtasks().size());
        // enum + reader
        Assertions.assertEquals(2, checkpointPlans.get(2).getPipelineActions().size());
    }

    @Test
    public void testGenerateDynamicLookupPhysicalAndCheckpointPlan() {
        IdGenerator idGenerator = new IdGenerator();
        JobContext jobContext = new JobContext(2L);
        jobContext.setJobMode(JobMode.STREAMING);
        JobConfig config = new JobConfig();
        config.setName("dynamic-lookup-phase-zero");
        config.setJobContext(jobContext);

        Action fact =
                new SourceAction<>(
                        idGenerator.getNextId(),
                        "fact",
                        createFakeSource(jobContext),
                        Collections.emptySet(),
                        Collections.emptySet());
        fact.setParallelism(2);
        Action dimension =
                new SourceAction<>(
                        idGenerator.getNextId(),
                        "dimension",
                        createFakeSource(jobContext),
                        Collections.emptySet(),
                        Collections.emptySet());
        dimension.setParallelism(2);
        DynamicLookupAction lookup =
                new DynamicLookupAction(
                        idGenerator.getNextId(),
                        "lookup",
                        "orders-customer-lookup",
                        fact,
                        "orders",
                        dimension,
                        "customer_dimension",
                        lookupDescriptor("enriched_orders"),
                        lookupCatalogTable("enriched_orders"),
                        512L * 1024L * 1024L,
                        512L * 1024L * 1024L,
                        Collections.emptySet(),
                        Collections.emptySet());
        lookup.setParallelism(2);
        LogicalDag logicalDag =
                new LogicalDagGenerator(Collections.singletonList(lookup), config, idGenerator)
                        .generate();
        JobImmutableInformation jobInfo =
                new JobImmutableInformation(
                        2L,
                        "Dynamic Lookup Phase Zero",
                        nodeEngine.getSerializationService(),
                        logicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        IMap<Object, Object> runningJobState =
                nodeEngine.getHazelcastInstance().getMap("dynamicLookupRunningJobState");
        IMap<Object, Long[]> runningJobStateTimestamp =
                nodeEngine.getHazelcastInstance().getMap("dynamicLookupRunningJobStateTimestamp");
        com.hazelcast.jet.datamodel.Tuple2<PhysicalPlan, Map<Integer, CheckpointPlan>> plans =
                PlanUtils.fromLogicalDAG(
                        logicalDag,
                        nodeEngine,
                        jobInfo,
                        System.currentTimeMillis(),
                        Executors.newCachedThreadPool(),
                        server.getClassLoaderService(),
                        instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME),
                        runningJobState,
                        runningJobStateTimestamp,
                        QueueType.BLOCKINGQUEUE,
                        new EngineConfig());

        Assertions.assertEquals(1, plans.f0().getPipelineList().size());
        List<PhysicalVertex> physicalVertices =
                plans.f0().getPipelineList().get(0).getPhysicalVertexList();
        List<PhysicalVertex> coordinatorVertices =
                plans.f0().getPipelineList().get(0).getCoordinatorVertexList();
        Assertions.assertEquals(2, physicalVertices.size());
        Assertions.assertEquals(3, coordinatorVertices.size());

        List<PhysicalVertex> lookupPhysicalVertices =
                physicalVertices.stream()
                        .filter(vertex -> !vertex.getInputPorts().isEmpty())
                        .collect(Collectors.toList());
        Assertions.assertEquals(2, lookupPhysicalVertices.size());
        for (PhysicalVertex lookupVertex : lookupPhysicalVertices) {
            Map<Integer, InputPortDescriptor> ports =
                    lookupVertex.getInputPorts().stream()
                            .collect(
                                    Collectors.toMap(
                                            InputPortDescriptor::getInputPortId,
                                            descriptor -> descriptor));
            Assertions.assertEquals(
                    new HashSet<>(
                            Arrays.asList(
                                    DynamicLookupAction.FACT_INPUT,
                                    DynamicLookupAction.DIMENSION_INPUT)),
                    ports.keySet());
            Assertions.assertEquals(
                    1, ports.get(DynamicLookupAction.FACT_INPUT).getChannels().size());
            Assertions.assertEquals(
                    1, ports.get(DynamicLookupAction.DIMENSION_INPUT).getChannels().size());
            ports.forEach(
                    (port, descriptor) ->
                            descriptor
                                    .getChannels()
                                    .forEach(
                                            channel -> {
                                                Assertions.assertEquals(
                                                        "orders-customer-lookup",
                                                        channel.getLogicalChannelKey()
                                                                .getOperatorUid());
                                                Assertions.assertEquals(
                                                        port == DynamicLookupAction.FACT_INPUT
                                                                ? "orders"
                                                                : "customer_dimension",
                                                        channel.getLogicalChannelKey()
                                                                .getSourceActionUid());
                                                Assertions.assertTrue(
                                                        channel.getLogicalChannelKey()
                                                                                .getDownstreamSubtask()
                                                                        >= 0
                                                                && channel.getLogicalChannelKey()
                                                                                .getDownstreamSubtask()
                                                                        < 2);
                                            }));
        }

        long coordinatorTaskCount =
                coordinatorVertices.stream()
                        .flatMap(vertex -> vertex.getTaskGroup().getTasks().stream())
                        .filter(DynamicLookupCoordinatorTask.class::isInstance)
                        .count();
        Assertions.assertEquals(1, coordinatorTaskCount);

        PhysicalVertex lookupPhysicalVertex = lookupPhysicalVertices.get(0);
        Assertions.assertEquals(2, lookupPhysicalVertex.getInputPorts().size());
        Assertions.assertEquals(
                2,
                lookupPhysicalVertex.getTaskGroup().getTasks().stream()
                        .filter(SourceSeaTunnelTask.class::isInstance)
                        .count());
        Assertions.assertTrue(
                lookupPhysicalVertex.getTaskGroup().getTasks().stream()
                        .anyMatch(TransformSeaTunnelTask.class::isInstance));

        ChannelAttemptId firstAttempt =
                lookupPhysicalVertex
                        .getInputPorts()
                        .get(0)
                        .getChannels()
                        .get(0)
                        .bindAttempts(7L, 11L, 21L, 31L);
        ChannelAttemptId secondAttempt =
                lookupPhysicalVertex
                        .getInputPorts()
                        .get(0)
                        .getChannels()
                        .get(0)
                        .bindAttempts(7L, 11L, 21L, 32L);
        Assertions.assertNotEquals(firstAttempt, secondAttempt);
        Assertions.assertEquals("2", firstAttempt.getChannelKey().getJobId());
        Assertions.assertEquals(
                "orders-customer-lookup", firstAttempt.getChannelKey().getOperatorUid());
        Assertions.assertTrue(
                Arrays.asList("orders", "customer_dimension")
                        .contains(firstAttempt.getChannelKey().getSourceActionUid()));

        CheckpointPlan checkpointPlan = plans.f1().get(1);
        Assertions.assertTrue(checkpointPlan.getPipelineSubtasks().size() >= 7);
        Assertions.assertEquals(2, checkpointPlan.getStartingSubtasks().size());
        Assertions.assertEquals(1, checkpointPlan.getCoordinatorCheckpointRoots().size());
        Assertions.assertEquals(1, checkpointPlan.getCoordinatorTasks().size());
        Assertions.assertEquals(2, checkpointPlan.getInputPortsByTask().size());
        Assertions.assertEquals(4, checkpointPlan.getPipelineActions().size());
        CoordinatorStateKey coordinatorStateKey =
                checkpointPlan.getCoordinatorTasks().keySet().stream()
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        Assertions.assertTrue(checkpointPlan.getPipelineActions().containsKey(coordinatorStateKey));
        Assertions.assertEquals(0, checkpointPlan.getPipelineActions().get(coordinatorStateKey));
        Assertions.assertTrue(
                checkpointPlan.getSubtaskActions()
                        .get(checkpointPlan.getCoordinatorTasks().get(coordinatorStateKey)).stream()
                        .anyMatch(state -> coordinatorStateKey.equals(state.f0())));
        DynamicLookupCoordinatorTask coordinatorTask =
                coordinatorVertices.stream()
                        .flatMap(vertex -> vertex.getTaskGroup().getTasks().stream())
                        .filter(DynamicLookupCoordinatorTask.class::isInstance)
                        .map(DynamicLookupCoordinatorTask.class::cast)
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        Assertions.assertEquals(coordinatorStateKey, coordinatorTask.getCoordinatorStateKey());
        CoordinatorStateKey restoredCoordinatorStateKey =
                nodeEngine
                        .getSerializationService()
                        .toObject(nodeEngine.getSerializationService().toData(coordinatorStateKey));
        Assertions.assertEquals(coordinatorStateKey, restoredCoordinatorStateKey);
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> restoredCoordinatorStateKey.setName("mutated"));
        ActionState coordinatorActionState = new ActionState(coordinatorStateKey, 0);
        ActionSubtaskState coordinatorSubtaskState =
                new ActionSubtaskState(
                        coordinatorStateKey,
                        CheckpointPlan.COORDINATOR_INDEX,
                        Collections.emptyList());
        coordinatorActionState.reportState(
                CheckpointPlan.COORDINATOR_INDEX, coordinatorSubtaskState);
        Assertions.assertTrue(coordinatorActionState.getSubtaskStates().isEmpty());
        Assertions.assertSame(
                coordinatorSubtaskState, coordinatorActionState.getCoordinatorState());

        Map<ActionStateKey, ActionState> taskStates = new HashMap<>();
        taskStates.put(coordinatorStateKey, coordinatorActionState);
        CompletedCheckpoint completedCheckpoint =
                new CompletedCheckpoint(
                        2L,
                        1,
                        41L,
                        101L,
                        CheckpointType.CHECKPOINT_TYPE,
                        202L,
                        taskStates,
                        Collections.emptyMap());
        ProtoStuffSerializer serializer = new ProtoStuffSerializer();
        CompletedCheckpoint restoredCheckpoint =
                serializer.deserialize(
                        serializer.serialize(completedCheckpoint), CompletedCheckpoint.class);
        CoordinatorStateKey expectedRestoredKey = new CoordinatorStateKey("orders-customer-lookup");
        ActionStateKey persistedMapKey =
                restoredCheckpoint.getTaskStates().keySet().iterator().next();
        Assertions.assertEquals(expectedRestoredKey.getName(), persistedMapKey.getName());
        Assertions.assertEquals(expectedRestoredKey, persistedMapKey);
        Assertions.assertEquals(persistedMapKey, expectedRestoredKey);
        ActionState restoredCoordinatorActionState =
                restoredCheckpoint.getTaskStates().get(expectedRestoredKey);
        Assertions.assertNotNull(restoredCoordinatorActionState);
        Assertions.assertNotNull(restoredCoordinatorActionState.getCoordinatorState());
        Assertions.assertEquals(
                CheckpointPlan.COORDINATOR_INDEX,
                restoredCoordinatorActionState.getCoordinatorState().getIndex());
        Assertions.assertEquals(
                expectedRestoredKey,
                restoredCoordinatorActionState.getCoordinatorState().getStateKey());

        // PhysicalPlan.initStateFuture() belongs to the JobMaster deployment lifecycle.
        // This unit test only verifies plan generation and checkpoint serialization.
    }

    private static void fillVirtualVertex(
            IdGenerator idGenerator, LogicalDag logicalDag, int parallelism) {
        JobContext jobContext = new JobContext();
        jobContext.setJobMode(JobMode.BATCH);
        Config fakeSourceConfig =
                ConfigFactory.parseMap(
                        Collections.singletonMap(
                                "schema",
                                Collections.singletonMap(
                                        "fields", ImmutableMap.of("id", "int", "name", "string"))));
        FakeSource fakeSource = new FakeSource(ReadonlyConfig.fromConfig(fakeSourceConfig));
        fakeSource.setJobContext(jobContext);

        Action fake =
                new SourceAction<>(
                        idGenerator.getNextId(),
                        "fake",
                        fakeSource,
                        Collections.emptySet(),
                        Collections.emptySet());
        fake.setParallelism(parallelism);
        LogicalVertex fakeVertex = new LogicalVertex(fake.getId(), fake, parallelism);

        List<Column> columns = new ArrayList<>();
        columns.add(PhysicalColumn.of("id", BasicType.INT_TYPE, 11L, 0, true, 111, ""));

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("default", TablePath.DEFAULT),
                        TableSchema.builder().columns(columns).build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "fake");

        ConsoleSink consoleSink =
                new ConsoleSink(catalogTable, ReadonlyConfig.fromMap(new HashMap<>()));
        consoleSink.setJobContext(jobContext);
        Action console =
                new SinkAction<>(
                        idGenerator.getNextId(),
                        "console",
                        consoleSink,
                        Collections.emptySet(),
                        Collections.emptySet());
        console.setParallelism(parallelism);
        LogicalVertex consoleVertex = new LogicalVertex(console.getId(), console, parallelism);

        LogicalEdge edge = new LogicalEdge(fakeVertex, consoleVertex);

        logicalDag.getEdges().add(edge);
        logicalDag.addLogicalVertex(fakeVertex);
        logicalDag.addLogicalVertex(consoleVertex);
    }

    private static FakeSource createFakeSource(JobContext jobContext) {
        Config fakeSourceConfig =
                ConfigFactory.parseMap(
                        Collections.singletonMap(
                                "schema",
                                Collections.singletonMap(
                                        "fields", ImmutableMap.of("id", "int", "name", "string"))));
        FakeSource fakeSource = new FakeSource(ReadonlyConfig.fromConfig(fakeSourceConfig));
        fakeSource.setJobContext(jobContext);
        return fakeSource;
    }

    private static DynamicLookupDescriptor lookupDescriptor(String outputId) {
        return new DynamicLookupDescriptor(
                outputId,
                new DynamicLookupSideSpec(
                        "orders",
                        "orders",
                        Collections.singletonList("id"),
                        Collections.singletonList(0)),
                new DynamicLookupSideSpec(
                        "customer_dimension",
                        "customer_dimension",
                        Collections.singletonList("id"),
                        Collections.singletonList(0)),
                DynamicLookupDescriptor.JoinType.LEFT,
                Collections.singletonList(
                        new DynamicLookupProjectionField(
                                DynamicLookupProjectionField.InputSide.FACT, "id", 0, "id")));
    }

    private static CatalogTable lookupCatalogTable(String outputId) {
        List<Column> columns = new ArrayList<>();
        columns.add(PhysicalColumn.of("id", BasicType.INT_TYPE, 11L, 0, true, null, ""));
        return CatalogTable.of(
                TableIdentifier.of("default", TablePath.of("default", outputId)),
                TableSchema.builder().columns(columns).build(),
                new HashMap<>(),
                Collections.emptyList(),
                "dynamic lookup output");
    }
}
