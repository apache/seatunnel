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

package org.apache.seatunnel.engine.server.dag.execution;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupAction;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupDescriptor;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupProjectionField;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupSideSpec;
import org.apache.seatunnel.engine.core.dag.actions.PortAwareAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDagGenerator;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ExecutionPlanGeneratorTest {

    @Test
    public void testComputeChainedTransformVertexIdUsesMinIdAndNotAdvanceGenerator() {
        IdGenerator idGenerator = new IdGenerator();

        SeaTunnelTransform<?> dummyTransform =
                new SeaTunnelTransform<Object>() {
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
                        return "dummy";
                    }
                };

        ExecutionVertex v10 =
                new ExecutionVertex(
                        10L,
                        new TransformAction(
                                10L,
                                "t1",
                                (SeaTunnelTransform<?>) dummyTransform,
                                Collections.emptySet(),
                                Collections.<ConnectorJarIdentifier>emptySet()),
                        1);
        ExecutionVertex v8 =
                new ExecutionVertex(
                        8L,
                        new TransformAction(
                                8L,
                                "t2",
                                (SeaTunnelTransform<?>) dummyTransform,
                                Collections.emptySet(),
                                Collections.<ConnectorJarIdentifier>emptySet()),
                        1);
        List<ExecutionVertex> vertices = Arrays.asList(v10, v8);

        long id = ExecutionPlanGenerator.computeChainedTransformVertexId(vertices, idGenerator);
        Assertions.assertEquals(8L, id);

        // Should not have advanced the generator when min is present.
        Assertions.assertEquals(1L, idGenerator.getNextId());
    }

    @Test
    public void testComputeChainedTransformVertexIdUsesGeneratorWhenEmpty() {
        IdGenerator idGenerator = new IdGenerator();
        long id =
                ExecutionPlanGenerator.computeChainedTransformVertexId(
                        Collections.emptyList(), idGenerator);
        Assertions.assertEquals(1L, id);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testPortAwareMultiInputTargetIsNotSplitOrDuplicated() {
        IdGenerator idGenerator = new IdGenerator();
        SourceAction fact =
                new SourceAction(
                        101L,
                        "fact",
                        Mockito.mock(SeaTunnelSource.class),
                        Collections.emptySet(),
                        Collections.emptySet());
        fact.setParallelism(3);
        SourceAction dimension =
                new SourceAction(
                        205L,
                        "dimension",
                        Mockito.mock(SeaTunnelSource.class),
                        Collections.emptySet(),
                        Collections.emptySet());
        dimension.setParallelism(3);
        DynamicLookupAction lookup =
                new DynamicLookupAction(
                        999L,
                        "lookup",
                        "orders-customer-lookup",
                        fact,
                        "orders",
                        dimension,
                        "customer_dimension",
                        lookupDescriptor("lookup-output"),
                        lookupCatalogTable("lookup-output"),
                        512L * 1024L * 1024L,
                        512L * 1024L * 1024L,
                        Collections.emptySet(),
                        Collections.emptySet());
        lookup.setParallelism(3);
        LogicalDag logicalDag =
                new LogicalDagGenerator(
                                Collections.singletonList(lookup), new JobConfig(), idGenerator)
                        .generate();

        ExecutionPlan executionPlan =
                new ExecutionPlanGenerator(
                                logicalDag, new JobImmutableInformation(), new EngineConfig())
                        .generate();

        Assertions.assertEquals(1, executionPlan.getPipelines().size());
        Pipeline pipeline = executionPlan.getPipelines().get(0);
        Assertions.assertEquals(3, pipeline.getVertexes().size());
        Assertions.assertEquals(2, pipeline.getEdges().size());
        Assertions.assertEquals(
                1,
                pipeline.getVertexes().values().stream()
                        .map(ExecutionVertex::getAction)
                        .filter(DynamicLookupAction.class::isInstance)
                        .count());
        List<PortAwareExecutionEdge> inputEdges =
                pipeline.getEdges().stream()
                        .filter(PortAwareExecutionEdge.class::isInstance)
                        .map(PortAwareExecutionEdge.class::cast)
                        .collect(Collectors.toList());
        Assertions.assertEquals(2, inputEdges.size());
        Set<Integer> expectedPorts =
                new HashSet<>(
                        Arrays.asList(
                                DynamicLookupAction.FACT_INPUT,
                                DynamicLookupAction.DIMENSION_INPUT));
        Assertions.assertEquals(
                expectedPorts,
                inputEdges.stream()
                        .map(PortAwareExecutionEdge::getTargetInputPort)
                        .collect(Collectors.toSet()));
        Assertions.assertSame(
                inputEdges.get(0).getRightVertex(), inputEdges.get(1).getRightVertex());
        Assertions.assertEquals(3, inputEdges.get(0).getRightVertex().getParallelism());
        DynamicLookupAction executionLookup =
                pipeline.getVertexes().values().stream()
                        .map(ExecutionVertex::getAction)
                        .filter(DynamicLookupAction.class::isInstance)
                        .map(DynamicLookupAction.class::cast)
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        Assertions.assertEquals(101L, executionLookup.getFactSourceActionId());
        Assertions.assertEquals(205L, executionLookup.getDimensionSourceActionId());
        Assertions.assertEquals("orders", executionLookup.getFactSourceActionUid());
        Assertions.assertEquals(
                "customer_dimension", executionLookup.getDimensionSourceActionUid());
        Assertions.assertEquals(
                512L * 1024L * 1024L, executionLookup.getMaxLogicalStateBytesPerSubtask());
        Assertions.assertEquals(
                512L * 1024L * 1024L, executionLookup.getMaxResidentStateBytesPerSubtask());
        inputEdges.forEach(
                edge ->
                        Assertions.assertEquals(
                                edge.getEdgeId(),
                                executionLookup.getInputPortBindings().stream()
                                        .filter(
                                                binding ->
                                                        binding.getTargetInputPort()
                                                                == edge.getTargetInputPort())
                                        .findFirst()
                                        .orElseThrow(AssertionError::new)
                                        .getEdgeId()));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testDynamicLookupSourceCannotFeedNormalBranch() {
        SourceAction fact = sourceAction(11L, "fact");
        SourceAction dimension = sourceAction(12L, "dimension");
        DynamicLookupAction lookup =
                dynamicLookup(13L, "lookup-uid", fact, "orders", dimension, "customers");
        Action sink =
                new SinkAction(
                        14L,
                        "ordinary-sink",
                        Collections.singletonList(fact),
                        Mockito.mock(SeaTunnelSink.class),
                        Collections.emptySet(),
                        Collections.emptySet());

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new LogicalDagGenerator(
                                                Arrays.asList(lookup, sink),
                                                new JobConfig(),
                                                new IdGenerator())
                                        .generate());

        Assertions.assertTrue(
                error.getMessage().contains("DYNAMIC_LOOKUP_SOURCE_SHARED_WITH_NORMAL_BRANCH"));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testDynamicLookupDimensionSourceMustBeDedicated() {
        SourceAction fact = sourceAction(15L, "fact");
        SourceAction dimension = sourceAction(16L, "dimension");
        DynamicLookupAction lookup =
                dynamicLookup(17L, "lookup-uid", fact, "orders", dimension, "customers");
        Action sink =
                new SinkAction(
                        18L,
                        "ordinary-sink",
                        Collections.singletonList(dimension),
                        Mockito.mock(SeaTunnelSink.class),
                        Collections.emptySet(),
                        Collections.emptySet());

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new LogicalDagGenerator(
                                                Arrays.asList(lookup, sink),
                                                new JobConfig(),
                                                new IdGenerator())
                                        .generate());

        Assertions.assertTrue(
                error.getMessage().contains("DYNAMIC_LOOKUP_SOURCE_SHARED_WITH_NORMAL_BRANCH"));
    }

    @Test
    public void testDynamicLookupSourceCannotHaveTwoLookupOwners() {
        SourceAction<?, ?, ?> fact = sourceAction(21L, "fact");
        SourceAction<?, ?, ?> firstDimension = sourceAction(22L, "dimension-1");
        SourceAction<?, ?, ?> secondDimension = sourceAction(23L, "dimension-2");
        DynamicLookupAction first =
                dynamicLookup(
                        24L, "lookup-1", fact, "orders", firstDimension, "customer_dimension");
        DynamicLookupAction second =
                dynamicLookup(25L, "lookup-2", fact, "orders", secondDimension, "region_dimension");

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new LogicalDagGenerator(
                                                Arrays.asList(first, second),
                                                new JobConfig(),
                                                new IdGenerator())
                                        .generate());

        Assertions.assertTrue(error.getMessage().contains("DYNAMIC_LOOKUP_SOURCE_MULTIPLE_OWNERS"));
    }

    @Test
    public void testDynamicLookupOperatorUidMustBeUnique() {
        DynamicLookupAction first =
                dynamicLookup(
                        31L,
                        "duplicate-uid",
                        sourceAction(32L, "fact-1"),
                        "orders_1",
                        sourceAction(33L, "dimension-1"),
                        "customers_1");
        DynamicLookupAction second =
                dynamicLookup(
                        34L,
                        "duplicate-uid",
                        sourceAction(35L, "fact-2"),
                        "orders_2",
                        sourceAction(36L, "dimension-2"),
                        "customers_2");

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new LogicalDagGenerator(
                                                Arrays.asList(first, second),
                                                new JobConfig(),
                                                new IdGenerator())
                                        .generate());

        Assertions.assertTrue(error.getMessage().contains("DYNAMIC_LOOKUP_OPERATOR_UID_COLLISION"));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testDynamicLookupOutputCanFeedDownstreamSink() {
        DynamicLookupAction lookup =
                dynamicLookup(
                        41L,
                        "lookup-with-output",
                        sourceAction(42L, "fact"),
                        "orders",
                        sourceAction(43L, "dimension"),
                        "customers");
        Action sink =
                new SinkAction(
                        44L,
                        "sink",
                        Collections.singletonList(lookup),
                        Mockito.mock(SeaTunnelSink.class),
                        Collections.emptySet(),
                        Collections.emptySet());

        LogicalDag logicalDag =
                new LogicalDagGenerator(
                                Collections.singletonList(sink), new JobConfig(), new IdGenerator())
                        .generate();
        ExecutionPlan executionPlan =
                new ExecutionPlanGenerator(
                                logicalDag, new JobImmutableInformation(), new EngineConfig())
                        .generate();

        Assertions.assertTrue(
                executionPlan.getPipelines().stream()
                        .flatMap(pipeline -> pipeline.getEdges().stream())
                        .anyMatch(
                                edge ->
                                        edge.getLeftVertex().getAction()
                                                        instanceof DynamicLookupAction
                                                && edge.getRightVertex().getAction()
                                                        instanceof SinkAction));
    }

    @Test
    public void testDynamicLookupRejectsOtherPortAwareActionTypes() {
        PortAwareAction unsupportedAction = Mockito.mock(PortAwareAction.class);
        Mockito.when(unsupportedAction.getId()).thenReturn(51L);
        Mockito.when(unsupportedAction.getName()).thenReturn("unsupported-port-aware-action");

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new LogicalDagGenerator(
                                                Collections.singletonList(unsupportedAction),
                                                new JobConfig(),
                                                new IdGenerator())
                                        .generate());

        Assertions.assertTrue(error.getMessage().contains("only DynamicLookupAction"));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testLegacyMultiSourceSinkKeepsExistingPipelineSplit() {
        IdGenerator idGenerator = new IdGenerator();
        SourceAction first =
                new SourceAction(
                        idGenerator.getNextId(),
                        "first",
                        Mockito.mock(SeaTunnelSource.class),
                        Collections.emptySet(),
                        Collections.emptySet());
        SourceAction second =
                new SourceAction(
                        idGenerator.getNextId(),
                        "second",
                        Mockito.mock(SeaTunnelSource.class),
                        Collections.emptySet(),
                        Collections.emptySet());
        Action sink =
                new SinkAction(
                        idGenerator.getNextId(),
                        "sink",
                        Arrays.asList(first, second),
                        Mockito.mock(SeaTunnelSink.class),
                        Collections.emptySet(),
                        Collections.emptySet());
        LogicalDag logicalDag =
                new LogicalDagGenerator(
                                Collections.singletonList(sink), new JobConfig(), idGenerator)
                        .generate();

        ExecutionPlan executionPlan =
                new ExecutionPlanGenerator(
                                logicalDag, new JobImmutableInformation(), new EngineConfig())
                        .generate();

        Assertions.assertEquals(2, executionPlan.getPipelines().size());
        Assertions.assertTrue(
                executionPlan.getPipelines().stream()
                        .allMatch(pipeline -> pipeline.getEdges().size() == 1));
        Assertions.assertTrue(
                executionPlan.getPipelines().stream()
                        .flatMap(pipeline -> pipeline.getEdges().stream())
                        .noneMatch(PortAwareExecutionEdge.class::isInstance));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static SourceAction sourceAction(long id, String name) {
        return new SourceAction(
                id,
                name,
                Mockito.mock(SeaTunnelSource.class),
                Collections.emptySet(),
                Collections.emptySet());
    }

    private static DynamicLookupAction dynamicLookup(
            long id,
            String operatorUid,
            Action fact,
            String factSourceUid,
            Action dimension,
            String dimensionSourceUid) {
        return new DynamicLookupAction(
                id,
                "lookup-" + id,
                operatorUid,
                fact,
                factSourceUid,
                dimension,
                dimensionSourceUid,
                lookupDescriptor("lookup-" + id),
                lookupCatalogTable("lookup-" + id),
                512L * 1024L * 1024L,
                512L * 1024L * 1024L,
                Collections.emptySet(),
                Collections.emptySet());
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
