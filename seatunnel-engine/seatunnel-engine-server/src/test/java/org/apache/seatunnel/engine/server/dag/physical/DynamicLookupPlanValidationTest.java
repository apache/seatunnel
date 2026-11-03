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

package org.apache.seatunnel.engine.server.dag.physical;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupAction;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupDescriptor;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupProjectionField;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupSideSpec;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.ExchangeDescriptor;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDagGenerator;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionPlan;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionPlanGenerator;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex;
import org.apache.seatunnel.engine.server.dag.execution.PortAwareExecutionEdge;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/** Prevents programmatically built FORWARD lookup plans from bypassing key co-location checks. */
class DynamicLookupPlanValidationTest {

    @Test
    void rejectsParallelSourcesAndLookupEvenWhenTheirParallelismMatches() {
        for (int[] parallelism : new int[][] {{2, 2, 2}, {2, 1, 1}, {1, 2, 1}, {1, 1, 2}}) {
            DynamicLookupAction lookup = Mockito.mock(DynamicLookupAction.class);
            Mockito.when(lookup.getParallelism()).thenReturn(parallelism[2]);
            ExecutionVertex target = new ExecutionVertex(3L, lookup, parallelism[2]);
            ExecutionVertex fact =
                    new ExecutionVertex(1L, Mockito.mock(SourceAction.class), parallelism[0]);
            ExecutionVertex dimension =
                    new ExecutionVertex(2L, Mockito.mock(SourceAction.class), parallelism[1]);
            IllegalArgumentException error =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    PhysicalPlanGenerator.validateDynamicLookupInputEdges(
                                            lookup,
                                            Arrays.asList(
                                                    new PortAwareExecutionEdge(
                                                            fact,
                                                            target,
                                                            10L,
                                                            0,
                                                            ExchangeDescriptor.forward()),
                                                    new PortAwareExecutionEdge(
                                                            dimension,
                                                            target,
                                                            11L,
                                                            1,
                                                            ExchangeDescriptor.forward()))));
            Assertions.assertTrue(
                    error.getMessage().contains("parallelism = 1"), error.getMessage());
        }
    }

    /**
     * Regression for the dimension role being bound to the lookup's logical source ID.
     *
     * <p>With logical fact=1, dimension=2, lookup=3 the execution plan allocates fact=1, lookup=2,
     * dimension=3. The physical plan must mark the source on the dimension port as the dimension
     * (and the one on the fact port as the gated fact) by its execution ID; resolving it from
     * {@link DynamicLookupAction#getDimensionSourceActionId()} yields 2, matches no source, and the
     * dimension never emits its bootstrap COMPLETE marker.
     */
    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void resolvesLookupSourceRolesFromRegeneratedExecutionIds() {
        SourceAction fact =
                new SourceAction(
                        1L,
                        "fact",
                        Mockito.mock(SeaTunnelSource.class),
                        Collections.emptySet(),
                        Collections.emptySet());
        SourceAction dimension =
                new SourceAction(
                        2L,
                        "dimension",
                        Mockito.mock(SeaTunnelSource.class),
                        Collections.emptySet(),
                        Collections.emptySet());
        DynamicLookupAction lookup =
                new DynamicLookupAction(
                        3L,
                        "lookup",
                        "orders-customer-lookup",
                        fact,
                        "orders",
                        dimension,
                        "customer_dimension",
                        lookupDescriptor(),
                        lookupCatalogTable(),
                        512L * 1024L * 1024L,
                        512L * 1024L * 1024L,
                        Collections.emptySet(),
                        Collections.emptySet());
        LogicalDag logicalDag =
                new LogicalDagGenerator(
                                Collections.singletonList(lookup),
                                new JobConfig(),
                                new IdGenerator())
                        .generate();
        ExecutionPlan executionPlan =
                new ExecutionPlanGenerator(
                                logicalDag, new JobImmutableInformation(), new EngineConfig())
                        .generate();
        List<PortAwareExecutionEdge> inputEdges =
                executionPlan.getPipelines().get(0).getEdges().stream()
                        .filter(PortAwareExecutionEdge.class::isInstance)
                        .map(PortAwareExecutionEdge.class::cast)
                        .collect(Collectors.toList());
        PortAwareExecutionEdge dimensionEdge =
                inputEdges.stream()
                        .filter(
                                edge ->
                                        edge.getTargetInputPort()
                                                == DynamicLookupAction.DIMENSION_INPUT)
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        DynamicLookupAction executionLookup =
                (DynamicLookupAction) dimensionEdge.getRightVertex().getAction();

        Assertions.assertEquals("dimension", dimensionEdge.getLeftVertex().getAction().getName());
        Assertions.assertEquals(3L, dimensionEdge.getLeftVertex().getAction().getId());
        Assertions.assertEquals(2L, executionLookup.getDimensionSourceActionId());
        Assertions.assertEquals(
                3L,
                PhysicalPlanGenerator.resolveDynamicLookupSourceActionId(
                        inputEdges, DynamicLookupAction.DIMENSION_INPUT));
        Assertions.assertEquals(
                1L,
                PhysicalPlanGenerator.resolveDynamicLookupSourceActionId(
                        inputEdges, DynamicLookupAction.FACT_INPUT));
    }

    private static DynamicLookupDescriptor lookupDescriptor() {
        return new DynamicLookupDescriptor(
                "lookup-output",
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

    private static CatalogTable lookupCatalogTable() {
        return CatalogTable.of(
                TableIdentifier.of("default", TablePath.of("default", "lookup-output")),
                TableSchema.builder()
                        .columns(
                                Collections.singletonList(
                                        PhysicalColumn.of(
                                                "id", BasicType.INT_TYPE, 11L, 0, true, null, "")))
                        .build(),
                new HashMap<>(),
                Collections.emptyList(),
                "dynamic lookup output");
    }
}
