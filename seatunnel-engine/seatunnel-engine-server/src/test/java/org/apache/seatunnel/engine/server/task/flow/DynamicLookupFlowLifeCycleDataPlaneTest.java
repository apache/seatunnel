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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupAction;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupDescriptor;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupProjectionField;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupSideSpec;
import org.apache.seatunnel.engine.server.dag.physical.config.DynamicLookupConfig;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.TaskRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

/** Covers dynamic lookup dimension mutation, projection, and state-budget admission. */
public class DynamicLookupFlowLifeCycleDataPlaneTest {

    @Test
    void projectFactShouldUseLatestDimensionRow() throws Exception {
        DynamicLookupFlowLifeCycle flow =
                newFlow(DynamicLookupDescriptor.JoinType.LEFT, Long.MAX_VALUE, Long.MAX_VALUE);
        applyDimension(flow, dimensionRow(1, "Alice"));

        SeaTunnelRow output = projectFact(flow, factRow(1, "order-1"));

        Assertions.assertEquals("order-1", output.getField(0));
        Assertions.assertEquals("Alice", output.getField(1));
    }

    @Test
    void innerJoinMissShouldDropFactRow() throws Exception {
        DynamicLookupFlowLifeCycle flow =
                newFlow(DynamicLookupDescriptor.JoinType.INNER, Long.MAX_VALUE, Long.MAX_VALUE);

        Assertions.assertNull(projectFact(flow, factRow(404, "missing")));
    }

    @Test
    void dimensionMutationShouldFailFastWhenResidentBudgetIsExceeded() throws Exception {
        DynamicLookupFlowLifeCycle flow =
                newFlow(DynamicLookupDescriptor.JoinType.LEFT, Long.MAX_VALUE, 1L);

        InvocationTargetException exception =
                Assertions.assertThrows(
                        InvocationTargetException.class,
                        () -> applyDimension(flow, dimensionRow(1, "oversized")));

        Assertions.assertInstanceOf(TaskRuntimeException.class, exception.getCause());
        Assertions.assertTrue(exception.getCause().getMessage().contains("resident budget"));
    }

    private static DynamicLookupFlowLifeCycle newFlow(
            DynamicLookupDescriptor.JoinType joinType,
            long maxLogicalStateBytes,
            long maxResidentStateBytes) {
        DynamicLookupAction action = Mockito.mock(DynamicLookupAction.class);
        Mockito.when(action.getDescriptor()).thenReturn(descriptor(joinType));
        SeaTunnelTask runningTask = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(runningTask.getTaskLocation()).thenReturn(Mockito.mock(TaskLocation.class));
        DynamicLookupConfig config = Mockito.mock(DynamicLookupConfig.class);
        Mockito.when(config.getMaxLogicalStateBytesPerSubtask()).thenReturn(maxLogicalStateBytes);
        Mockito.when(config.getMaxResidentStateBytesPerSubtask()).thenReturn(maxResidentStateBytes);
        return new DynamicLookupFlowLifeCycle(
                action, runningTask, config, new CompletableFuture<>());
    }

    private static DynamicLookupDescriptor descriptor(DynamicLookupDescriptor.JoinType joinType) {
        return new DynamicLookupDescriptor(
                "lookup_out",
                new DynamicLookupSideSpec(
                        "fact", "catalog.db.fact", Arrays.asList("id"), Arrays.asList(0)),
                new DynamicLookupSideSpec(
                        "dimension", "catalog.db.dimension", Arrays.asList("id"), Arrays.asList(0)),
                joinType,
                Arrays.asList(
                        new DynamicLookupProjectionField(
                                DynamicLookupProjectionField.InputSide.FACT, "order", 1, "order"),
                        new DynamicLookupProjectionField(
                                DynamicLookupProjectionField.InputSide.DIMENSION,
                                "name",
                                1,
                                "name")));
    }

    private static SeaTunnelRow factRow(int key, String order) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {key, order});
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    private static SeaTunnelRow dimensionRow(int key, String name) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {key, name});
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    private static void applyDimension(DynamicLookupFlowLifeCycle flow, SeaTunnelRow row)
            throws Exception {
        Method method =
                DynamicLookupFlowLifeCycle.class.getDeclaredMethod(
                        "applyDimension", SeaTunnelRow.class);
        method.setAccessible(true);
        method.invoke(flow, row);
    }

    private static SeaTunnelRow projectFact(DynamicLookupFlowLifeCycle flow, SeaTunnelRow row)
            throws Exception {
        Method method =
                DynamicLookupFlowLifeCycle.class.getDeclaredMethod(
                        "projectFact", SeaTunnelRow.class);
        method.setAccessible(true);
        return (SeaTunnelRow) method.invoke(flow, row);
    }
}
