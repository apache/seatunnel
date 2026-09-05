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

import org.apache.seatunnel.api.source.SourceGateCommand;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupAction;
import org.apache.seatunnel.engine.server.dag.physical.config.DynamicLookupConfig;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.TaskRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/** Covers checkpoint control-plane behavior of the dynamic lookup runtime. */
public class DynamicLookupFlowLifeCycleControlPlaneTest {

    @Test
    void notifyCheckpointAbortedShouldReleaseAlignedState() throws Exception {
        DynamicLookupFlowLifeCycle flow = newFlow();

        @SuppressWarnings("rawtypes")
        Map barrierAlignments = getField(flow, "barrierAlignments");
        @SuppressWarnings("rawtypes")
        Map blockedPorts = getField(flow, "blockedPorts");
        barrierAlignments.put(7L, "aligned");
        blockedPorts.put(DynamicLookupAction.FACT_INPUT, 7L);
        blockedPorts.put(DynamicLookupAction.DIMENSION_INPUT, 7L);
        setField(flow, "pendingFactGateOpenCheckpointId", 7L);

        flow.notifyCheckpointAborted(7L);

        Assertions.assertTrue(barrierAlignments.isEmpty());
        Assertions.assertTrue(blockedPorts.isEmpty());
        Assertions.assertEquals(-1L, (long) getField(flow, "pendingFactGateOpenCheckpointId"));
    }

    @Test
    void notifyCheckpointCompleteShouldOpenFactGateOnce() throws Exception {
        DynamicLookupFlowLifeCycle flow = newFlow();
        @SuppressWarnings("unchecked")
        BlockingQueue<SourceGateCommand> queue = Mockito.mock(BlockingQueue.class);
        Mockito.when(queue.offer(SourceGateCommand.OPEN, 10, java.util.concurrent.TimeUnit.SECONDS))
                .thenReturn(true);
        setField(flow, "factGateCommandQueue", queue);
        setField(flow, "pendingFactGateOpenCheckpointId", 9L);

        flow.notifyCheckpointComplete(9L);

        Mockito.verify(queue)
                .offer(SourceGateCommand.OPEN, 10, java.util.concurrent.TimeUnit.SECONDS);
        Assertions.assertTrue((boolean) getField(flow, "factGateOpened"));
        Assertions.assertFalse((boolean) getField(flow, "factGateOpening"));
        Assertions.assertEquals(-1L, (long) getField(flow, "pendingFactGateOpenCheckpointId"));
    }

    @Test
    void notifyCheckpointCompleteShouldClearPendingWhenOpenOfferFails() throws Exception {
        DynamicLookupFlowLifeCycle flow = newFlow();
        @SuppressWarnings("unchecked")
        BlockingQueue<SourceGateCommand> queue = Mockito.mock(BlockingQueue.class);
        Mockito.when(queue.offer(SourceGateCommand.OPEN, 10, java.util.concurrent.TimeUnit.SECONDS))
                .thenReturn(false);
        setField(flow, "factGateCommandQueue", queue);
        setField(flow, "pendingFactGateOpenCheckpointId", 11L);

        Assertions.assertThrows(
                TaskRuntimeException.class, () -> flow.notifyCheckpointComplete(11L));

        Assertions.assertFalse((boolean) getField(flow, "factGateOpened"));
        Assertions.assertFalse((boolean) getField(flow, "factGateOpening"));
        Assertions.assertEquals(-1L, (long) getField(flow, "pendingFactGateOpenCheckpointId"));
    }

    private static DynamicLookupFlowLifeCycle newFlow() {
        SeaTunnelTask runningTask = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(runningTask.getTaskLocation()).thenReturn(Mockito.mock(TaskLocation.class));
        return new DynamicLookupFlowLifeCycle(
                Mockito.mock(DynamicLookupAction.class),
                runningTask,
                Mockito.mock(DynamicLookupConfig.class),
                new CompletableFuture<>());
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
