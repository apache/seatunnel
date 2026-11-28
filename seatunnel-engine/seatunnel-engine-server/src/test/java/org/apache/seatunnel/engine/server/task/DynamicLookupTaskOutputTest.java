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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupAction;
import org.apache.seatunnel.engine.core.dag.internal.IntermediateQueue;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.dag.physical.config.DynamicLookupConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.IntermediateQueueConfig;
import org.apache.seatunnel.engine.server.dag.physical.flow.IntermediateExecutionFlow;
import org.apache.seatunnel.engine.server.dag.physical.flow.PhysicalExecutionFlow;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.task.group.TaskGroupWithIntermediateBlockingQueue;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DynamicLookupTaskOutputTest {
    @Test
    @SuppressWarnings("unchecked")
    void alignedBarrierReachesSplitSinkQueue() throws Exception {
        IntermediateExecutionFlow<IntermediateQueueConfig> downstream =
                new IntermediateExecutionFlow<>(new IntermediateQueue(4L, "sink", 1));
        downstream.setConfig(new IntermediateQueueConfig(4L, 8));
        PhysicalExecutionFlow<DynamicLookupAction, DynamicLookupConfig> root =
                new PhysicalExecutionFlow<>(
                        mock(DynamicLookupAction.class), Collections.singletonList(downstream));
        Map<Integer, IntermediateQueueConfig> inputs = new HashMap<>();
        inputs.put(DynamicLookupAction.FACT_INPUT, new IntermediateQueueConfig(1L, 8));
        inputs.put(DynamicLookupAction.DIMENSION_INPUT, new IntermediateQueueConfig(2L, 8));
        root.setConfig(
                new DynamicLookupConfig(inputs, new IntermediateQueueConfig(3L, 1), 1024, 1024));

        TaskGroupLocation groupLocation = new TaskGroupLocation(1L, 1, 1L);
        TaskLocation location = new TaskLocation(groupLocation, 0, 0);
        TransformSeaTunnelTask task =
                new TransformSeaTunnelTask(1L, location, 0, root, Collections.emptyMap());
        TaskGroupWithIntermediateBlockingQueue group =
                new TaskGroupWithIntermediateBlockingQueue(
                        groupLocation, "lookup", Collections.singletonList(task));
        TaskExecutionContext context = mock(TaskExecutionContext.class);
        SeaTunnelMetricsContext metrics = new SeaTunnelMetricsContext();
        when(context.getOrCreateMetricsContext(location)).thenReturn(metrics);
        task.setTaskExecutionContext(context);
        group.init();
        task.init();
        task.startFlowLifeCycle.open();

        CheckpointBarrier barrier = mock(CheckpointBarrier.class);
        BlockingQueue<Record<?>> fact =
                (BlockingQueue<Record<?>>)
                        group.getQueueCache(1L, 8, metrics).getIntermediateQueue();
        BlockingQueue<Record<?>> dimension =
                (BlockingQueue<Record<?>>)
                        group.getQueueCache(2L, 8, metrics).getIntermediateQueue();
        BlockingQueue<?> sink =
                (BlockingQueue<?>) group.getQueueCache(4L, 8, metrics).getIntermediateQueue();
        dimension.add(new Record<>(barrier));
        task.collect();
        assertTrue(sink.isEmpty());
        fact.add(new Record<>(barrier));
        task.collect();
        Record<?> forwarded = (Record<?>) sink.poll();
        assertNotNull(forwarded, "Aligned barrier must reach the sink queue");
        assertSame(barrier, forwarded.getData());
        assertTrue(sink.isEmpty(), "An aligned barrier must only be forwarded once");
    }
}
