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

import org.apache.seatunnel.api.common.metrics.ThreadSafeCounter;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.trace.StainTraceConstants;
import org.apache.seatunnel.engine.server.trace.StainTracePayload;
import org.apache.seatunnel.engine.server.trace.StainTraceStage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Verifies transform flow propagation rules for stain trace payload fan-out. */
public class TransformFlowLifeCycleStainTraceTest {

    @Test
    void testFlatMapOnlyFirstOutputInheritsTracePayload() throws Exception {
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1});
        byte[] payload = StainTracePayload.init(11L, 22L);
        payload =
                StainTracePayload.append(payload, StainTraceStage.SOURCE_EMIT, 1L, 2L, 32)
                        .getPayload();
        inputRow.getOptions().put(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY, payload);

        SeaTunnelRow out1 = new SeaTunnelRow(new Object[] {1});
        SeaTunnelRow out2 = new SeaTunnelRow(new Object[] {2});

        SeaTunnelFlatMapTransform<SeaTunnelRow> flatMapTransform =
                Mockito.mock(SeaTunnelFlatMapTransform.class);
        Mockito.when(flatMapTransform.flatMap(Mockito.any())).thenReturn(Arrays.asList(out1, out2));

        TransformChainAction<SeaTunnelRow> action =
                new TransformChainAction<>(
                        1L,
                        "t",
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.singletonList(flatMapTransform));

        List<Record<?>> outputs = new ArrayList<>();
        Collector<Record<?>> collector =
                new Collector<Record<?>>() {
                    @Override
                    public void collect(Record<?> record) {
                        outputs.add(record);
                    }

                    @Override
                    public void close() {}
                };

        SeaTunnelTask runningTask = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(runningTask.getTaskID()).thenReturn(100L);

        TransformFlowLifeCycle<SeaTunnelRow> flow =
                new TransformFlowLifeCycle<>(
                        action, runningTask, collector, new CompletableFuture<>());
        setField(flow, "stainTraceMaxEntriesPerTrace", 32);
        setField(flow, "stainTraceEntriesTruncatedTotal", new ThreadSafeCounter("c"));
        setField(flow, "stainTracePropagateToAllSplits", Boolean.FALSE);

        flow.received(new Record<>(inputRow));

        Assertions.assertEquals(2, outputs.size());
        SeaTunnelRow r1 = (SeaTunnelRow) outputs.get(0).getData();
        SeaTunnelRow r2 = (SeaTunnelRow) outputs.get(1).getData();

        byte[] p1 =
                (byte[]) r1.getOptionsOrNull().get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY);
        Assertions.assertNotNull(p1);
        Assertions.assertTrue(StainTracePayload.isValid(p1));
        Assertions.assertEquals(11L, StainTracePayload.readTraceId(p1));

        Assertions.assertNull(r2.getOptionsOrNull());
    }

    @Test
    void testFlatMapAllOutputsInheritTracePayloadWhenEnabled() throws Exception {
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1});
        byte[] payload = StainTracePayload.init(11L, 22L);
        payload =
                StainTracePayload.append(payload, StainTraceStage.SOURCE_EMIT, 1L, 2L, 32)
                        .getPayload();
        inputRow.getOptions().put(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY, payload);

        SeaTunnelRow out1 = new SeaTunnelRow(new Object[] {1});
        SeaTunnelRow out2 = new SeaTunnelRow(new Object[] {2});

        SeaTunnelFlatMapTransform<SeaTunnelRow> flatMapTransform =
                Mockito.mock(SeaTunnelFlatMapTransform.class);
        Mockito.when(flatMapTransform.flatMap(Mockito.any())).thenReturn(Arrays.asList(out1, out2));

        TransformChainAction<SeaTunnelRow> action =
                new TransformChainAction<>(
                        1L,
                        "t",
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.singletonList(flatMapTransform));

        List<Record<?>> outputs = new ArrayList<>();
        Collector<Record<?>> collector =
                new Collector<Record<?>>() {
                    @Override
                    public void collect(Record<?> record) {
                        outputs.add(record);
                    }

                    @Override
                    public void close() {}
                };

        SeaTunnelTask runningTask = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(runningTask.getTaskID()).thenReturn(100L);

        TransformFlowLifeCycle<SeaTunnelRow> flow =
                new TransformFlowLifeCycle<>(
                        action, runningTask, collector, new CompletableFuture<>());
        setField(flow, "stainTraceMaxEntriesPerTrace", 32);
        setField(flow, "stainTraceEntriesTruncatedTotal", new ThreadSafeCounter("c"));
        setField(flow, "stainTracePropagateToAllSplits", Boolean.TRUE);

        flow.received(new Record<>(inputRow));

        Assertions.assertEquals(2, outputs.size());
        SeaTunnelRow r1 = (SeaTunnelRow) outputs.get(0).getData();
        SeaTunnelRow r2 = (SeaTunnelRow) outputs.get(1).getData();

        byte[] p1 =
                (byte[]) r1.getOptionsOrNull().get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY);
        byte[] p2 =
                (byte[]) r2.getOptionsOrNull().get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY);

        Assertions.assertNotNull(p1);
        Assertions.assertNotNull(p2);
        Assertions.assertNotSame(p1, p2);
        Assertions.assertTrue(StainTracePayload.isValid(p1));
        Assertions.assertTrue(StainTracePayload.isValid(p2));
        Assertions.assertEquals(11L, StainTracePayload.readTraceId(p1));
        Assertions.assertEquals(11L, StainTracePayload.readTraceId(p2));
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
