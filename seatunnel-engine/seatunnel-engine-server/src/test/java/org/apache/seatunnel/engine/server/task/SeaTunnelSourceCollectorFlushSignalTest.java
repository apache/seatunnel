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

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.common.metrics.ThreadSafeCounter;
import org.apache.seatunnel.api.common.metrics.ThreadSafeQPSMeter;
import org.apache.seatunnel.api.signal.FlushSignal;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.flow.OneInputFlowLifeCycle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class SeaTunnelSourceCollectorFlushSignalTest {

    @Test
    void sendFlushSignalBroadcastsExactlyOneRecordPerOutput() throws Exception {
        RecordingOutput a = new RecordingOutput();
        RecordingOutput b = new RecordingOutput();
        SeaTunnelSourceCollector<Object> collector = newCollector(a, b);

        collector.sendFlushSignal(7L, 42L);

        Assertions.assertEquals(1, a.records.size());
        Assertions.assertEquals(1, b.records.size());
        Assertions.assertInstanceOf(FlushSignal.class, a.records.get(0).getData());
        FlushSignal signal = (FlushSignal) a.records.get(0).getData();
        Assertions.assertEquals(7L, signal.getJobId());
        Assertions.assertEquals(42L, signal.getTaskId());
        Assertions.assertTrue(
                signal.getCreatedTime() > 0L, "createdTime should be populated at construction");
    }

    @Test
    void sendFlushSignalIsNoopWhenOutputsEmpty() throws Exception {
        SeaTunnelSourceCollector<Object> collector = newCollector();
        Assertions.assertDoesNotThrow(() -> collector.sendFlushSignal(1L, 1L));
    }

    private static SeaTunnelSourceCollector<Object> newCollector(RecordingOutput... outputs) {
        List<OneInputFlowLifeCycle<Record<?>>> outputList = new ArrayList<>();
        Collections.addAll(outputList, outputs);
        // new SeaTunnelSourceCollector<>(new Object(),outputList,null,null,null,null,null,null)
        SeaTunnelSourceCollectorFlushSignalTest.TestMetricsContext metricsContext =
                new SeaTunnelSourceCollectorFlushSignalTest.TestMetricsContext();

        EngineConfig engineConfig = new EngineConfig();
        engineConfig.setStainTraceEnabled(false);

        SeaTunnelTask sourceTask = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(sourceTask.getTaskLocation())
                .thenReturn(new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 0));

        return new SeaTunnelSourceCollector<>(
                new Object(),
                outputList,
                metricsContext,
                FlowControlStrategy.builder().build(),
                new SeaTunnelRowType(new String[0], new SeaTunnelDataType<?>[0]),
                Collections.singletonList(TablePath.DEFAULT),
                sourceTask,
                engineConfig);
    }

    private static class RecordingOutput implements OneInputFlowLifeCycle<Record<?>> {
        private final List<Record<?>> records = new ArrayList<>();

        @Override
        public void received(Record<?> record) {
            records.add(record);
        }
    }

    private static class TestMetricsContext implements MetricsContext {
        @Override
        public Counter counter(String name) {
            return new ThreadSafeCounter(name);
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            return counter;
        }

        @Override
        public Meter meter(String name) {
            return new ThreadSafeQPSMeter(name);
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            return meter;
        }
    }
}
