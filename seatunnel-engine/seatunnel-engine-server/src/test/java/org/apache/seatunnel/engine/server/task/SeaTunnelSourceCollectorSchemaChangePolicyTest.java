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

import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.common.metrics.ThreadSafeCounter;
import org.apache.seatunnel.api.common.metrics.ThreadSafeQPSMeter;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.SchemaChangeBehavior;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;
import org.apache.seatunnel.api.table.type.BasicType;
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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class SeaTunnelSourceCollectorSchemaChangePolicyTest {

    @Test
    void testStrictBehaviorFailsBeforeForwardingSchemaChange() {
        RecordingFlowLifeCycle output = new RecordingFlowLifeCycle();
        SeaTunnelSourceCollector<Object> collector =
                createCollector(SchemaChangeBehavior.STRICT, output);

        Assertions.assertThrows(
                SchemaEvolutionException.class, () -> collector.collect(createCommentEvent()));
        Assertions.assertEquals(0, output.receivedCount.get());
    }

    @Test
    void testIgnoreBehaviorDropsSafeCommentEventBeforeForwarding() {
        RecordingFlowLifeCycle output = new RecordingFlowLifeCycle();
        SeaTunnelSourceCollector<Object> collector =
                createCollector(SchemaChangeBehavior.IGNORE, output);

        collector.collect(createCommentEvent());

        Assertions.assertEquals(0, output.receivedCount.get());
    }

    @Test
    void testIgnoreBehaviorFailsForUnsafeSchemaChange() {
        RecordingFlowLifeCycle output = new RecordingFlowLifeCycle();
        SeaTunnelSourceCollector<Object> collector =
                createCollector(SchemaChangeBehavior.IGNORE, output);

        Assertions.assertThrows(
                SchemaEvolutionException.class,
                () ->
                        collector.collect(
                                AlterTableAddColumnEvent.add(
                                        TableIdentifier.of("catalog", "database", "table"),
                                        org.apache.seatunnel.api.table.catalog.PhysicalColumn.of(
                                                "new_col",
                                                BasicType.STRING_TYPE,
                                                64L,
                                                true,
                                                null,
                                                null))));
        Assertions.assertEquals(0, output.receivedCount.get());
    }

    private static AlterTableCommentEvent createCommentEvent() {
        return AlterTableCommentEvent.of(
                TableIdentifier.of("catalog", "database", "table"), "old comment", "new comment");
    }

    private static SeaTunnelSourceCollector<Object> createCollector(
            SchemaChangeBehavior behavior, OneInputFlowLifeCycle<Record<?>> output) {
        SeaTunnelTask sourceTask = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(sourceTask.getTaskLocation())
                .thenReturn(new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 0));
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"f1"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        return new SeaTunnelSourceCollector<>(
                new Object(),
                Collections.singletonList(output),
                new TestMetricsContext(),
                FlowControlStrategy.builder().build(),
                rowType,
                Collections.singletonList(TablePath.of("database", "table")),
                sourceTask,
                new EngineConfig(),
                null,
                behavior,
                System::currentTimeMillis);
    }

    private static class RecordingFlowLifeCycle implements OneInputFlowLifeCycle<Record<?>> {
        private final AtomicInteger receivedCount = new AtomicInteger();

        @Override
        public void received(Record<?> record) {
            receivedCount.incrementAndGet();
        }
    }

    private static class TestMetricsContext implements MetricsContext {
        @Override
        public ThreadSafeCounter counter(String name) {
            return new ThreadSafeCounter(name);
        }

        @Override
        public <C extends org.apache.seatunnel.api.common.metrics.Counter> C counter(
                String name, C counter) {
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
