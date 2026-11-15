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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.flow.OneInputFlowLifeCycle;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_COUNT;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeaTunnelSourceCollectorRowAccountingTest {

    @Test
    void testNullableMapArrayCollection() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"items"},
                        new SeaTunnelDataType<?>[] {
                            new ArrayType<>(
                                    Map[].class,
                                    new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE))
                        });
        assertCollected(
                new Object[] {new Map[] {null, Collections.singletonMap("a", 1)}}, rowType, 5);
        assertCollected(new Object[] {new Map[] {null}}, rowType, 0);
    }

    @Test
    void testRowArrayCollection() throws Exception {
        SeaTunnelRowType elementType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "items"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE, new ArrayType<>(SeaTunnelRow[].class, elementType)
                        });
        assertCollected(
                new Object[] {
                    7, new SeaTunnelRow[] {null, new SeaTunnelRow(new Object[] {"abcd"})}
                },
                rowType,
                8);
    }

    private void assertCollected(Object[] fields, SeaTunnelRowType rowType, long expectedBytes)
            throws Exception {
        TablePath tablePath = TablePath.of("database", "table");
        for (boolean multiTable : new boolean[] {false, true}) {
            SeaTunnelRow row = new SeaTunnelRow(fields);
            row.setTableId(tablePath.getFullName());
            MetricsContext metrics = Mockito.mock(MetricsContext.class);
            when(metrics.counter(Mockito.anyString()))
                    .thenAnswer(invocation -> Mockito.mock(Counter.class));
            when(metrics.meter(Mockito.anyString()))
                    .thenAnswer(invocation -> Mockito.mock(Meter.class));
            Counter bytes = Mockito.mock(Counter.class);
            Counter count = Mockito.mock(Counter.class);
            Counter tableBytes = Mockito.mock(Counter.class);
            when(metrics.counter(SOURCE_RECEIVED_BYTES)).thenReturn(bytes);
            when(metrics.counter(SOURCE_RECEIVED_COUNT)).thenReturn(count);
            when(metrics.counter(SOURCE_RECEIVED_BYTES + "#" + tablePath.getFullName()))
                    .thenReturn(tableBytes);
            OneInputFlowLifeCycle<Record<?>> output = Mockito.mock(OneInputFlowLifeCycle.class);
            SeaTunnelTask sourceTask = Mockito.mock(SeaTunnelTask.class);
            when(sourceTask.getTaskLocation())
                    .thenReturn(new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 0));
            SeaTunnelSourceCollector<SeaTunnelRow> collector =
                    new SeaTunnelSourceCollector<>(
                            new Object(),
                            Collections.singletonList(output),
                            metrics,
                            FlowControlStrategy.ofBytes(1000),
                            multiTable
                                    ? new MultipleRowType(
                                            Collections.singletonMap(
                                                    tablePath.getFullName(), rowType))
                                    : rowType,
                            multiTable
                                    ? Collections.singletonList(tablePath)
                                    : Collections.emptyList(),
                            sourceTask,
                            new EngineConfig(),
                            Collections.emptyMap(),
                            null);

            collector.collect(row);

            verify(bytes).inc(expectedBytes);
            verify(count).inc();
            if (multiTable) {
                verify(tableBytes).inc(expectedBytes);
            }
            verify(output).received(Mockito.argThat(record -> record.getData() == row));
        }
    }
}
