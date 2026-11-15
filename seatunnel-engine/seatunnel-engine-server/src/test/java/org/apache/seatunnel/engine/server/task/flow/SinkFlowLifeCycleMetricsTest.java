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

import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableAggregatedCommitInfo;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableCommitInfo;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableState;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;

class SinkFlowLifeCycleMetricsTest {

    private static final TaskLocation TASK_LOCATION =
            new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 2L, 0);

    @Test
    void schemaFirstTableIdUsesResolvedSinkTableMetrics() throws Exception {
        TablePath sourceTable = TablePath.of(null, "PDC_SCHEMA", "CUSTOMER");
        TablePath sinkTable = TablePath.of("target_db", "CUSTOMER");
        MultiTableSink sink = Mockito.mock(MultiTableSink.class);
        Mockito.when(sink.getSinkTables()).thenReturn(Collections.singletonList(sinkTable));
        Mockito.when(sink.getSinkTableMapping())
                .thenReturn(Collections.singletonMap(sourceTable, sinkTable));
        SinkAction<
                        SeaTunnelRow,
                        MultiTableState,
                        MultiTableCommitInfo,
                        MultiTableAggregatedCommitInfo>
                action =
                        new SinkAction<>(
                                7L, "sink", sink, Collections.emptySet(), Collections.emptySet());
        SeaTunnelTask task = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(task.isObservabilityEnabled()).thenReturn(true);
        SeaTunnelMetricsContext metrics = new SeaTunnelMetricsContext();
        SinkFlowLifeCycle<
                        SeaTunnelRow,
                        MultiTableCommitInfo,
                        MultiTableAggregatedCommitInfo,
                        MultiTableState>
                flow =
                        new SinkFlowLifeCycle<>(
                                action,
                                TASK_LOCATION,
                                0,
                                task,
                                null,
                                false,
                                new CompletableFuture<>(),
                                metrics);
        setWriter(flow, Mockito.mock(SinkWriter.class));
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId(sourceTable.getFullName());

        flow.received(new Record<>(row));

        Assertions.assertEquals(
                1L,
                metrics.counter(MetricNames.SINK_WRITE_COUNT + "#" + sinkTable.getFullName())
                        .getCount());
        Assertions.assertEquals(
                0L,
                metrics.counter(
                                MetricNames.SINK_WRITE_COUNT
                                        + "#"
                                        + TablePath.DEFAULT.getFullName())
                        .getCount());
    }

    private static void setWriter(SinkFlowLifeCycle<?, ?, ?, ?> flow, SinkWriter<?, ?, ?> writer)
            throws Exception {
        Field field = SinkFlowLifeCycle.class.getDeclaredField("writer");
        field.setAccessible(true);
        field.set(flow, writer);
    }
}
