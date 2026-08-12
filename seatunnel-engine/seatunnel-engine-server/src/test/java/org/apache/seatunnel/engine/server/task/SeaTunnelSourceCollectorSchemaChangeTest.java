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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
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

import java.io.IOException;
import java.util.Collections;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeaTunnelSourceCollectorSchemaChangeTest {

    @Test
    void shouldUseRestoredSchemaBeforeCollectingPostRecoveryRow() throws IOException {
        SeaTunnelRowType initialRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        SeaTunnelRowType restoredRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "email"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });
        CatalogTable restoredTable =
                CatalogTableUtil.getCatalogTable("test_db.test_table", restoredRowType);

        OneInputFlowLifeCycle<Record<?>> output = Mockito.mock(OneInputFlowLifeCycle.class);
        SeaTunnelSourceCollector<SeaTunnelRow> collector =
                new SeaTunnelSourceCollector<>(
                        new Object(),
                        Collections.singletonList(output),
                        metricsContext(),
                        FlowControlStrategy.builder().build(),
                        initialRowType,
                        Collections.singletonList(TablePath.of("test_db", "test_table")),
                        sourceTask(),
                        new EngineConfig());

        collector.restoreSchema(Collections.singletonList(restoredTable));
        collector.collect(new SeaTunnelRow(new Object[] {1, "Alice", "alice@example.com"}));

        verify(output).received(Mockito.any());
    }

    @Test
    void shouldIgnoreSchemaChangeForUnknownTableInMultipleRowType() throws IOException {
        TablePath knownTable = TablePath.of("test_db", "known_table");
        TablePath unknownTable = TablePath.of("test_db", "unknown_table");
        SeaTunnelRowType knownRowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        MultipleRowType multipleRowType =
                new MultipleRowType(
                        new String[] {knownTable.toString()},
                        new SeaTunnelRowType[] {knownRowType});

        OneInputFlowLifeCycle<Record<?>> output = Mockito.mock(OneInputFlowLifeCycle.class);
        SeaTunnelSourceCollector<Object> collector =
                new SeaTunnelSourceCollector<>(
                        new Object(),
                        Collections.singletonList(output),
                        metricsContext(),
                        FlowControlStrategy.builder().build(),
                        multipleRowType,
                        Collections.singletonList(knownTable),
                        sourceTask(),
                        new EngineConfig());

        collector.collect(
                AlterTableChangeColumnEvent.change(
                        TableIdentifier.of("", unknownTable),
                        "old_name",
                        PhysicalColumn.of(
                                "new_name", BasicType.STRING_TYPE, (Long) null, true, null, null)));

        verify(output, never()).received(Mockito.any());
    }

    private static SeaTunnelTask sourceTask() {
        SeaTunnelTask sourceTask = Mockito.mock(SeaTunnelTask.class);
        when(sourceTask.getTaskLocation())
                .thenReturn(new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 0));
        return sourceTask;
    }

    private static MetricsContext metricsContext() {
        MetricsContext metricsContext = Mockito.mock(MetricsContext.class);
        when(metricsContext.counter(Mockito.anyString())).thenReturn(Mockito.mock(Counter.class));
        when(metricsContext.meter(Mockito.anyString())).thenReturn(Mockito.mock(Meter.class));
        return metricsContext;
    }
}
