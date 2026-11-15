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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/** Verifies bounded sample reporting across each stage in a transform chain. */
class TransformFlowLifeCycleDryRunSampleTest {

    @Test
    void shouldApplyAndCountEveryTransformStage() throws Exception {
        SeaTunnelMapTransform<String> first = Mockito.mock(SeaTunnelMapTransform.class);
        SeaTunnelMapTransform<String> second = Mockito.mock(SeaTunnelMapTransform.class);
        when(first.map("row")).thenReturn("row-first");
        when(second.map("row-first")).thenReturn("row-second");

        TransformChainAction<String> action =
                new TransformChainAction<>(
                        1L,
                        "sample-chain",
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Arrays.asList(first, second));
        TransformFlowLifeCycle<String> flow =
                new TransformFlowLifeCycle<>(
                        action,
                        Mockito.mock(SeaTunnelTask.class),
                        new Collector<Record<?>>() {
                            @Override
                            public void collect(Record<?> record) {}

                            @Override
                            public void close() {}
                        },
                        new CompletableFuture<>());
        setField(flow, "dryRunSampleEnabled", true);
        setField(flow, "dryRunSamplePrintData", true);
        setField(flow, "dryRunSampleLimit", 1);

        assertEquals(Collections.singletonList("row-second"), flow.transform("row"));
        assertEquals(Collections.singletonList("row-second"), flow.transform("row"));

        assertArrayEquals(new int[] {1, 1}, (int[]) getField(flow, "dryRunSampleCounts"));
    }

    @Test
    void shouldExcludeCatalogOptionsFromSampleSchemaLog() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id", BasicType.INT_TYPE, 10L, 0, false, null, ""))
                                .build(),
                        Collections.singletonMap("password", "sample-secret"),
                        Collections.emptyList(),
                        "sample table");

        String schemaLog =
                TransformFlowLifeCycle.describeProducedSchemas(
                                Collections.singletonList(catalogTable))
                        .toString();

        assertTrue(schemaLog.contains("database.table"));
        assertTrue(schemaLog.contains("ROW<id INT>"));
        assertFalse(schemaLog.contains("password"));
        assertFalse(schemaLog.contains("sample-secret"));
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
