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

package org.apache.seatunnel.transform.table;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class TableMergeTransformTest {

    private static TableMergeTransform createTransform() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "source", "main", "events"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "name",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                true,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);
        return new TableMergeTransform(
                new TableMergeConfig().setDatabase("target").setSchema("main").setTable("merged"),
                catalogTable);
    }

    private static SeaTunnelRow inputRow(String tableId) {
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {null});
        inputRow.setTableId(tableId);
        return inputRow;
    }

    @Test
    void testRemapCopiesInputPreservingFieldsNullRowKindOptionsAndSourceTableId() {
        for (RowKind kind : RowKind.values()) {
            SeaTunnelRow inputRow = inputRow("source.main.events");
            inputRow.setRowKind(kind);
            SeaTunnelRow outputRow = createTransform().map(inputRow);
            assertEquals("target.main.merged", outputRow.getTableId());
            assertNotSame(inputRow, outputRow);
            assertEquals(kind, outputRow.getRowKind());
            assertNull(outputRow.getFields()[0]);
            assertEquals("source.main.events", inputRow.getTableId());
        }
        Map<String, Object> options = new HashMap<>();
        options.put("marker", "1");
        SeaTunnelRow inputRow = inputRow("source.main.events");
        inputRow.setOptions(options);
        SeaTunnelRow outputRow = createTransform().map(inputRow);
        assertEquals("1", outputRow.getOptions().get("marker"));
        outputRow.getOptions().put("marker", "2");
        assertEquals("1", inputRow.getOptions().get("marker"));
    }

    @Test
    void testNullInputTableIdRemainsNullAndOutputUsesTarget() {
        SeaTunnelRow inputRow = inputRow(null);
        SeaTunnelRow outputRow = createTransform().map(inputRow);
        assertNull(inputRow.getTableId());
        assertEquals("target.main.merged", outputRow.getTableId());
        assertNotSame(inputRow, outputRow);
    }

    @Test
    void testAlreadyTargetReturnsSameInstance() {
        SeaTunnelRow inputRow = inputRow("target.main.merged");
        assertSame(inputRow, createTransform().transformRow(inputRow));
    }
}
