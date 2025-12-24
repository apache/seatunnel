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

package org.apache.seatunnel.transform.rowkind;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;

class RowKindExtractorTransformTest {

    static CatalogTable catalogTable;

    static Object[] values;

    static SeaTunnelRow inputRow;

    @BeforeAll
    static void setUp() {
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "key1",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key2",
                                                BasicType.INT_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key3",
                                                BasicType.LONG_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key4",
                                                BasicType.DOUBLE_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key5",
                                                BasicType.FLOAT_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "comment");
        values = new Object[] {"value1", 1, 896657703886127105L, 3.1415916, 3.14};
        inputRow = new SeaTunnelRow(values);
    }

    @Test
    void testCdcRowTransformShort() {
        RowKindExtractorTransform rowKindExtractorTransform =
                new RowKindExtractorTransform(
                        ReadonlyConfig.fromMap(new HashMap<>()), catalogTable);
        rowKindExtractorTransform.initRowContainerGenerator();
        SeaTunnelRow insertRow = inputRow.copy();
        Assertions.assertEquals(
                "SeaTunnelRow{tableId=, kind=+I, fields=[value1, 1, 896657703886127105, 3.1415916, 3.14, +I]}",
                rowKindExtractorTransform.transformRow(insertRow).toString());
        SeaTunnelRow updateBeforeRow = inputRow.copy();
        updateBeforeRow.setRowKind(RowKind.UPDATE_BEFORE);
        Assertions.assertEquals(
                "SeaTunnelRow{tableId=, kind=+I, fields=[value1, 1, 896657703886127105, 3.1415916, 3.14, -U]}",
                rowKindExtractorTransform.transformRow(updateBeforeRow).toString());
        SeaTunnelRow updateAfterRow = inputRow.copy();
        updateAfterRow.setRowKind(RowKind.UPDATE_AFTER);
        Assertions.assertEquals(
                "SeaTunnelRow{tableId=, kind=+I, fields=[value1, 1, 896657703886127105, 3.1415916, 3.14, +U]}",
                rowKindExtractorTransform.transformRow(updateAfterRow).toString());
        SeaTunnelRow deleteRow = inputRow.copy();
        deleteRow.setRowKind(RowKind.DELETE);
        Assertions.assertEquals(
                "SeaTunnelRow{tableId=, kind=+I, fields=[value1, 1, 896657703886127105, 3.1415916, 3.14, -D]}",
                rowKindExtractorTransform.transformRow(deleteRow).toString());
    }

    @Test
    void testCdcRowTransformFull() {
        HashMap<String, Object> conf = new HashMap<>();
        conf.put("transform_type", "FULL");
        RowKindExtractorTransform rowKindExtractorTransform =
                new RowKindExtractorTransform(ReadonlyConfig.fromMap(conf), catalogTable);
        rowKindExtractorTransform.initRowContainerGenerator();
        SeaTunnelRow insertRow = inputRow.copy();
        Assertions.assertEquals(
                "SeaTunnelRow{tableId=, kind=+I, fields=[value1, 1, 896657703886127105, 3.1415916, 3.14, INSERT]}",
                rowKindExtractorTransform.transformRow(insertRow).toString());
        SeaTunnelRow updateBeforeRow = inputRow.copy();
        updateBeforeRow.setRowKind(RowKind.UPDATE_BEFORE);
        Assertions.assertEquals(
                "SeaTunnelRow{tableId=, kind=+I, fields=[value1, 1, 896657703886127105, 3.1415916, 3.14, UPDATE_BEFORE]}",
                rowKindExtractorTransform.transformRow(updateBeforeRow).toString());
        SeaTunnelRow updateAfterRow = inputRow.copy();
        updateAfterRow.setRowKind(RowKind.UPDATE_AFTER);
        Assertions.assertEquals(
                "SeaTunnelRow{tableId=, kind=+I, fields=[value1, 1, 896657703886127105, 3.1415916, 3.14, UPDATE_AFTER]}",
                rowKindExtractorTransform.transformRow(updateAfterRow).toString());
        SeaTunnelRow deleteRow = inputRow.copy();
        deleteRow.setRowKind(RowKind.DELETE);
        Assertions.assertEquals(
                "SeaTunnelRow{tableId=, kind=+I, fields=[value1, 1, 896657703886127105, 3.1415916, 3.14, DELETE]}",
                rowKindExtractorTransform.transformRow(deleteRow).toString());
    }
}
