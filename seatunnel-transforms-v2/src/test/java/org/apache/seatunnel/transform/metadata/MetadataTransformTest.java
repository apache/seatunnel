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

package org.apache.seatunnel.transform.metadata;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.MetadataColumn;
import org.apache.seatunnel.api.table.catalog.MetadataSchema;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MetadataTransformTest {

    static CatalogTable catalogTable;

    static Object[] values;

    static SeaTunnelRow inputRow;

    static Long eventTime;

    @BeforeAll
    static void setUp() {
        List<Column> metadata = new ArrayList<>();
        metadata.add(
                MetadataColumn.of(
                        CommonOptions.EVENT_TIME.getName(),
                        BasicType.LONG_TYPE,
                        (Long) null,
                        true,
                        null,
                        null));
        metadata.add(
                MetadataColumn.of(
                        CommonOptions.DELAY.getName(),
                        BasicType.LONG_TYPE,
                        (Long) null,
                        true,
                        null,
                        null));
        metadata.add(
                MetadataColumn.of(
                        CommonOptions.PARTITION.getName(),
                        ArrayType.STRING_ARRAY_TYPE,
                        (Long) null,
                        true,
                        null,
                        null));
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
                        "comment",
                        "test",
                        MetadataSchema.builder().columns(metadata).build());
        values = new Object[] {"value1", 1, 896657703886127105L, 3.1415916, 3.14};
        inputRow = new SeaTunnelRow(values);
        inputRow.setTableId(TablePath.DEFAULT.getFullName());
        eventTime = LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli();
        MetadataUtil.setDelay(inputRow, 150L);
        MetadataUtil.setEventTime(inputRow, eventTime);
        MetadataUtil.setPartition(inputRow, Arrays.asList("key1", "key2").toArray(new String[0]));
    }

    @Test
    void testMetadataTransform() {
        Map<String, String> metadataMapping = new LinkedHashMap<>();
        metadataMapping.put("Database", "database");
        metadataMapping.put("Table", "table");
        metadataMapping.put("Partition", "partition");
        metadataMapping.put("RowKind", "rowKind");
        metadataMapping.put("EventTime", "ts_ms");
        metadataMapping.put("Delay", "delay");
        Map<String, Object> config = new HashMap<>();
        config.put("metadata_fields", metadataMapping);
        MetadataTransform transform =
                new MetadataTransform(ReadonlyConfig.fromMap(config), catalogTable);
        transform.initRowContainerGenerator();

        Column[] columns = transform.getOutputColumns();
        Assertions.assertEquals("database", columns[0].getName());
        Assertions.assertEquals("table", columns[1].getName());
        Assertions.assertEquals("partition", columns[2].getName());
        Assertions.assertEquals("rowKind", columns[3].getName());
        Assertions.assertEquals("ts_ms", columns[4].getName());
        Assertions.assertEquals("delay", columns[5].getName());

        Assertions.assertEquals(BasicType.STRING_TYPE, columns[0].getDataType());
        Assertions.assertEquals(BasicType.STRING_TYPE, columns[1].getDataType());
        Assertions.assertEquals(ArrayType.STRING_ARRAY_TYPE, columns[2].getDataType());
        Assertions.assertEquals(BasicType.STRING_TYPE, columns[3].getDataType());
        Assertions.assertEquals(BasicType.LONG_TYPE, columns[4].getDataType());
        Assertions.assertEquals(BasicType.LONG_TYPE, columns[5].getDataType());

        Assertions.assertInstanceOf(PhysicalColumn.class, columns[0]);
        Assertions.assertInstanceOf(PhysicalColumn.class, columns[5]);

        SeaTunnelRow outputRow = transform.map(inputRow);
        Assertions.assertEquals(values.length + 6, outputRow.getArity());
        Assertions.assertEquals("default.default.default", outputRow.getTableId());
        Assertions.assertEquals(RowKind.INSERT, outputRow.getRowKind());
        Assertions.assertEquals("value1", outputRow.getField(0));
        Assertions.assertEquals(1, outputRow.getField(1));
        Assertions.assertEquals(896657703886127105L, outputRow.getField(2));
        Assertions.assertEquals(3.1415916, outputRow.getField(3));
        Assertions.assertEquals(3.14, outputRow.getField(4));
        Assertions.assertEquals("default", outputRow.getField(5));
        Assertions.assertEquals("default", outputRow.getField(6));
        Assertions.assertArrayEquals(
                new String[] {"key1", "key2"}, (String[]) outputRow.getField(7));
        Assertions.assertEquals("+I", outputRow.getField(8));
        Assertions.assertEquals(eventTime, outputRow.getField(9));
        Assertions.assertEquals(150L, outputRow.getField(10));
    }
}
