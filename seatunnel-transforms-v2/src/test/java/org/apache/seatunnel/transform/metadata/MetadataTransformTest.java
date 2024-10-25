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
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MetadataTransformTest {

    static CatalogTable catalogTable;

    static Object[] values;

    static SeaTunnelRow inputRow;

    static Long eventTime;

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
        inputRow.setTableId(TablePath.DEFAULT.getFullName());
        eventTime = LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli();
        MetadataUtil.setDelay(inputRow, 150L);
        MetadataUtil.setEventTime(inputRow, eventTime);
        MetadataUtil.setPartition(inputRow, Arrays.asList("key1", "key2").toArray(new String[0]));
    }

    @Test
    void testMetadataTransform() {
        Map<String, String> metadataMapping = new HashMap<>();
        metadataMapping.put("database", "database");
        metadataMapping.put("table", "table");
        metadataMapping.put("partition", "partition");
        metadataMapping.put("rowKind", "rowKind");
        metadataMapping.put("ts_ms", "ts_ms");
        metadataMapping.put("delay", "delay");
        Map<String, Object> config = new HashMap<>();
        config.put("metadata_fields", metadataMapping);
        MetadataTransform transform =
                new MetadataTransform(ReadonlyConfig.fromMap(config), catalogTable);
        transform.initRowContainerGenerator();
        SeaTunnelRow outputRow = transform.map(inputRow);
        Assertions.assertEquals(values.length + 6, outputRow.getArity());
        Assertions.assertEquals(
                "SeaTunnelRow{tableId=default.default.default, kind=+I, fields=[value1, 1, 896657703886127105, 3.1415916, 3.14, default, key1,key2, 150, default, INSERT, "
                        + eventTime
                        + "]}",
                outputRow.toString());
    }
}
