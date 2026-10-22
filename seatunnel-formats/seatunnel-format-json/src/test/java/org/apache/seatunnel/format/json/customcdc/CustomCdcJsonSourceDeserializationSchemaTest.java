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

package org.apache.seatunnel.format.json.customcdc;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class CustomCdcJsonSourceDeserializationSchemaTest {

    @Test
    void shouldRouteMultiTableCdcRows() {
        CustomCdcJsonSourceDeserializationSchema schema = schema();
        ListCollector collector = new ListCollector();

        deserialize(
                schema,
                collector,
                "{\"schema\":\"public\",\"table\":\"student\",\"operation\":\"insert\","
                        + "\"before\":null,\"after\":{\"id\":1,\"name\":\"Alice\"}}");
        deserialize(
                schema,
                collector,
                "{\"schema\":\"public\",\"table\":\"teacher\",\"operation\":\"update\","
                        + "\"before\":{\"id\":2,\"name\":\"Old\"},"
                        + "\"after\":{\"id\":2,\"name\":\"New\"}}");
        deserialize(
                schema,
                collector,
                "{\"schema\":\"public\",\"table\":\"student\",\"operation\":\"delete\","
                        + "\"before\":{\"id\":3,\"name\":\"Bob\"},\"after\":null}");

        Assertions.assertEquals(4, collector.rows.size());
        assertRow(collector.rows.get(0), "public.student", RowKind.INSERT, 1, "Alice");
        assertRow(collector.rows.get(1), "public.teacher", RowKind.UPDATE_BEFORE, 2, "Old");
        assertRow(collector.rows.get(2), "public.teacher", RowKind.UPDATE_AFTER, 2, "New");
        assertRow(collector.rows.get(3), "public.student", RowKind.DELETE, 3, "Bob");
    }

    @Test
    void shouldSkipUnconfiguredTablesAndRejectIncompleteChanges() {
        CustomCdcJsonSourceDeserializationSchema schema = schema();
        ListCollector collector = new ListCollector();

        deserialize(
                schema,
                collector,
                "{\"schema\":\"archive\",\"table\":\"student\",\"operation\":\"insert\","
                        + "\"after\":{\"id\":1,\"name\":\"ignored\"}}");
        Assertions.assertTrue(collector.rows.isEmpty());

        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                deserialize(
                                        schema,
                                        collector,
                                        "{\"schema\":\"public\",\"table\":\"student\","
                                                + "\"operation\":\"update\",\"after\":{\"id\":1}}"));
        Assertions.assertTrue(exception.getMessage().contains("before data is null"));
    }

    @Test
    void shouldSupportConfiguredOperationMappingAfterSerialization() throws Exception {
        CatalogTable table = table("public.account");
        Map<TablePath, Map<String, String>> mappings =
                Collections.singletonMap(
                        table.getTablePath(),
                        new HashMap<String, String>() {
                            {
                                put("id", "$.ID");
                                put("name", "$.NAME");
                            }
                        });
        CustomCdcJsonSourceDeserializationSchema schema =
                CustomCdcJsonSourceDeserializationSchema.builder(Collections.singletonList(table))
                        .setJsonFieldMappings(mappings)
                        .setSchemaPath("$.schema")
                        .setTablePath("$.table")
                        .setOperationPath("$.operation")
                        .setOperationTypeMapping(Collections.singletonMap("A", "UPDATE_AFTER"))
                        .setBeforePath("$.before")
                        .setAfterPath("$.after")
                        .build();

        CustomCdcJsonSourceDeserializationSchema restoredSchema = roundTrip(schema);
        ListCollector collector = new ListCollector();
        deserialize(
                restoredSchema,
                collector,
                "{\"schema\":\"public\",\"table\":\"account\",\"operation\":\"A\","
                        + "\"after\":{\"ID\":7,\"NAME\":\"Ada\"}}");

        Assertions.assertEquals(1, collector.rows.size());
        assertRow(collector.rows.get(0), "public.account", RowKind.UPDATE_AFTER, 7, "Ada");
    }

    @Test
    void shouldRejectUnknownOperations() {
        CustomCdcJsonSourceDeserializationSchema schema = schema();
        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                deserialize(
                                        schema,
                                        new ListCollector(),
                                        "{\"schema\":\"public\",\"table\":\"student\","
                                                + "\"operation\":\"merge\",\"after\":{\"id\":1}}"));
        Assertions.assertTrue(
                exception.getMessage().contains("Unsupported CUSTOM_CDC_JSON operation"));
    }

    private static CustomCdcJsonSourceDeserializationSchema schema() {
        CatalogTable student = table("public.student");
        CatalogTable teacher = table("public.teacher");
        return CustomCdcJsonSourceDeserializationSchema.builder(Arrays.asList(student, teacher))
                .setSimpleTablePaths(Collections.emptySet())
                .setSchemaPath("$.schema")
                .setTablePath("$.table")
                .setOperationPath("$.operation")
                .setBeforePath("$.before")
                .setAfterPath("$.after")
                .build();
    }

    private static CatalogTable table(String tableName) {
        TablePath tablePath = TablePath.of(tableName);
        return CatalogTable.of(
                TableIdentifier.of("", tablePath),
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 0, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 0, true, null, null))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private static void deserialize(
            CustomCdcJsonSourceDeserializationSchema schema, ListCollector collector, String json) {
        schema.deserialize(json.getBytes(StandardCharsets.UTF_8), collector);
    }

    private static void assertRow(
            SeaTunnelRow row, String tableId, RowKind rowKind, int id, String name) {
        Assertions.assertEquals(tableId, row.getTableId());
        Assertions.assertEquals(rowKind, row.getRowKind());
        Assertions.assertEquals(id, row.getField(0));
        Assertions.assertEquals(name, row.getField(1));
    }

    private static CustomCdcJsonSourceDeserializationSchema roundTrip(
            CustomCdcJsonSourceDeserializationSchema schema) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutput = new ObjectOutputStream(output)) {
            objectOutput.writeObject(schema);
        }
        try (ObjectInputStream objectInput =
                new ObjectInputStream(new ByteArrayInputStream(output.toByteArray()))) {
            return (CustomCdcJsonSourceDeserializationSchema) objectInput.readObject();
        }
    }

    private static class ListCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return this;
        }
    }
}
