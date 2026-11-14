/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.json.maxwell;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.type.BasicType.FLOAT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class MaxWellJsonSerDeSchemaTest {

    private static final SeaTunnelRowType SEATUNNEL_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name", "description", "weight"},
                    new SeaTunnelDataType[] {INT_TYPE, STRING_TYPE, STRING_TYPE, FLOAT_TYPE});
    private static final CatalogTable catalogTables =
            CatalogTableUtil.getCatalogTable("", "", "", "test", SEATUNNEL_ROW_TYPE);

    @Test
    public void testFilteringTables() throws Exception {
        List<String> lines = readLines("maxwell-data-filter-table.txt");
        MaxWellJsonDeserializationSchema deserializationSchema =
                new MaxWellJsonDeserializationSchema.Builder(catalogTables)
                        .setDatabase("^test.*")
                        .setTable("^prod.*")
                        .build();
        runTest(lines, deserializationSchema);
    }

    @Test
    public void testSparseOldImageDoesNotApplyCatalogDefaults() {
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("test", TablePath.of("test.test")),
                        TableSchema.builder()
                                .columns(
                                        Arrays.asList(
                                                PhysicalColumn.of(
                                                        "id",
                                                        INT_TYPE,
                                                        (Long) null,
                                                        false,
                                                        null,
                                                        null),
                                                PhysicalColumn.of(
                                                        "name",
                                                        STRING_TYPE,
                                                        (Long) null,
                                                        false,
                                                        "default-name",
                                                        null),
                                                PhysicalColumn.of(
                                                        "description",
                                                        STRING_TYPE,
                                                        (Long) null,
                                                        false,
                                                        "default-description",
                                                        null)))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "test");
        MaxWellJsonDeserializationSchema schema =
                MaxWellJsonDeserializationSchema.builder(table).build();
        SimpleCollector collector = new SimpleCollector();

        schema.deserialize(
                "{\"database\":\"\",\"table\":\"test\",\"type\":\"update\",\"old\":{\"id\":1,\"name\":null},\"data\":{\"id\":1,\"name\":\"after\",\"description\":\"before-description\"}}"
                        .getBytes(StandardCharsets.UTF_8),
                collector);

        assertEquals(2, collector.list.size());
        SeaTunnelRow before = collector.list.get(0);
        SeaTunnelRow after = collector.list.get(1);
        assertEquals(RowKind.UPDATE_BEFORE, before.getRowKind());
        assertEquals(RowKind.UPDATE_AFTER, after.getRowKind());
        assertEquals(1, before.getField(0));
        assertNull(before.getField(1));
        assertEquals("before-description", before.getField(2));
        assertEquals(1, after.getField(0));
        assertEquals("after", after.getField(1));
        assertEquals("before-description", after.getField(2));
    }

    @Test
    public void testMainImagesPreserveExplicitNullAndDefaultMissingFields() throws IOException {
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("test", TablePath.of("test.test")),
                        TableSchema.builder()
                                .columns(
                                        Arrays.asList(
                                                PhysicalColumn.of(
                                                        "id",
                                                        INT_TYPE,
                                                        (Long) null,
                                                        true,
                                                        null,
                                                        null),
                                                PhysicalColumn.of(
                                                        "name",
                                                        STRING_TYPE,
                                                        (Long) null,
                                                        true,
                                                        "default-name",
                                                        null),
                                                PhysicalColumn.of(
                                                        "description",
                                                        STRING_TYPE,
                                                        (Long) null,
                                                        true,
                                                        "default-description",
                                                        null)))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "test");
        MaxWellJsonDeserializationSchema schema =
                MaxWellJsonDeserializationSchema.builder(table).build();
        SimpleCollector collector = new SimpleCollector();
        schema.deserialize(
                "{\"database\":\"test\",\"table\":\"test\",\"type\":\"insert\",\"data\":{\"id\":1,\"name\":null}}"
                        .getBytes(StandardCharsets.UTF_8),
                collector);
        schema.deserialize(
                "{\"database\":\"test\",\"table\":\"test\",\"type\":\"update\",\"data\":{\"id\":1,\"name\":null},\"old\":{\"name\":\"before\"}}"
                        .getBytes(StandardCharsets.UTF_8),
                collector);
        schema.deserialize(
                "{\"database\":\"test\",\"table\":\"test\",\"type\":\"delete\",\"data\":{\"id\":1,\"name\":null}}"
                        .getBytes(StandardCharsets.UTF_8),
                collector);
        assertEquals(4, collector.list.size());
        assertEquals(RowKind.INSERT, collector.list.get(0).getRowKind());
        assertEquals(RowKind.UPDATE_BEFORE, collector.list.get(1).getRowKind());
        assertEquals(RowKind.UPDATE_AFTER, collector.list.get(2).getRowKind());
        assertEquals(RowKind.DELETE, collector.list.get(3).getRowKind());
        for (int i = 0; i < collector.list.size(); i++) {
            SeaTunnelRow row = collector.list.get(i);
            assertEquals(1, row.getField(0));
            if (i == 1) {
                assertEquals("before", row.getField(1));
            } else {
                assertNull(row.getField(1));
            }
            assertEquals("default-description", row.getField(2));
        }
    }

    @Test
    public void testDeserializeNullRow() throws Exception {
        final MaxWellJsonDeserializationSchema deserializationSchema =
                createMaxWellJsonDeserializationSchema(null, null);
        final SimpleCollector collector = new SimpleCollector();

        deserializationSchema.deserialize(null, collector);
        assertEquals(0, collector.list.size());
    }

    public void runTest(List<String> lines, MaxWellJsonDeserializationSchema deserializationSchema)
            throws IOException {
        SimpleCollector collector = new SimpleCollector();
        for (String line : lines) {
            deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }
        List<String> expected =
                Arrays.asList(
                        "SeaTunnelRow{tableId=..test, kind=+I, fields=[101, scooter, Small 2-wheel scooter, 3.14]}",
                        "SeaTunnelRow{tableId=..test, kind=+I, fields=[102, car battery, 12V car battery, 8.1]}",
                        "SeaTunnelRow{tableId=..test, kind=+I, fields=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]}",
                        "SeaTunnelRow{tableId=..test, kind=+I, fields=[104, hammer, 12oz carpenter's hammer, 0.75]}",
                        "SeaTunnelRow{tableId=..test, kind=+I, fields=[105, hammer, 14oz carpenter's hammer, 0.875]}",
                        "SeaTunnelRow{tableId=..test, kind=+I, fields=[106, hammer, 16oz carpenter's hammer, 1.0]}",
                        "SeaTunnelRow{tableId=..test, kind=+I, fields=[107, rocks, box of assorted rocks, 5.3]}",
                        "SeaTunnelRow{tableId=..test, kind=+I, fields=[108, jacket, water resistent black wind breaker, 0.1]}",
                        "SeaTunnelRow{tableId=..test, kind=+I, fields=[109, spare tire, 24 inch spare tire, 22.2]}",
                        "SeaTunnelRow{tableId=..test, kind=-U, fields=[106, hammer, 16oz carpenter's hammer, 1.0]}",
                        "SeaTunnelRow{tableId=..test, kind=+U, fields=[106, hammer, 18oz carpenter hammer, 1.0]}",
                        "SeaTunnelRow{tableId=..test, kind=-U, fields=[107, rocks, box of assorted rocks, 5.3]}",
                        "SeaTunnelRow{tableId=..test, kind=+U, fields=[107, rocks, box of assorted rocks, 5.1]}",
                        "SeaTunnelRow{tableId=..test, kind=+I, fields=[110, jacket, water resistent white wind breaker, 0.2]}",
                        "SeaTunnelRow{tableId=..test, kind=+I, fields=[111, scooter, Big 2-wheel scooter , 5.18]}",
                        "SeaTunnelRow{tableId=..test, kind=-U, fields=[110, jacket, water resistent white wind breaker, 0.2]}",
                        "SeaTunnelRow{tableId=..test, kind=+U, fields=[110, jacket, new water resistent white wind breaker, 0.5]}",
                        "SeaTunnelRow{tableId=..test, kind=-U, fields=[111, scooter, Big 2-wheel scooter , 5.18]}",
                        "SeaTunnelRow{tableId=..test, kind=+U, fields=[111, scooter, Big 2-wheel scooter , 5.17]}",
                        "SeaTunnelRow{tableId=..test, kind=-D, fields=[111, scooter, Big 2-wheel scooter , 5.17]}",
                        "SeaTunnelRow{tableId=..test, kind=-U, fields=[101, scooter, Small 2-wheel scooter, 3.14]}",
                        "SeaTunnelRow{tableId=..test, kind=+U, fields=[101, scooter, Small 2-wheel scooter, 5.17]}",
                        "SeaTunnelRow{tableId=..test, kind=-U, fields=[102, car battery, 12V car battery, 8.1]}",
                        "SeaTunnelRow{tableId=..test, kind=+U, fields=[102, car battery, 12V car battery, 5.17]}",
                        "SeaTunnelRow{tableId=..test, kind=-D, fields=[102, car battery, 12V car battery, 5.17]}",
                        "SeaTunnelRow{tableId=..test, kind=-D, fields=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]}");
        List<String> actual =
                collector.list.stream().map(Object::toString).collect(Collectors.toList());
        assertEquals(expected, actual);

        // test Serialization
        MaxWellJsonSerializationSchema serializationSchema =
                new MaxWellJsonSerializationSchema(catalogTables.getSeaTunnelRowType());
        List<String> result = new ArrayList<>();
        for (SeaTunnelRow rowData : collector.list) {
            result.add(new String(serializationSchema.serialize(rowData), StandardCharsets.UTF_8));
        }

        List<String> expectedResult =
                Arrays.asList(
                        "{\"old\":null,\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":8.1},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":0.8},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":104,\"name\":\"hammer\",\"description\":\"12oz carpenter's hammer\",\"weight\":0.75},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":105,\"name\":\"hammer\",\"description\":\"14oz carpenter's hammer\",\"weight\":0.875},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":1.0},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.3},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":108,\"name\":\"jacket\",\"description\":\"water resistent black wind breaker\",\"weight\":0.1},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":22.2},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":1.0},\"type\":\"delete\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684893000}",
                        "{\"old\":null,\"data\":{\"id\":106,\"name\":\"hammer\",\"description\":\"18oz carpenter hammer\",\"weight\":1.0},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684893000}",
                        "{\"old\":null,\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.3},\"type\":\"delete\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684897000}",
                        "{\"old\":null,\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.1},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684897000}",
                        "{\"old\":null,\"data\":{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":0.2},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684900000}",
                        "{\"old\":null,\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684904000}",
                        "{\"old\":null,\"data\":{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":0.2},\"type\":\"delete\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684906000}",
                        "{\"old\":null,\"data\":{\"id\":110,\"name\":\"jacket\",\"description\":\"new water resistent white wind breaker\",\"weight\":0.5},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684906000}",
                        "{\"old\":null,\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18},\"type\":\"delete\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684912000}",
                        "{\"old\":null,\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.17},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684912000}",
                        "{\"old\":null,\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.17},\"type\":\"delete\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684914000}",
                        "{\"old\":null,\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14},\"type\":\"delete\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684928000}",
                        "{\"old\":null,\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":5.17},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684928000}",
                        "{\"old\":null,\"data\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":8.1},\"type\":\"delete\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684928000}",
                        "{\"old\":null,\"data\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":5.17},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684928000}",
                        "{\"old\":null,\"data\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":5.17},\"type\":\"delete\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684938000}",
                        "{\"old\":null,\"data\":{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":0.8},\"type\":\"delete\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684938000}");
        assertEquals(expectedResult, result);

        // test merge_update_event
        serializationSchema =
                new MaxWellJsonSerializationSchema(
                        catalogTables.getSeaTunnelRowType(), StandardCharsets.UTF_8, true);
        actual.clear();
        for (SeaTunnelRow rowData : collector.list) {
            if (serializationSchema.serialize(rowData) != null) {
                actual.add(
                        new String(serializationSchema.serialize(rowData), StandardCharsets.UTF_8));
            }
        }
        expected =
                Arrays.asList(
                        "{\"old\":null,\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":8.1},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":0.8},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":104,\"name\":\"hammer\",\"description\":\"12oz carpenter's hammer\",\"weight\":0.75},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":105,\"name\":\"hammer\",\"description\":\"14oz carpenter's hammer\",\"weight\":0.875},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":1.0},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.3},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":108,\"name\":\"jacket\",\"description\":\"water resistent black wind breaker\",\"weight\":0.1},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":null,\"data\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":22.2},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684883000}",
                        "{\"old\":{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":1.0},\"data\":{\"id\":106,\"name\":\"hammer\",\"description\":\"18oz carpenter hammer\",\"weight\":1.0},\"type\":\"update\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684893000}",
                        "{\"old\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.3},\"data\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.1},\"type\":\"update\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684897000}",
                        "{\"old\":null,\"data\":{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":0.2},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684900000}",
                        "{\"old\":null,\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18},\"type\":\"insert\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684904000}",
                        "{\"old\":{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":0.2},\"data\":{\"id\":110,\"name\":\"jacket\",\"description\":\"new water resistent white wind breaker\",\"weight\":0.5},\"type\":\"update\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684906000}",
                        "{\"old\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18},\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.17},\"type\":\"update\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684912000}",
                        "{\"old\":null,\"data\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.17},\"type\":\"delete\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684914000}",
                        "{\"old\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14},\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":5.17},\"type\":\"update\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684928000}",
                        "{\"old\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":8.1},\"data\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":5.17},\"type\":\"update\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684928000}",
                        "{\"old\":null,\"data\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":5.17},\"type\":\"delete\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684938000}",
                        "{\"old\":null,\"data\":{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":0.8},\"type\":\"delete\",\"database\":\"\",\"table\":\"test\",\"ts\":1596684938000}");
        assertEquals(expected, actual);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private MaxWellJsonDeserializationSchema createMaxWellJsonDeserializationSchema(
            String database, String table) {
        return MaxWellJsonDeserializationSchema.builder(catalogTables)
                .setDatabase(database)
                .setTable(table)
                .setIgnoreParseErrors(false)
                .build();
    }

    private static List<String> readLines(String resource) throws IOException {
        final URL url = MaxWellJsonSerDeSchemaTest.class.getClassLoader().getResource(resource);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    private static class SimpleCollector implements Collector<SeaTunnelRow> {

        private List<SeaTunnelRow> list = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            list.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return null;
        }
    }
}
