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

package org.apache.seatunnel.format.json.debezium;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.type.LocalTimeType.LOCAL_TIME_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DebeziumJsonDeserializationSchemaDispatcherTest {

    @Test
    void testDispatcher() throws IOException {
        List<String> actual =
                getRowsByTablePath(
                        TablePath.of("inventory.products"),
                        DebeziumJsonSerDeSchemaTest.catalogTables,
                        "debezium-data.txt");
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
                        "SeaTunnelRow{tableId=..test, kind=-D, fields=[111, scooter, Big 2-wheel scooter , 5.17]}");
        assertEquals(expected, actual);
    }

    @Test
    void testDispatcherFilterAllRow() throws IOException {
        List<String> actual =
                getRowsByTablePath(
                        TablePath.of("inventory.notExistTable"),
                        DebeziumJsonSerDeSchemaTest.catalogTables,
                        "debezium-data.txt");
        assertTrue(actual.isEmpty());
    }

    @Test
    void testDispatcherWithDBIsNullWithOracle() throws IOException {
        List<String> actual =
                getRowsByTablePath(
                        TablePath.of("ORCL", "QA_SOURCE", "ALL_TYPES1"),
                        DebeziumJsonSerDeSchemaTest.oracleTable,
                        "debezium-oracle.txt");
        List<String> actualWithOutDB =
                getRowsByTablePath(
                        TablePath.of(null, "QA_SOURCE", "ALL_TYPES1"),
                        DebeziumJsonSerDeSchemaTest.oracleTable,
                        "debezium-oracle.txt");
        assertEquals(actual, actualWithOutDB);
        assertEquals(1, actual.size());
    }

    @Test
    void testDispatcherWithSchemaEnabledDebeziumTimeUnits() throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"time_millis", "time_micros", "time_nanos"},
                        new SeaTunnelDataType[] {
                            LOCAL_TIME_TYPE, LOCAL_TIME_TYPE, LOCAL_TIME_TYPE
                        });
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("default", rowType);
        TablePath tablePath = TablePath.of("inventory", null, "products");
        Map<TablePath, DebeziumJsonDeserializationSchema> tableDeserializationMap = new HashMap<>();
        tableDeserializationMap.put(
                tablePath, new DebeziumJsonDeserializationSchema(catalogTable, false, true));
        DebeziumJsonDeserializationSchemaDispatcher dispatcher =
                new DebeziumJsonDeserializationSchemaDispatcher(
                        tableDeserializationMap, false, true);
        DebeziumJsonSerDeSchemaTest.SimpleCollector collector =
                new DebeziumJsonSerDeSchemaTest.SimpleCollector();

        String message =
                "{\"schema\":{\"type\":\"struct\",\"fields\":["
                        + "{\"type\":\"struct\",\"fields\":["
                        + "{\"type\":\"int32\",\"name\":\"io.debezium.time.Time\","
                        + "\"field\":\"time_millis\"},"
                        + "{\"type\":\"int64\",\"name\":\"io.debezium.time.MicroTime\","
                        + "\"field\":\"time_micros\"},"
                        + "{\"type\":\"int64\",\"name\":\"io.debezium.time.NanoTime\","
                        + "\"field\":\"time_nanos\"}],\"field\":\"before\"},"
                        + "{\"type\":\"struct\",\"fields\":["
                        + "{\"type\":\"int32\",\"name\":\"io.debezium.time.Time\","
                        + "\"field\":\"time_millis\"},"
                        + "{\"type\":\"int64\",\"name\":\"io.debezium.time.MicroTime\","
                        + "\"field\":\"time_micros\"},"
                        + "{\"type\":\"int64\",\"name\":\"io.debezium.time.NanoTime\","
                        + "\"field\":\"time_nanos\"}],\"field\":\"after\"}]},"
                        + "\"payload\":{\"before\":null,\"after\":{\"time_millis\":60000,"
                        + "\"time_micros\":60000000,\"time_nanos\":60000000000},"
                        + "\"source\":{\"db\":\"inventory\",\"table\":\"products\"},\"op\":\"c\"}}";

        dispatcher.deserialize(message.getBytes(StandardCharsets.UTF_8), collector);

        SeaTunnelRow row = collector.getList().get(0);
        assertEquals(LocalTime.of(0, 1), row.getField(0));
        assertEquals(LocalTime.of(0, 1), row.getField(1));
        assertEquals(LocalTime.of(0, 1), row.getField(2));
    }

    private List<String> getRowsByTablePath(
            TablePath tablePath, CatalogTable catalogTable, String dataFile) throws IOException {
        Map<TablePath, DebeziumJsonDeserializationSchema> tableDeserializationMap = new HashMap<>();
        tableDeserializationMap.put(
                tablePath, new DebeziumJsonDeserializationSchema(catalogTable, false));
        DebeziumJsonDeserializationSchemaDispatcher dispatcher =
                new DebeziumJsonDeserializationSchemaDispatcher(
                        tableDeserializationMap, false, false);

        List<String> lines = DebeziumJsonSerDeSchemaTest.readLines(dataFile);

        DebeziumJsonSerDeSchemaTest.SimpleCollector collector =
                new DebeziumJsonSerDeSchemaTest.SimpleCollector();

        for (String line : lines) {
            dispatcher.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }

        return collector.getList().stream().map(Object::toString).collect(Collectors.toList());
    }
}
