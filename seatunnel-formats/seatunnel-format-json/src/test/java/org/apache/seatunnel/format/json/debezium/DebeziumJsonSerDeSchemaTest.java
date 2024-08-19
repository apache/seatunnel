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

package org.apache.seatunnel.format.json.debezium;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.type.BasicType.FLOAT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DebeziumJsonSerDeSchemaTest {
    private static final String FORMAT = "Debezium";

    private static final SeaTunnelRowType SEATUNNEL_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name", "description", "weight"},
                    new SeaTunnelDataType[] {INT_TYPE, STRING_TYPE, STRING_TYPE, FLOAT_TYPE});
    private static final CatalogTable catalogTables =
            CatalogTableUtil.getCatalogTable("", "", "", "test", SEATUNNEL_ROW_TYPE);

    @Test
    void testNullRowMessages() throws Exception {
        DebeziumJsonDeserializationSchema deserializationSchema =
                new DebeziumJsonDeserializationSchema(catalogTables, false);
        SimpleCollector collector = new SimpleCollector();

        deserializationSchema.deserialize(null, collector);
        deserializationSchema.deserialize(new byte[0], collector);
        assertEquals(0, collector.list.size());
    }

    @Test
    public void testSerializationAndSchemaExcludeDeserialization() throws Exception {
        testSerializationDeserialization("debezium-data.txt", false);
    }

    @Test
    public void testDeserializeNoJson() throws Exception {
        final DebeziumJsonDeserializationSchema deserializationSchema =
                new DebeziumJsonDeserializationSchema(catalogTables, false);
        final SimpleCollector collector = new SimpleCollector();

        String noJsonMsg = "{]";

        SeaTunnelRuntimeException expected = CommonError.jsonOperationError(FORMAT, noJsonMsg);
        SeaTunnelRuntimeException cause =
                assertThrows(
                        expected.getClass(),
                        () -> {
                            deserializationSchema.deserialize(noJsonMsg.getBytes(), collector);
                        });
        assertEquals(cause.getMessage(), expected.getMessage());
    }

    @Test
    public void testDeserializeEmptyJson() throws Exception {
        final DebeziumJsonDeserializationSchema deserializationSchema =
                new DebeziumJsonDeserializationSchema(catalogTables, false);
        final SimpleCollector collector = new SimpleCollector();
        String emptyMsg = "{}";
        SeaTunnelRuntimeException expected = CommonError.jsonOperationError(FORMAT, emptyMsg);
        SeaTunnelRuntimeException cause =
                assertThrows(
                        expected.getClass(),
                        () -> {
                            deserializationSchema.deserialize(emptyMsg.getBytes(), collector);
                        });
        assertEquals(cause.getMessage(), expected.getMessage());
    }

    @Test
    public void testDeserializeNoDataJson() throws Exception {
        final DebeziumJsonDeserializationSchema deserializationSchema =
                new DebeziumJsonDeserializationSchema(catalogTables, false);
        final SimpleCollector collector = new SimpleCollector();
        String noDataMsg = "{\"op\":\"u\"}";
        SeaTunnelRuntimeException expected = CommonError.jsonOperationError(FORMAT, noDataMsg);
        SeaTunnelRuntimeException cause =
                assertThrows(
                        expected.getClass(),
                        () -> {
                            deserializationSchema.deserialize(noDataMsg.getBytes(), collector);
                        });
        assertEquals(cause.getMessage(), expected.getMessage());

        Throwable noDataCause = cause.getCause();
        assertEquals(noDataCause.getClass(), IllegalStateException.class);
        assertEquals(
                noDataCause.getMessage(),
                String.format(
                        "The \"before\" field of %s operation is null, "
                                + "if you are using Debezium Postgres Connector, "
                                + "please check the Postgres table has been set REPLICA IDENTITY to FULL level.",
                        "UPDATE"));
    }

    @Test
    public void testDeserializeUnknownOperationTypeJson() throws Exception {
        final DebeziumJsonDeserializationSchema deserializationSchema =
                new DebeziumJsonDeserializationSchema(catalogTables, false);
        final SimpleCollector collector = new SimpleCollector();
        String unknownType = "XX";
        String unknownOperationMsg =
                "{\"before\":null,\"after\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14},\"op\":\""
                        + unknownType
                        + "\"}";
        SeaTunnelRuntimeException expected =
                CommonError.jsonOperationError(FORMAT, unknownOperationMsg);
        SeaTunnelRuntimeException cause =
                assertThrows(
                        expected.getClass(),
                        () -> {
                            deserializationSchema.deserialize(
                                    unknownOperationMsg.getBytes(), collector);
                        });
        assertEquals(cause.getMessage(), expected.getMessage());

        Throwable unknownTypeCause = cause.getCause();
        assertEquals(unknownTypeCause.getClass(), IllegalStateException.class);
        assertEquals(
                unknownTypeCause.getMessage(),
                String.format("Unknown operation type '%s'.", unknownType));
    }

    private void testSerializationDeserialization(String resourceFile, boolean schemaInclude)
            throws Exception {
        List<String> lines = readLines(resourceFile);
        DebeziumJsonDeserializationSchema deserializationSchema =
                new DebeziumJsonDeserializationSchema(catalogTables, true, schemaInclude);

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
                        "SeaTunnelRow{tableId=..test, kind=-D, fields=[111, scooter, Big 2-wheel scooter , 5.17]}");
        List<String> actual =
                collector.list.stream().map(Object::toString).collect(Collectors.toList());
        assertEquals(expected, actual);

        DebeziumJsonSerializationSchema serializationSchema =
                new DebeziumJsonSerializationSchema(SEATUNNEL_ROW_TYPE);

        actual = new ArrayList<>();
        for (SeaTunnelRow rowData : collector.list) {
            actual.add(new String(serializationSchema.serialize(rowData), StandardCharsets.UTF_8));
        }

        expected =
                Arrays.asList(
                        "{\"before\":null,\"after\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":8.1},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":0.8},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":104,\"name\":\"hammer\",\"description\":\"12oz carpenter's hammer\",\"weight\":0.75},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":105,\"name\":\"hammer\",\"description\":\"14oz carpenter's hammer\",\"weight\":0.875},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":1.0},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.3},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":108,\"name\":\"jacket\",\"description\":\"water resistent black wind breaker\",\"weight\":0.1},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":22.2},\"op\":\"c\"}",
                        "{\"before\":{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":1.0},\"after\":null,\"op\":\"d\"}",
                        "{\"before\":null,\"after\":{\"id\":106,\"name\":\"hammer\",\"description\":\"18oz carpenter hammer\",\"weight\":1.0},\"op\":\"c\"}",
                        "{\"before\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.3},\"after\":null,\"op\":\"d\"}",
                        "{\"before\":null,\"after\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.1},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":0.2},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18},\"op\":\"c\"}",
                        "{\"before\":{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":0.2},\"after\":null,\"op\":\"d\"}",
                        "{\"before\":null,\"after\":{\"id\":110,\"name\":\"jacket\",\"description\":\"new water resistent white wind breaker\",\"weight\":0.5},\"op\":\"c\"}",
                        "{\"before\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18},\"after\":null,\"op\":\"d\"}",
                        "{\"before\":null,\"after\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.17},\"op\":\"c\"}",
                        "{\"before\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.17},\"after\":null,\"op\":\"d\"}");
        assertEquals(expected, actual);
    }
    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private static List<String> readLines(String resource) throws IOException {
        final URL url = DebeziumJsonSerDeSchemaTest.class.getClassLoader().getResource(resource);
        Assertions.assertNotNull(url);
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
