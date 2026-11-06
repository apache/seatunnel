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

package org.apache.seatunnel.connectors.doris.source.reader;

import org.apache.seatunnel.shade.org.apache.arrow.memory.RootAllocator;
import org.apache.seatunnel.shade.org.apache.arrow.vector.IntVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VarCharVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.seatunnel.shade.org.apache.arrow.vector.ipc.ArrowStreamWriter;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.arrow.reader.ArrowToSeatunnelRowReader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

public class DorisValueReaderIpv4Test {

    @Test
    public void testIpv4ToString() {
        // Doris BE sends IPV4 as a signed 32-bit integer
        Assertions.assertEquals("0.0.0.0", DorisValueReader.ipv4ToString(0));
        Assertions.assertEquals("255.255.255.255", DorisValueReader.ipv4ToString(-1));
        Assertions.assertEquals("192.168.0.1", DorisValueReader.ipv4ToString(-1062731775));
        Assertions.assertEquals("10.0.0.255", DorisValueReader.ipv4ToString(167772415));
        Assertions.assertEquals("255.255.255.255", DorisValueReader.ipv4ToString(4294967295L));
    }

    @Test
    public void testConvertOnlyIpv4Fields() {
        CatalogTable catalogTable = catalogTable();
        int[] indexes =
                DorisValueReader.ipv4FieldIndexes(catalogTable, catalogTable.getSeaTunnelRowType());
        Assertions.assertArrayEquals(new int[] {1}, indexes);

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, -1062731775, "2001:db8::1", "a"});
        DorisValueReader.convertIpv4Fields(row, indexes);
        Assertions.assertArrayEquals(
                new Object[] {1, "192.168.0.1", "2001:db8::1", "a"}, row.getFields());

        SeaTunnelRow nullRow = new SeaTunnelRow(new Object[] {2, null, null, null});
        DorisValueReader.convertIpv4Fields(nullRow, indexes);
        Assertions.assertNull(nullRow.getField(1));
    }

    @Test
    public void testConvertIpv4ReadFromArrowInt32() throws Exception {
        // the same Arrow shape Doris BE sends: IPV4 as Int(32, true), IPV6 as Utf8
        byte[] arrow;
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                IntVector id = new IntVector("id", allocator);
                IntVector v4 = new IntVector("v4", allocator);
                VarCharVector v6 = new VarCharVector("v6", allocator);
                VarCharVector name = new VarCharVector("name", allocator)) {
            id.allocateNew(2);
            v4.allocateNew(2);
            v6.allocateNew(2);
            name.allocateNew(2);
            id.set(0, 1);
            v4.set(0, -1062731775);
            v6.setSafe(0, "2001:db8::1".getBytes(StandardCharsets.UTF_8));
            name.setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
            id.set(1, 2);
            v4.setNull(1);
            v6.setNull(1);
            name.setNull(1);
            id.setValueCount(2);
            v4.setValueCount(2);
            v6.setValueCount(2);
            name.setValueCount(2);
            try (VectorSchemaRoot root = VectorSchemaRoot.of(id, v4, v6, name);
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    ArrowStreamWriter writer =
                            new ArrowStreamWriter(root, null, Channels.newChannel(output))) {
                writer.start();
                writer.writeBatch();
                writer.end();
                arrow = output.toByteArray();
            }
        }

        CatalogTable catalogTable = catalogTable();
        int[] indexes =
                DorisValueReader.ipv4FieldIndexes(catalogTable, catalogTable.getSeaTunnelRowType());
        try (ArrowToSeatunnelRowReader reader =
                new ArrowToSeatunnelRowReader(arrow, catalogTable.getSeaTunnelRowType())
                        .readArrow()) {
            SeaTunnelRow row = reader.next();
            // the Arrow reader keeps the integer even though the field type is STRING
            Assertions.assertEquals(-1062731775, row.getField(1));
            DorisValueReader.convertIpv4Fields(row, indexes);
            Assertions.assertArrayEquals(
                    new Object[] {1, "192.168.0.1", "2001:db8::1", "a"}, row.getFields());

            SeaTunnelRow nullRow = reader.next();
            DorisValueReader.convertIpv4Fields(nullRow, indexes);
            Assertions.assertArrayEquals(new Object[] {2, null, null, null}, nullRow.getFields());
        }
    }

    private static CatalogTable catalogTable() {
        return CatalogTable.of(
                TableIdentifier.of("doris", "db", "t"),
                TableSchema.builder()
                        .columns(
                                Arrays.asList(
                                        column("id", BasicType.INT_TYPE, "int"),
                                        column("v4", BasicType.STRING_TYPE, "ipv4"),
                                        column("v6", BasicType.STRING_TYPE, "ipv6"),
                                        column("name", BasicType.STRING_TYPE, "varchar(10)")))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    private static PhysicalColumn column(
            String name, SeaTunnelDataType<?> type, String sourceType) {
        return PhysicalColumn.builder().name(name).dataType(type).sourceType(sourceType).build();
    }
}
