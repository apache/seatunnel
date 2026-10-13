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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("doris", "db", "t"),
                        TableSchema.builder()
                                .columns(
                                        Arrays.asList(
                                                column("id", BasicType.INT_TYPE, "int"),
                                                column("v4", BasicType.STRING_TYPE, "ipv4"),
                                                column("v6", BasicType.STRING_TYPE, "ipv6"),
                                                column(
                                                        "name",
                                                        BasicType.STRING_TYPE,
                                                        "varchar(10)")))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");
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

    private static PhysicalColumn column(
            String name,
            org.apache.seatunnel.api.table.type.SeaTunnelDataType<?> type,
            String sourceType) {
        return PhysicalColumn.builder().name(name).dataType(type).sourceType(sourceType).build();
    }
}
