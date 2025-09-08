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

package org.apache.seatunnel.format.avro;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

class AvroSerializationSchemaTest {

    private static final LocalDate localDate = LocalDate.of(2023, 1, 1);
    private static final BigDecimal bigDecimal =
            new BigDecimal("61592600349703735722.724745739637773662");
    private static final LocalDateTime localDateTime = LocalDateTime.of(2023, 1, 1, 6, 30, 40);

    private SeaTunnelRow buildSeaTunnelRow() {
        SeaTunnelRow subSeaTunnelRow = new SeaTunnelRow(14);
        Map<String, String> map = new HashMap<>();
        map.put("k1", "1");
        map.put("k2", "2");
        String[] strArray = new String[] {"l1", "l2"};
        byte byteVal = 100;
        subSeaTunnelRow.setField(0, map);
        subSeaTunnelRow.setField(1, strArray);
        subSeaTunnelRow.setField(2, "strVal");
        subSeaTunnelRow.setField(3, true);
        subSeaTunnelRow.setField(4, 1);
        subSeaTunnelRow.setField(5, 2);
        subSeaTunnelRow.setField(6, 3);
        subSeaTunnelRow.setField(7, Long.MAX_VALUE - 1);
        subSeaTunnelRow.setField(8, 33.333F);
        subSeaTunnelRow.setField(9, 123.456);
        subSeaTunnelRow.setField(10, byteVal);
        subSeaTunnelRow.setField(11, localDate);
        subSeaTunnelRow.setField(12, bigDecimal);
        subSeaTunnelRow.setField(13, localDateTime);

        Map<String, Short> mapData = new HashMap<>();
        mapData.put("k1", Short.valueOf("1"));
        mapData.put("k2", Short.valueOf("2"));
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(15);
        seaTunnelRow.setField(0, mapData);
        seaTunnelRow.setField(1, strArray);
        seaTunnelRow.setField(2, "strVal");
        seaTunnelRow.setField(3, true);
        seaTunnelRow.setField(4, new Byte("1"));
        seaTunnelRow.setField(5, Short.valueOf("2"));
        seaTunnelRow.setField(6, 3);
        seaTunnelRow.setField(7, Long.MAX_VALUE - 1);
        seaTunnelRow.setField(8, 33.333F);
        seaTunnelRow.setField(9, 123.456);
        seaTunnelRow.setField(10, byteVal);
        seaTunnelRow.setField(11, localDate);
        seaTunnelRow.setField(12, bigDecimal);
        seaTunnelRow.setField(13, localDateTime);
        seaTunnelRow.setField(14, subSeaTunnelRow);
        return seaTunnelRow;
    }

    private SeaTunnelRowType buildSeaTunnelRowType() {
        String[] subField = {
            "c_map",
            "c_array",
            "c_string",
            "c_boolean",
            "c_tinyint",
            "c_smallint",
            "c_int",
            "c_bigint",
            "c_float",
            "c_double",
            "c_bytes",
            "c_date",
            "c_decimal",
            "c_timestamp"
        };
        SeaTunnelDataType<?>[] subFieldTypes = {
            new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
            ArrayType.STRING_ARRAY_TYPE,
            BasicType.STRING_TYPE,
            BasicType.BOOLEAN_TYPE,
            BasicType.INT_TYPE,
            BasicType.INT_TYPE,
            BasicType.INT_TYPE,
            BasicType.LONG_TYPE,
            BasicType.FLOAT_TYPE,
            BasicType.DOUBLE_TYPE,
            BasicType.BYTE_TYPE,
            LocalTimeType.LOCAL_DATE_TYPE,
            new DecimalType(38, 18),
            LocalTimeType.LOCAL_DATE_TIME_TYPE
        };

        String[] fieldNames = {
            "c_map",
            "c_array",
            "c_string",
            "c_boolean",
            "c_tinyint",
            "c_smallint",
            "c_int",
            "c_bigint",
            "c_float",
            "c_double",
            "c_bytes",
            "c_date",
            "c_decimal",
            "c_timestamp",
            "c_row"
        };
        SeaTunnelDataType<?>[] fieldTypes = {
            new MapType<>(BasicType.STRING_TYPE, BasicType.SHORT_TYPE),
            ArrayType.STRING_ARRAY_TYPE,
            BasicType.STRING_TYPE,
            BasicType.BOOLEAN_TYPE,
            BasicType.BYTE_TYPE,
            BasicType.SHORT_TYPE,
            BasicType.INT_TYPE,
            BasicType.LONG_TYPE,
            BasicType.FLOAT_TYPE,
            BasicType.DOUBLE_TYPE,
            BasicType.BYTE_TYPE,
            LocalTimeType.LOCAL_DATE_TYPE,
            new DecimalType(38, 18),
            LocalTimeType.LOCAL_DATE_TIME_TYPE,
            new SeaTunnelRowType(subField, subFieldTypes)
        };
        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    @Test
    public void testSerialization() throws IOException {
        SeaTunnelRowType rowType = buildSeaTunnelRowType();
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);
        SeaTunnelRow seaTunnelRow = buildSeaTunnelRow();
        AvroSerializationSchema serializationSchema = new AvroSerializationSchema(rowType);
        byte[] bytes = serializationSchema.serialize(seaTunnelRow);
        AvroDeserializationSchema deserializationSchema =
                new AvroDeserializationSchema(catalogTable);
        SeaTunnelRow deserialize = deserializationSchema.deserialize(bytes);
        String[] strArray1 = (String[]) seaTunnelRow.getField(1);
        String[] strArray2 = (String[]) deserialize.getField(1);
        Assertions.assertArrayEquals(strArray1, strArray2);
        SeaTunnelRow subRow = (SeaTunnelRow) deserialize.getField(14);
        Assertions.assertEquals((double) subRow.getField(9), 123.456);
        BigDecimal bigDecimal1 = (BigDecimal) subRow.getField(12);
        Assertions.assertEquals(bigDecimal1.compareTo(bigDecimal), 0);
        LocalDateTime localDateTime1 = (LocalDateTime) subRow.getField(13);
        Assertions.assertEquals(localDateTime1.compareTo(localDateTime), 0);
    }

    private SeaTunnelRow buildSeaTunnelRowValueNull() {
        SeaTunnelRow subSeaTunnelRow = new SeaTunnelRow(14);
        subSeaTunnelRow.setField(0, null);
        subSeaTunnelRow.setField(1, null);
        subSeaTunnelRow.setField(2, null);
        subSeaTunnelRow.setField(3, null);
        subSeaTunnelRow.setField(4, null);
        subSeaTunnelRow.setField(5, null);
        subSeaTunnelRow.setField(6, null);
        subSeaTunnelRow.setField(7, null);
        subSeaTunnelRow.setField(8, null);
        subSeaTunnelRow.setField(9, null);
        subSeaTunnelRow.setField(10, null);
        subSeaTunnelRow.setField(11, null);
        subSeaTunnelRow.setField(12, null);
        subSeaTunnelRow.setField(13, null);

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(15);
        seaTunnelRow.setField(0, null);
        seaTunnelRow.setField(1, null);
        seaTunnelRow.setField(2, null);
        seaTunnelRow.setField(3, null);
        seaTunnelRow.setField(4, null);
        seaTunnelRow.setField(5, null);
        seaTunnelRow.setField(6, null);
        seaTunnelRow.setField(7, null);
        seaTunnelRow.setField(8, null);
        seaTunnelRow.setField(9, null);
        seaTunnelRow.setField(10, null);
        seaTunnelRow.setField(11, null);
        seaTunnelRow.setField(12, null);
        seaTunnelRow.setField(13, null);
        seaTunnelRow.setField(14, subSeaTunnelRow);
        return seaTunnelRow;
    }

    @Test
    public void testSerializationValueNull() throws IOException {
        SeaTunnelRowType rowType = buildSeaTunnelRowType();
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);
        SeaTunnelRow seaTunnelRow = buildSeaTunnelRowValueNull();
        AvroSerializationSchema serializationSchema = new AvroSerializationSchema(rowType);
        byte[] bytes = serializationSchema.serialize(seaTunnelRow);
        AvroDeserializationSchema deserializationSchema =
                new AvroDeserializationSchema(catalogTable);
        SeaTunnelRow deserialize = deserializationSchema.deserialize(bytes);
        String[] strArray1 = (String[]) seaTunnelRow.getField(1);
        String[] strArray2 = (String[]) deserialize.getField(1);
        Assertions.assertArrayEquals(strArray1, strArray2);
        SeaTunnelRow subRow = (SeaTunnelRow) deserialize.getField(14);
        Assertions.assertEquals(subRow.getField(9), null);
        Assertions.assertEquals(subRow.getField(12), null);
        Assertions.assertEquals(subRow.getField(13), null);
    }
}
