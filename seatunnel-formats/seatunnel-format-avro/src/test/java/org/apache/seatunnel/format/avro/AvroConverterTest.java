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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

class AvroConverterTest {

    private SeaTunnelRow buildSeaTunnelRow() {
        SeaTunnelRow subSeaTunnelRow = new SeaTunnelRow(14);
        Map<String, String> map = new HashMap<String, String>();
        map.put("k1", "v1");
        map.put("k2", "v2");
        String[] strArray = new String[] {"l1", "l2"};
        byte byteVal = 100;
        LocalDate localDate = LocalDate.of(2023, 1, 1);

        BigDecimal bigDecimal = new BigDecimal("61592600349703735722.724745739637773662");
        LocalDateTime localDateTime = LocalDateTime.of(2023, 1, 1, 6, 30, 40);

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
        SeaTunnelRowType subRow = new SeaTunnelRowType(subField, subFieldTypes);

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
            subRow
        };
        SeaTunnelRowType rowType = new SeaTunnelRowType(fieldNames, fieldTypes);
        return rowType;
    }

    @Test
    public void testConverter() {

        SeaTunnelRowType rowType = buildSeaTunnelRowType();
        SeaTunnelRow seaTunnelRow = buildSeaTunnelRow();
        RowToAvroConverter rowToAvroConverter = new RowToAvroConverter(rowType);
        GenericRecord record = rowToAvroConverter.convertRowToGenericRecord(seaTunnelRow);

        AvroToRowConverter avroToRowConverter = new AvroToRowConverter(rowType);
        SeaTunnelRow converterRow = avroToRowConverter.converter(record, rowType);

        Assertions.assertEquals(converterRow, seaTunnelRow);
    }

    // The default mode is the legacy mode, so the GenericRecord shape built for a nested ROW must
    // stay exactly the shape that existed before the nullable ROW option was introduced: the nested
    // record still carries the historic top level record name and the same values. The explicit
    // false mode is the same public mode, so it has to produce that identical shape as well.
    @Test
    public void testLegacyNestedRecordShapeAndValuesUnchanged() {
        SeaTunnelRowType rowType = buildSeaTunnelRowType();
        SeaTunnelRow seaTunnelRow = buildSeaTunnelRow();
        SeaTunnelRowType nestedRowType = (SeaTunnelRowType) rowType.getFieldType(14);

        GenericRecord defaultRecord =
                new RowToAvroConverter(rowType).convertRowToGenericRecord(seaTunnelRow);
        GenericRecord explicitFalseRecord =
                new RowToAvroConverter(rowType, false).convertRowToGenericRecord(seaTunnelRow);

        Assertions.assertEquals(
                SeaTunnelRowTypeToAvroSchemaConverter.buildAvroSchemaWithRowType(rowType)
                        .toString(),
                defaultRecord.getSchema().toString());

        GenericRecord defaultNested = (GenericRecord) defaultRecord.get("c_row");
        GenericRecord explicitFalseNested = (GenericRecord) explicitFalseRecord.get("c_row");
        String legacyNestedSchema =
                SeaTunnelRowTypeToAvroSchemaConverter.buildAvroSchemaWithRowType(nestedRowType)
                        .toString();
        Assertions.assertEquals("SeaTunnelRecord", defaultNested.getSchema().getName());
        Assertions.assertEquals(legacyNestedSchema, defaultNested.getSchema().toString());
        Assertions.assertEquals("SeaTunnelRecord", explicitFalseNested.getSchema().getName());
        Assertions.assertEquals(legacyNestedSchema, explicitFalseNested.getSchema().toString());
        Assertions.assertEquals(defaultNested.toString(), explicitFalseNested.toString());
        Assertions.assertEquals("strVal", defaultNested.get("c_string"));
        Assertions.assertEquals(Integer.valueOf(3), defaultNested.get("c_int"));
    }

    // With the opt-in nullable layout a ROW field is a [null, record] union, so a present nested
    // row must be built under the name the enclosing schema gave the field, otherwise the write
    // fails with UnresolvedUnionException.
    @Test
    public void testPresentNestedRowSerializesWithNullableRow() throws IOException {
        SeaTunnelRowType rowType = buildSeaTunnelRowType();
        SeaTunnelRow seaTunnelRow = buildSeaTunnelRow();

        RowToAvroConverter converter = new RowToAvroConverter(rowType, true);
        GenericRecord record = converter.convertRowToGenericRecord(seaTunnelRow);
        Assertions.assertEquals(
                "c_row", ((GenericRecord) record.get("c_row")).getSchema().getName());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        converter.getWriter().write(record, encoder);
        encoder.flush();
        Assertions.assertTrue(out.size() > 0);
    }

    // Two ROW fields sit under different parents but share the same leaf ROW field name, and the
    // two nested records differ structurally: one carries a STRING and one an INT. With the opt-in
    // nullable layout the nested record name is derived from the field name, so both nested
    // records are named "detail" while describing different fields. The public binary round trip
    // still has to read every value back through its own parent branch, both for a present and for
    // a null nested row.
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSameLeafRowNameUnderDifferentParentsRoundTripsNullableRow(boolean present)
            throws IOException {
        SeaTunnelRowType leftDetailType =
                new SeaTunnelRowType(
                        new String[] {"code"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        SeaTunnelRowType rightDetailType =
                new SeaTunnelRowType(
                        new String[] {"amount"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});
        SeaTunnelRowType leftParentType =
                new SeaTunnelRowType(
                        new String[] {"detail"}, new SeaTunnelDataType<?>[] {leftDetailType});
        SeaTunnelRowType rightParentType =
                new SeaTunnelRowType(
                        new String[] {"detail"}, new SeaTunnelDataType<?>[] {rightDetailType});
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"left_parent", "right_parent"},
                        new SeaTunnelDataType<?>[] {leftParentType, rightParentType});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("", "", "", "test", rowType);

        SeaTunnelRow leftDetail = new SeaTunnelRow(new Object[] {"left-code"});
        SeaTunnelRow rightDetail = new SeaTunnelRow(new Object[] {7});
        SeaTunnelRow leftParent = new SeaTunnelRow(new Object[] {present ? leftDetail : null});
        SeaTunnelRow rightParent = new SeaTunnelRow(new Object[] {present ? rightDetail : null});
        SeaTunnelRow sourceRow = new SeaTunnelRow(new Object[] {leftParent, rightParent});

        byte[] bytes = new AvroSerializationSchema(rowType, true).serialize(sourceRow);
        SeaTunnelRow result =
                new AvroDeserializationSchema(catalogTable, null, false, true).deserialize(bytes);

        SeaTunnelRow leftResult = (SeaTunnelRow) result.getField(0);
        SeaTunnelRow rightResult = (SeaTunnelRow) result.getField(1);
        if (present) {
            SeaTunnelRow leftDetailResult = (SeaTunnelRow) leftResult.getField(0);
            SeaTunnelRow rightDetailResult = (SeaTunnelRow) rightResult.getField(0);
            Assertions.assertEquals("left-code", leftDetailResult.getField(0));
            Assertions.assertEquals(7, rightDetailResult.getField(0));
        } else {
            Assertions.assertNull(leftResult.getField(0));
            Assertions.assertNull(rightResult.getField(0));
        }
    }
}
