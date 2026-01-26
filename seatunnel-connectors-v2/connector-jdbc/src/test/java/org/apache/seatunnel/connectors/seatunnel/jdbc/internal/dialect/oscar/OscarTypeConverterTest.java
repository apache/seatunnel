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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oscar;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

public class OscarTypeConverterTest {
    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("aaa").dataType("aaa").build();
        try {
            OscarTypeConverter.INSTANCE.convert(typeDefine);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testReconvertUnsupported() {
        Column column =
                PhysicalColumn.of(
                        "test",
                        new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                        (Long) null,
                        true,
                        null,
                        null);
        try {
            OscarTypeConverter.INSTANCE.reconvert(column);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testConvertBit() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("bit").dataType("bit").build();
        Column column = OscarTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertInt() {
        /** tinyint */
        testConvertType("test", "tinyint", "tinyint", BasicType.BYTE_TYPE);
        testConvertType("test", "int1", "int1", BasicType.BYTE_TYPE);
        /** smallint */
        testConvertType("test", "int2", "int2", BasicType.SHORT_TYPE);
        testConvertType("test", "smallint", "smallint", BasicType.SHORT_TYPE);
        /** int */
        testConvertType("test", "int4", "int4", BasicType.INT_TYPE);
        testConvertType("test", "integer", "integer", BasicType.INT_TYPE);
        testConvertType("test", "int", "int", BasicType.INT_TYPE);
        /** bigint */
        testConvertType("test", "int8", "int8", BasicType.LONG_TYPE);
        testConvertType("test", "bigint", "bigint", BasicType.LONG_TYPE);
    }

    @Test
    public void testConvertFloat() {
        testConvertType("test", "real", "real", BasicType.FLOAT_TYPE);
        testConvertType("test", "float4", "float4", BasicType.FLOAT_TYPE);
        testConvertType("test", "float", "float", BasicType.DOUBLE_TYPE);
        testConvertType("test", "float8", "float8", BasicType.DOUBLE_TYPE);
        testConvertType("test", "double", "double", BasicType.DOUBLE_TYPE);
        testConvertType("test", "double precision", "double precision", BasicType.DOUBLE_TYPE);
    }

    @Test
    public void testConvertDecimal() {
        testConvertDecimalType("test", "numeric(10,2)", "numeric", 10L, 2, new DecimalType(10, 2));
        testConvertDecimalType("test", "number(10,2)", "number", 10L, 2, new DecimalType(10, 2));
        testConvertDecimalType("test", "decimal(10,2)", "decimal", 10L, 2, new DecimalType(10, 2));
    }

    @Test
    public void testConvertString() {
        testConvertStringType("test", "char(2)", "char", 2L, "char(%s)", BasicType.STRING_TYPE);
        testConvertStringType("test", "bpchar(3)", "bpchar", 3L, "char(%s)", BasicType.STRING_TYPE);
        testConvertStringType(
                "test", "character(4)", "character", 4L, "char(%s)", BasicType.STRING_TYPE);
    }

    @Test
    public void testConvertLob() {
        testConvertBinaryType(
                "test",
                "text",
                "text",
                (2L << 23) - 1,
                BasicTypeDefine::getColumnType,
                BasicType.STRING_TYPE);
        testConvertBinaryType(
                "test",
                "long",
                "long",
                (2L << 32) - 1,
                BasicTypeDefine::getColumnType,
                BasicType.STRING_TYPE);
        testConvertBinaryType(
                "test",
                "bfile",
                "bfile",
                (2L << 32) - 1,
                BasicTypeDefine::getColumnType,
                BasicType.STRING_TYPE);
        testConvertBinaryType(
                "test",
                "blob",
                "blob",
                (2L << 32) - 1,
                BasicTypeDefine::getColumnType,
                PrimitiveByteArrayType.INSTANCE);
    }

    @Test
    public void testConvertDatetime() {
        testConvertType("test", "date", "date", LocalTimeType.LOCAL_DATE_TYPE);

        testConvertType("test", "time", "time", LocalTimeType.LOCAL_TIME_TYPE);
        testConvertDataTimeType(
                "test",
                "time(6)",
                "time",
                6,
                typeDefine -> String.format("time(%s)", typeDefine.getScale()),
                LocalTimeType.LOCAL_TIME_TYPE);

        testConvertType(
                "test",
                "time with time zone",
                "time with time zone",
                LocalTimeType.LOCAL_TIME_TYPE);
        testConvertDataTimeType(
                "test",
                "time(6) with time zone",
                "time with time zone",
                6,
                typeDefine -> String.format("time(%s) with time zone", typeDefine.getScale()),
                LocalTimeType.LOCAL_TIME_TYPE);

        testConvertType("test", "datetime", "datetime", LocalTimeType.LOCAL_DATE_TIME_TYPE);
        testConvertDataTimeType(
                "test",
                "datetime(6)",
                "datetime",
                6,
                typeDefine -> String.format("datetime(%s)", typeDefine.getScale()),
                LocalTimeType.LOCAL_DATE_TIME_TYPE);

        testConvertType("test", "timestamp", "timestamp", LocalTimeType.LOCAL_DATE_TIME_TYPE);
        testConvertDataTimeType(
                "test",
                "timestamp(6)",
                "timestamp",
                6,
                typeDefine -> String.format("timestamp(%s)", typeDefine.getScale()),
                LocalTimeType.LOCAL_DATE_TIME_TYPE);
    }

    @Test
    public void testConvertBinary() {
        testConvertBinaryType(
                "test",
                "binary(2)",
                "BINARY",
                2L,
                typeDefine -> String.format("binary(%s)", typeDefine.getLength()),
                PrimitiveByteArrayType.INSTANCE);
        testConvertBinaryType(
                "test",
                "varbinary(3)",
                "VARBINARY",
                3L,
                typeDefine -> String.format("varbinary(%s)", typeDefine.getLength()),
                PrimitiveByteArrayType.INSTANCE);
    }

    private void testConvertType(
            String colName, String colType, String colDataType, SeaTunnelDataType<?> basicType) {
        testConvertType(
                colName,
                colType,
                colDataType,
                null,
                null,
                null,
                null,
                BasicTypeDefine::getColumnType,
                basicType);
    }

    private void testConvertStringType(
            String colName,
            String colType,
            String colDataType,
            Long charLen,
            String typeFormat,
            SeaTunnelDataType<?> basicType) {
        testConvertType(
                colName,
                colType,
                colDataType,
                charLen,
                null,
                null,
                null,
                typeDefine -> String.format(typeFormat, typeDefine.getLength()),
                basicType);
    }

    private void testConvertBinaryType(
            String colName,
            String colType,
            String colDataType,
            Long byteLen,
            Function<BasicTypeDefine, String> typeNameMapping,
            SeaTunnelDataType<?> basicType) {
        testConvertType(
                colName,
                colType,
                colDataType,
                null,
                byteLen,
                null,
                null,
                typeNameMapping,
                basicType);
    }

    private void testConvertDataTimeType(
            String colName,
            String colType,
            String colDataType,
            Integer scale,
            Function<BasicTypeDefine, String> typeNameMapping,
            SeaTunnelDataType<?> basicType) {
        testConvertType(
                colName, colType, colDataType, null, null, null, scale, typeNameMapping, basicType);
    }

    private void testConvertDecimalType(
            String colName,
            String colType,
            String colDataType,
            Long precision,
            Integer scale,
            BasicType<?> basicType) {
        testConvertType(
                colName,
                colType,
                colDataType,
                null,
                null,
                precision,
                scale,
                typeDefine ->
                        String.format(
                                "decimal(%s,%s)", typeDefine.getPrecision(), typeDefine.getScale()),
                basicType);
    }

    private void testConvertType(
            String colName,
            String colType,
            String colDataType,
            Long charLen,
            Long byteLen,
            Long precision,
            Integer scale,
            Function<BasicTypeDefine, String> typeNameMapping,
            SeaTunnelDataType<?> basicType) {

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(colName)
                        .columnType(colType)
                        .dataType(colDataType)
                        .length(charLen != null ? charLen : byteLen)
                        .precision(precision)
                        .scale(scale)
                        .build();
        Column column = OscarTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(basicType, column.getDataType());

        if (charLen != null) {
            Assertions.assertEquals(charLen * 4, column.getColumnLength());
        } else if (byteLen != null) {
            Assertions.assertEquals(byteLen, column.getColumnLength());
        }

        if (precision != null) {
            Assertions.assertEquals(precision, column.getColumnLength());
        }
        if (scale != null) {
            Assertions.assertEquals(scale, column.getScale());
        }
        Assertions.assertEquals(
                typeNameMapping.apply(typeDefine), column.getSourceType().toLowerCase());
    }

    @Test
    public void testReconvertBoolean() {
        testReconvertType(
                "test",
                BasicType.BOOLEAN_TYPE,
                OscarTypeConverter.OSCAR_BIT,
                OscarTypeConverter.OSCAR_BIT);
    }

    @Test
    public void testReconvertInt() {
        testReconvertType(
                "test",
                BasicType.SHORT_TYPE,
                OscarTypeConverter.OSCAR_SMALLINT,
                OscarTypeConverter.OSCAR_SMALLINT);

        testReconvertType(
                "test",
                BasicType.INT_TYPE,
                OscarTypeConverter.OSCAR_INT,
                OscarTypeConverter.OSCAR_INT);

        testReconvertType(
                "test",
                BasicType.LONG_TYPE,
                OscarTypeConverter.OSCAR_BIGINT,
                OscarTypeConverter.OSCAR_BIGINT);
    }

    @Test
    public void testReconvertFloat() {
        testReconvertType(
                "test",
                BasicType.FLOAT_TYPE,
                OscarTypeConverter.OSCAR_REAL,
                OscarTypeConverter.OSCAR_REAL);

        testReconvertType(
                "test",
                BasicType.DOUBLE_TYPE,
                OscarTypeConverter.OSCAR_DOUBLE,
                OscarTypeConverter.OSCAR_DOUBLE);
    }

    @Test
    public void testReconvertDecimal() {
        testReconvertType(
                "test",
                new DecimalType(0, 0),
                String.format(
                        "%s(%s,%s)",
                        OscarTypeConverter.OSCAR_DECIMAL,
                        OscarTypeConverter.DEFAULT_PRECISION,
                        OscarTypeConverter.DEFAULT_SCALE),
                OscarTypeConverter.OSCAR_DECIMAL);

        testReconvertType(
                "test",
                new DecimalType(10, 2),
                String.format("%s(%s,%s)", OscarTypeConverter.OSCAR_DECIMAL, 10, 2),
                OscarTypeConverter.OSCAR_DECIMAL);

        testReconvertType(
                "test",
                new DecimalType(1005, 7),
                String.format("%s(%s,%s)", OscarTypeConverter.OSCAR_DECIMAL, 38, 0),
                OscarTypeConverter.OSCAR_DECIMAL);

        testReconvertType(
                "test",
                new DecimalType(900, -7),
                String.format("%s(%s,%s)", OscarTypeConverter.OSCAR_DECIMAL, 38, 0),
                OscarTypeConverter.OSCAR_DECIMAL);
    }

    @Test
    public void testReconvertString() {
        testReconvertType(
                "test",
                BasicType.STRING_TYPE,
                OscarTypeConverter.OSCAR_TEXT,
                OscarTypeConverter.OSCAR_TEXT);
        testReconvertStringType(
                "test",
                BasicType.STRING_TYPE,
                OscarTypeConverter.OSCAR_TEXT,
                OscarTypeConverter.OSCAR_TEXT,
                -5L);
        testReconvertStringType(
                "test",
                BasicType.STRING_TYPE,
                OscarTypeConverter.OSCAR_TEXT,
                OscarTypeConverter.OSCAR_TEXT,
                50000L);
        testReconvertStringType(
                "test",
                BasicType.STRING_TYPE,
                String.format("%s(%s)", OscarTypeConverter.OSCAR_VARCHAR2, 7999L),
                OscarTypeConverter.OSCAR_VARCHAR2,
                7999L);
    }

    @Test
    public void testReconvertBytes() {
        testReconvertType(
                "test",
                PrimitiveByteArrayType.INSTANCE,
                OscarTypeConverter.OSCAR_BLOB,
                OscarTypeConverter.OSCAR_BLOB);
        testReconvertStringType(
                "test",
                PrimitiveByteArrayType.INSTANCE,
                OscarTypeConverter.OSCAR_BLOB,
                OscarTypeConverter.OSCAR_BLOB,
                -5L);
        testReconvertStringType(
                "test",
                PrimitiveByteArrayType.INSTANCE,
                OscarTypeConverter.OSCAR_BLOB,
                OscarTypeConverter.OSCAR_BLOB,
                50000L);
        testReconvertStringType(
                "test",
                PrimitiveByteArrayType.INSTANCE,
                String.format("%s(%s)", OscarTypeConverter.OSCAR_VARBINARY, 7999L),
                OscarTypeConverter.OSCAR_VARBINARY,
                7999L);
    }

    @Test
    public void testReconvertDatetime() {
        testReconvertType(
                "test",
                LocalTimeType.LOCAL_DATE_TYPE,
                OscarTypeConverter.OSCAR_DATE,
                OscarTypeConverter.OSCAR_DATE);

        testReconvertType(
                "test",
                LocalTimeType.LOCAL_TIME_TYPE,
                OscarTypeConverter.OSCAR_TIME,
                OscarTypeConverter.OSCAR_TIME);

        testReconvertDataTimeType(
                "test",
                LocalTimeType.LOCAL_TIME_TYPE,
                String.format("%s(%s)", OscarTypeConverter.OSCAR_TIME, 6),
                OscarTypeConverter.OSCAR_TIME,
                99);

        testReconvertDataTimeType(
                "test",
                LocalTimeType.LOCAL_TIME_TYPE,
                String.format("%s(%s)", OscarTypeConverter.OSCAR_TIME, 5),
                OscarTypeConverter.OSCAR_TIME,
                5);

        testReconvertType(
                "test",
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                OscarTypeConverter.OSCAR_TIMESTAMP,
                OscarTypeConverter.OSCAR_TIMESTAMP);

        testReconvertDataTimeType(
                "test",
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                String.format("%s(%s)", OscarTypeConverter.OSCAR_TIMESTAMP, 6),
                OscarTypeConverter.OSCAR_TIMESTAMP,
                99);

        testReconvertDataTimeType(
                "test",
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                String.format("%s(%s)", OscarTypeConverter.OSCAR_TIMESTAMP, 5),
                OscarTypeConverter.OSCAR_TIMESTAMP,
                5);
    }

    private void testReconvertType(
            String colName, SeaTunnelDataType<?> basicType, String colType, String colDataType) {
        testReconvertType(colName, basicType, colType, colDataType, null, null);
    }

    private void testReconvertDataTimeType(
            String colName,
            SeaTunnelDataType<?> basicType,
            String colType,
            String colDataType,
            Integer scale) {
        testReconvertType(colName, basicType, colType, colDataType, null, scale);
    }

    private void testReconvertStringType(
            String colName,
            SeaTunnelDataType<?> basicType,
            String colType,
            String colDataType,
            Long colLen) {
        testReconvertType(colName, basicType, colType, colDataType, colLen, null);
    }

    private void testReconvertType(
            String colName,
            SeaTunnelDataType<?> basicType,
            String colType,
            String colDataType,
            Long colLen,
            Integer scale) {
        Column column =
                PhysicalColumn.builder()
                        .name(colName)
                        .dataType(basicType)
                        .columnLength(colLen)
                        .scale(scale)
                        .build();
        BasicTypeDefine typeDefine = OscarTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(colType, typeDefine.getColumnType());
        Assertions.assertEquals(colDataType, typeDefine.getDataType());
    }
}
