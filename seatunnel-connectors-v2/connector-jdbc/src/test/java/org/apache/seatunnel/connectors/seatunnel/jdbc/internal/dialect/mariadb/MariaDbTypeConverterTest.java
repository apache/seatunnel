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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mariadb;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MariaDbTypeConverterTest {

    private final MariaDbTypeConverter converter = MariaDbTypeConverter.DEFAULT_INSTANCE;

    @Test
    public void testNumericTypes() {
        // TINYINT(1) with intTypeNarrowing = true (default)
        Column tinyint1 =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_tinyint1")
                                .columnType("tinyint(1)")
                                .dataType("tinyint")
                                .build());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, tinyint1.getDataType());

        // TINYINT(1) with intTypeNarrowing = false
        MariaDbTypeConverter noNarrowing = new MariaDbTypeConverter(false);
        Column tinyint1Byte =
                noNarrowing.convert(
                        BasicTypeDefine.builder()
                                .name("c_tinyint1")
                                .columnType("tinyint(1)")
                                .dataType("tinyint")
                                .build());
        Assertions.assertEquals(BasicType.BYTE_TYPE, tinyint1Byte.getDataType());

        // TINYINT
        Column tinyint =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_tinyint")
                                .columnType("tinyint(4)")
                                .dataType("tinyint")
                                .build());
        Assertions.assertEquals(BasicType.BYTE_TYPE, tinyint.getDataType());

        // TINYINT UNSIGNED
        Column tinyintUnsigned =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_tinyint_unsigned")
                                .columnType("tinyint(3) unsigned")
                                .dataType("tinyint unsigned")
                                .build());
        Assertions.assertEquals(BasicType.SHORT_TYPE, tinyintUnsigned.getDataType());

        // SMALLINT
        Column smallint =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_smallint")
                                .columnType("smallint")
                                .dataType("smallint")
                                .build());
        Assertions.assertEquals(BasicType.SHORT_TYPE, smallint.getDataType());

        // SMALLINT UNSIGNED
        Column smallintUnsigned =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_smallint_unsigned")
                                .columnType("smallint unsigned")
                                .dataType("smallint unsigned")
                                .build());
        Assertions.assertEquals(BasicType.INT_TYPE, smallintUnsigned.getDataType());

        // INT
        Column intCol =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_int")
                                .columnType("int")
                                .dataType("int")
                                .build());
        Assertions.assertEquals(BasicType.INT_TYPE, intCol.getDataType());

        // INT UNSIGNED
        Column intUnsigned =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_int_unsigned")
                                .columnType("int unsigned")
                                .dataType("int unsigned")
                                .build());
        Assertions.assertEquals(BasicType.LONG_TYPE, intUnsigned.getDataType());

        // BIGINT
        Column bigint =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_bigint")
                                .columnType("bigint")
                                .dataType("bigint")
                                .build());
        Assertions.assertEquals(BasicType.LONG_TYPE, bigint.getDataType());

        // BIGINT UNSIGNED
        Column bigintUnsigned =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_bigint_unsigned")
                                .columnType("bigint unsigned")
                                .dataType("bigint unsigned")
                                .build());
        Assertions.assertEquals(new DecimalType(20, 0), bigintUnsigned.getDataType());

        // FLOAT
        Column floatCol =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_float")
                                .columnType("float")
                                .dataType("float")
                                .build());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, floatCol.getDataType());

        // DOUBLE
        Column doubleCol =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_double")
                                .columnType("double")
                                .dataType("double")
                                .build());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, doubleCol.getDataType());

        // DECIMAL
        Column decimalCol =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_decimal")
                                .columnType("decimal(10,2)")
                                .dataType("decimal")
                                .precision(10L)
                                .scale(2)
                                .build());
        Assertions.assertEquals(new DecimalType(10, 2), decimalCol.getDataType());
    }

    @Test
    public void testStringAndBinaryAndSpecialTypes() {
        // VARCHAR
        Column varcharCol =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_varchar")
                                .columnType("varchar(255)")
                                .dataType("varchar")
                                .length(255L)
                                .build());
        Assertions.assertEquals(BasicType.STRING_TYPE, varcharCol.getDataType());

        // JSON
        Column jsonCol =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_json")
                                .columnType("json")
                                .dataType("json")
                                .build());
        Assertions.assertEquals(BasicType.STRING_TYPE, jsonCol.getDataType());

        // UUID
        Column uuidCol =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_uuid")
                                .columnType("uuid")
                                .dataType("uuid")
                                .build());
        Assertions.assertEquals(BasicType.STRING_TYPE, uuidCol.getDataType());

        // INET4 / INET6
        Column inetCol =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_inet")
                                .columnType("inet4")
                                .dataType("inet4")
                                .build());
        Assertions.assertEquals(BasicType.STRING_TYPE, inetCol.getDataType());

        // BLOB
        Column blobCol =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_blob")
                                .columnType("blob")
                                .dataType("blob")
                                .build());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, blobCol.getDataType());

        // BIT(1)
        Column bit1 =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_bit1")
                                .columnType("bit(1)")
                                .dataType("bit")
                                .length(1L)
                                .build());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, bit1.getDataType());

        // BIT(8)
        Column bit8 =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_bit8")
                                .columnType("bit(8)")
                                .dataType("bit")
                                .length(8L)
                                .build());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, bit8.getDataType());
    }

    @Test
    public void testTimeTypes() {
        // DATE
        Column dateCol =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_date")
                                .columnType("date")
                                .dataType("date")
                                .build());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, dateCol.getDataType());

        // TIME
        Column timeCol =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_time")
                                .columnType("time")
                                .dataType("time")
                                .build());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, timeCol.getDataType());

        // DATETIME
        Column datetimeCol =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_datetime")
                                .columnType("datetime")
                                .dataType("datetime")
                                .build());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, datetimeCol.getDataType());

        // TIMESTAMP
        Column timestampCol =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("c_timestamp")
                                .columnType("timestamp")
                                .dataType("timestamp")
                                .build());
        Assertions.assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, timestampCol.getDataType());
    }

    @Test
    public void testReconvert() {
        BasicTypeDefine intDefine =
                converter.reconvert(
                        PhysicalColumn.builder().name("id").dataType(BasicType.INT_TYPE).build());
        Assertions.assertEquals("INT", intDefine.getDataType());

        BasicTypeDefine boolDefine =
                converter.reconvert(
                        PhysicalColumn.builder()
                                .name("flag")
                                .dataType(BasicType.BOOLEAN_TYPE)
                                .build());
        Assertions.assertEquals("TINYINT", boolDefine.getDataType());
        Assertions.assertEquals("TINYINT(1)", boolDefine.getColumnType());

        BasicTypeDefine stringDefine =
                converter.reconvert(
                        PhysicalColumn.builder()
                                .name("str")
                                .dataType(BasicType.STRING_TYPE)
                                .columnLength(50L)
                                .build());
        Assertions.assertEquals("VARCHAR", stringDefine.getDataType());
        Assertions.assertEquals("VARCHAR(50)", stringDefine.getColumnType());
    }
}
