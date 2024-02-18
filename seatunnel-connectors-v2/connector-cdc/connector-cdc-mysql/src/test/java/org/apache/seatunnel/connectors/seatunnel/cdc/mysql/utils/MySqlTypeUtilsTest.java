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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.Column;

public class MySqlTypeUtilsTest {
    // ============================data types=====================

    private static final String MYSQL_UNKNOWN = "UNKNOWN";
    private static final String MYSQL_BIT = "BIT";

    // -------------------------number----------------------------
    private static final String MYSQL_TINYINT = "TINYINT";
    private static final String MYSQL_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String MYSQL_SMALLINT = "SMALLINT";
    private static final String MYSQL_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String MYSQL_MEDIUMINT = "MEDIUMINT";
    private static final String MYSQL_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String MYSQL_INT = "INT";
    private static final String MYSQL_INT_UNSIGNED = "INT UNSIGNED";
    private static final String MYSQL_INTEGER = "INTEGER";
    private static final String MYSQL_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String MYSQL_BIGINT = "BIGINT";
    private static final String MYSQL_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String MYSQL_DECIMAL = "DECIMAL";
    private static final String MYSQL_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String MYSQL_NUMERIC = "NUMERIC";
    private static final String MYSQL_NUMERIC_UNSIGNED = "NUMERIC UNSIGNED";
    private static final String MYSQL_FLOAT = "FLOAT";
    private static final String MYSQL_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String MYSQL_DOUBLE = "DOUBLE";
    private static final String MYSQL_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    private static final String MYSQL_REAL = "REAL";
    private static final String MYSQL_REAL_UNSIGNED = "REAL UNSIGNED";

    // -------------------------string----------------------------
    private static final String MYSQL_CHAR = "CHAR";
    private static final String MYSQL_VARCHAR = "VARCHAR";
    private static final String MYSQL_TINYTEXT = "TINYTEXT";
    private static final String MYSQL_MEDIUMTEXT = "MEDIUMTEXT";
    private static final String MYSQL_TEXT = "TEXT";
    private static final String MYSQL_LONGTEXT = "LONGTEXT";
    private static final String MYSQL_JSON = "JSON";
    private static final String MYSQL_ENUM = "ENUM";

    // ------------------------------time-------------------------
    private static final String MYSQL_DATE = "DATE";
    private static final String MYSQL_DATETIME = "DATETIME";
    private static final String MYSQL_TIME = "TIME";
    private static final String MYSQL_TIMESTAMP = "TIMESTAMP";
    private static final String MYSQL_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    private static final String MYSQL_TINYBLOB = "TINYBLOB";
    private static final String MYSQL_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String MYSQL_BLOB = "BLOB";
    private static final String MYSQL_LONGBLOB = "LONGBLOB";
    private static final String MYSQL_BINARY = "BINARY";
    private static final String MYSQL_VARBINARY = "VARBINARY";
    private static final String MYSQL_GEOMETRY = "GEOMETRY";
    public static final String IDENTIFIER = "MySQL-CDC";

    @Test
    public void testGeometry() {
        Column mockCol = Column.editor().type(MYSQL_GEOMETRY).name("c_mock").create();

        SeaTunnelRuntimeException expected =
                CommonError.convertToSeaTunnelTypeError(
                        IDENTIFIER, mockCol.typeName(), mockCol.name());
        SeaTunnelRuntimeException actual =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> MySqlTypeUtils.convertFromColumn(mockCol));

        Assertions.assertEquals(expected.getMessage(), actual.getMessage());
    }

    @Test
    public void testUnknown() {
        Column mockCol = Column.editor().type(MYSQL_UNKNOWN).name("c_mock").create();

        SeaTunnelRuntimeException expected =
                CommonError.convertToSeaTunnelTypeError(
                        IDENTIFIER, mockCol.typeName(), mockCol.name());
        SeaTunnelRuntimeException actual =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> MySqlTypeUtils.convertFromColumn(mockCol));

        Assertions.assertEquals(expected.getMessage(), actual.getMessage());
    }

    @Test
    public void testBit() {
        testConvertBasicType(MYSQL_BIT, BasicType.BOOLEAN_TYPE);
    }

    @Test
    public void testNumber() {
        testConvertBasicType(MYSQL_TINYINT, BasicType.INT_TYPE);
        testConvertBasicType(MYSQL_TINYINT, 1, BasicType.BOOLEAN_TYPE);
        testConvertBasicType(MYSQL_TINYINT_UNSIGNED, BasicType.SHORT_TYPE);
        testConvertBasicType(MYSQL_SMALLINT, BasicType.SHORT_TYPE);
        testConvertBasicType(MYSQL_SMALLINT_UNSIGNED, BasicType.INT_TYPE);
        testConvertBasicType(MYSQL_MEDIUMINT, BasicType.INT_TYPE);
        testConvertBasicType(MYSQL_MEDIUMINT_UNSIGNED, BasicType.INT_TYPE);
        testConvertBasicType(MYSQL_INT, BasicType.INT_TYPE);
        testConvertBasicType(MYSQL_INTEGER, BasicType.INT_TYPE);
        testConvertBasicType(MYSQL_YEAR, BasicType.INT_TYPE);
        testConvertBasicType(MYSQL_INT_UNSIGNED, BasicType.LONG_TYPE);
        testConvertBasicType(MYSQL_INTEGER_UNSIGNED, BasicType.LONG_TYPE);
        testConvertBasicType(MYSQL_BIGINT, BasicType.LONG_TYPE);
        testConvertBasicType(MYSQL_BIGINT_UNSIGNED, new DecimalType(20, 0));
        testConvertBasicType(MYSQL_DECIMAL, 30, 10, new DecimalType(30, 10));
        testConvertBasicType(MYSQL_DECIMAL_UNSIGNED, 30, 10, new DecimalType(30, 10));
        testConvertBasicType(MYSQL_NUMERIC, 30, 10, new DecimalType(30, 10));
        testConvertBasicType(MYSQL_NUMERIC_UNSIGNED, 30, 10, new DecimalType(30, 10));
        testConvertBasicType(MYSQL_FLOAT, BasicType.FLOAT_TYPE);
        testConvertBasicType(MYSQL_FLOAT_UNSIGNED, BasicType.FLOAT_TYPE);
        testConvertBasicType(MYSQL_DOUBLE, BasicType.DOUBLE_TYPE);
        testConvertBasicType(MYSQL_REAL, BasicType.DOUBLE_TYPE);
        testConvertBasicType(MYSQL_DOUBLE_UNSIGNED, BasicType.DOUBLE_TYPE);
        testConvertBasicType(MYSQL_REAL_UNSIGNED, BasicType.DOUBLE_TYPE);
    }

    @Test
    public void testString() {
        testConvertBasicType(MYSQL_CHAR, BasicType.STRING_TYPE);
        testConvertBasicType(MYSQL_TINYTEXT, BasicType.STRING_TYPE);
        testConvertBasicType(MYSQL_MEDIUMTEXT, BasicType.STRING_TYPE);
        testConvertBasicType(MYSQL_TEXT, BasicType.STRING_TYPE);
        testConvertBasicType(MYSQL_VARCHAR, BasicType.STRING_TYPE);
        testConvertBasicType(MYSQL_JSON, BasicType.STRING_TYPE);
        testConvertBasicType(MYSQL_ENUM, BasicType.STRING_TYPE);
        testConvertBasicType(MYSQL_LONGTEXT, BasicType.STRING_TYPE);
    }

    @Test
    public void testTime() {
        testConvertBasicType(MYSQL_DATE, LocalTimeType.LOCAL_DATE_TYPE);
        testConvertBasicType(MYSQL_TIME, LocalTimeType.LOCAL_TIME_TYPE);
        testConvertBasicType(MYSQL_DATETIME, LocalTimeType.LOCAL_DATE_TIME_TYPE);
        testConvertBasicType(MYSQL_TIMESTAMP, LocalTimeType.LOCAL_DATE_TIME_TYPE);
    }

    @Test
    public void testBlob() {
        testConvertBasicType(MYSQL_TINYBLOB, PrimitiveByteArrayType.INSTANCE);
        testConvertBasicType(MYSQL_MEDIUMBLOB, PrimitiveByteArrayType.INSTANCE);
        testConvertBasicType(MYSQL_BLOB, PrimitiveByteArrayType.INSTANCE);
        testConvertBasicType(MYSQL_LONGBLOB, PrimitiveByteArrayType.INSTANCE);
        testConvertBasicType(MYSQL_VARBINARY, PrimitiveByteArrayType.INSTANCE);
        testConvertBasicType(MYSQL_BINARY, PrimitiveByteArrayType.INSTANCE);
    }

    public void testConvertBasicType(String mysqlType, SeaTunnelDataType<?> expectedType) {
        Column mockCol = Column.editor().type(mysqlType).name("c_mock").create();

        SeaTunnelDataType<?> internalType = MySqlTypeUtils.convertFromColumn(mockCol);
        Assertions.assertEquals(expectedType, internalType);
    }

    public void testConvertBasicType(
            String mysqlType, int length, SeaTunnelDataType<?> expectedType) {
        Column mockCol = Column.editor().type(mysqlType).name("c_mock").length(length).create();

        SeaTunnelDataType<?> internalType = MySqlTypeUtils.convertFromColumn(mockCol);
        Assertions.assertEquals(expectedType, internalType);
    }

    public void testConvertBasicType(
            String mysqlType, int length, int scale, SeaTunnelDataType<?> expectedType) {
        Column mockCol =
                Column.editor().type(mysqlType).name("c_mock").length(length).scale(scale).create();

        SeaTunnelDataType<?> internalType = MySqlTypeUtils.convertFromColumn(mockCol);
        Assertions.assertEquals(expectedType, internalType);
    }
}
