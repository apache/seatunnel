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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.utils;

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

public class SqlServerTypeUtilsTest {
    // ============================data types=====================

    // -------------------------string----------------------------
    private static final String SQLSERVER_CHAR = "CHAR";
    private static final String SQLSERVER_VARCHAR = "VARCHAR";
    private static final String SQLSERVER_NCHAR = "NCHAR";
    private static final String SQLSERVER_NVARCHAR = "NVARCHAR";
    private static final String SQLSERVER_STRUCT = "STRUCT";
    private static final String SQLSERVER_CLOB = "CLOB";
    private static final String SQLSERVER_LONGVARCHAR = "LONGVARCHAR";
    private static final String SQLSERVER_LONGNVARCHAR = "LONGNVARCHAR";
    private static final String SQLSERVER_TEXT = "TEXT";
    private static final String SQLSERVER_NTEXT = "NTEXT";
    private static final String SQLSERVER_XML = "XML";

    // ------------------------------blob-------------------------
    private static final String SQLSERVER_BLOB = "BLOB";
    private static final String SQLSERVER_VARBINARY = "VARBINARY";

    // ------------------------------number-------------------------
    private static final String SQLSERVER_INTEGER = "INT";
    private static final String SQLSERVER_SMALLINT = "SMALLINT";
    private static final String SQLSERVER_TINYINT = "TINYINT";
    private static final String SQLSERVER_BIGINT = "BIGINT";
    private static final String SQLSERVER_FLOAT = "FLOAT";
    private static final String SQLSERVER_REAL = "REAL";
    private static final String SQLSERVER_DOUBLE = "DOUBLE";
    private static final String SQLSERVER_NUMERIC = "NUMERIC";
    private static final String SQLSERVER_DECIMAL = "DECIMAL";
    private static final String SQLSERVER_SMALLMONEY = "SMALLMONEY";
    private static final String SQLSERVER_MONEY = "MONEY";

    // ------------------------------date-------------------------
    private static final String SQLSERVER_TIMESTAMP = "TIMESTAMP";
    private static final String SQLSERVER_DATE = "DATE";
    private static final String SQLSERVER_TIME = "TIME";
    private static final String SQLSERVER_DATETIMEOFFSET = "DATETIMEOFFSET";
    private static final String SQLSERVER_DATETIME2 = "DATETIME2";
    private static final String SQLSERVER_DATETIME = "DATETIME";
    private static final String SQLSERVER_SMALLDATETIME = "SMALLDATETIME";

    // ------------------------------bool-------------------------
    private static final String SQLSERVER_BIT = "BIT";

    @Test
    public void testConvertToSeaTunnelTypeError() {
        Column mockCol = Column.editor().type("unknown").name("c_mock").create();

        SeaTunnelRuntimeException expected =
                CommonError.convertToSeaTunnelTypeError(
                        "SQLServer-CDC", mockCol.typeName(), mockCol.name());
        SeaTunnelRuntimeException actual =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> SqlServerTypeUtils.convertFromColumn(mockCol));

        Assertions.assertEquals(expected.getMessage(), actual.getMessage());
    }

    @Test
    public void testString() {
        testConvertBasicType(SQLSERVER_CHAR, BasicType.STRING_TYPE);
        testConvertBasicType(SQLSERVER_VARCHAR, BasicType.STRING_TYPE);
        testConvertBasicType(SQLSERVER_NCHAR, BasicType.STRING_TYPE);
        testConvertBasicType(SQLSERVER_NVARCHAR, BasicType.STRING_TYPE);
        testConvertBasicType(SQLSERVER_STRUCT, BasicType.STRING_TYPE);
        testConvertBasicType(SQLSERVER_CLOB, BasicType.STRING_TYPE);
        testConvertBasicType(SQLSERVER_LONGVARCHAR, BasicType.STRING_TYPE);
        testConvertBasicType(SQLSERVER_LONGNVARCHAR, BasicType.STRING_TYPE);
        testConvertBasicType(SQLSERVER_TEXT, BasicType.STRING_TYPE);
        testConvertBasicType(SQLSERVER_NTEXT, BasicType.STRING_TYPE);
        testConvertBasicType(SQLSERVER_XML, BasicType.STRING_TYPE);
    }

    @Test
    public void testBlob() {
        testConvertBasicType(SQLSERVER_BLOB, PrimitiveByteArrayType.INSTANCE);
        testConvertBasicType(SQLSERVER_VARBINARY, PrimitiveByteArrayType.INSTANCE);
    }

    @Test
    public void testNumber() {
        testConvertBasicType(SQLSERVER_INTEGER, BasicType.INT_TYPE);
        testConvertBasicType(SQLSERVER_SMALLINT, BasicType.SHORT_TYPE);
        testConvertBasicType(SQLSERVER_TINYINT, BasicType.SHORT_TYPE);
        testConvertBasicType(SQLSERVER_BIGINT, BasicType.LONG_TYPE);
        testConvertBasicType(SQLSERVER_REAL, BasicType.FLOAT_TYPE);
        testConvertBasicType(SQLSERVER_DOUBLE, BasicType.DOUBLE_TYPE);
        testConvertBasicType(SQLSERVER_FLOAT, BasicType.DOUBLE_TYPE);
        testConvertBasicType(SQLSERVER_NUMERIC, 30, 10, new DecimalType(30, 10));
        testConvertBasicType(SQLSERVER_DECIMAL, 30, 10, new DecimalType(30, 10));
        testConvertBasicType(SQLSERVER_SMALLMONEY, 30, 10, new DecimalType(30, 10));
        testConvertBasicType(SQLSERVER_MONEY, 30, 10, new DecimalType(30, 10));
    }

    @Test
    public void testDate() {
        testConvertBasicType(SQLSERVER_TIMESTAMP, LocalTimeType.LOCAL_DATE_TIME_TYPE);
        testConvertBasicType(SQLSERVER_DATETIMEOFFSET, LocalTimeType.LOCAL_DATE_TIME_TYPE);
        testConvertBasicType(SQLSERVER_DATETIME2, LocalTimeType.LOCAL_DATE_TIME_TYPE);
        testConvertBasicType(SQLSERVER_DATETIME, LocalTimeType.LOCAL_DATE_TIME_TYPE);
        testConvertBasicType(SQLSERVER_SMALLDATETIME, LocalTimeType.LOCAL_DATE_TIME_TYPE);
        testConvertBasicType(SQLSERVER_DATE, LocalTimeType.LOCAL_DATE_TYPE);
        testConvertBasicType(SQLSERVER_TIME, LocalTimeType.LOCAL_TIME_TYPE);
    }

    @Test
    public void testBool() {
        testConvertBasicType(SQLSERVER_BIT, BasicType.BOOLEAN_TYPE);
    }

    public void testConvertBasicType(String sqlServerType, SeaTunnelDataType<?> expectedType) {
        Column mockCol = Column.editor().type(sqlServerType).name("c_mock").create();

        SeaTunnelDataType<?> internalType = SqlServerTypeUtils.convertFromColumn(mockCol);
        Assertions.assertEquals(expectedType, internalType);
    }

    public void testConvertBasicType(
            String sqlServerType, int length, int scale, SeaTunnelDataType<?> expectedType) {
        Column mockCol =
                Column.editor()
                        .type(sqlServerType)
                        .length(length)
                        .scale(scale)
                        .name("c_mock")
                        .create();

        SeaTunnelDataType<?> internalType = SqlServerTypeUtils.convertFromColumn(mockCol);
        Assertions.assertEquals(expectedType, internalType);
    }
}
