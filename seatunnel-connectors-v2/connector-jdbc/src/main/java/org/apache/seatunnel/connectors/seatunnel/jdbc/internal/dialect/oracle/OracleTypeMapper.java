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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Slf4j
public class OracleTypeMapper implements JdbcDialectTypeMapper {

    // ============================data types=====================

    private static final String ORACLE_UNKNOWN = "UNKNOWN";

    // -------------------------number----------------------------
    private static final String ORACLE_BINARY_DOUBLE = "BINARY_DOUBLE";
    private static final String ORACLE_BINARY_FLOAT = "BINARY_FLOAT";
    private static final String ORACLE_NUMBER = "NUMBER";
    private static final String ORACLE_FLOAT = "FLOAT";
    private static final String ORACLE_REAL = "REAL";
    private static final String ORACLE_INTEGER = "INTEGER";

    // -------------------------string----------------------------
    private static final String ORACLE_CHAR = "CHAR";
    private static final String ORACLE_VARCHAR2 = "VARCHAR2";
    private static final String ORACLE_NCHAR = "NCHAR";
    private static final String ORACLE_NVARCHAR2 = "NVARCHAR2";
    private static final String ORACLE_LONG = "LONG";
    private static final String ORACLE_ROWID = "ROWID";
    private static final String ORACLE_CLOB = "CLOB";
    private static final String ORACLE_NCLOB = "NCLOB";

    // ------------------------------time-------------------------
    private static final String ORACLE_DATE = "DATE";
    private static final String ORACLE_TIMESTAMP = "TIMESTAMP";
    private static final String ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE = "TIMESTAMP WITH LOCAL TIME ZONE";

    // ------------------------------blob-------------------------
    private static final String ORACLE_BLOB = "BLOB";
    private static final String ORACLE_BFILE = "BFILE";
    private static final String ORACLE_RAW = "RAW";
    private static final String ORACLE_LONG_RAW = "LONG RAW";

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String oracleType = metadata.getColumnTypeName(colIndex).toUpperCase();
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (oracleType) {
            case ORACLE_INTEGER:
                return BasicType.INT_TYPE;
            case ORACLE_FLOAT:
                //The float type will be converted to DecimalType(10, -127),
                // which will lose precision in the spark engine
                return new DecimalType(38, 18);
            case ORACLE_NUMBER:
                if (scale == 0) {
                    if (precision <= 9) {
                        return BasicType.INT_TYPE;
                    }
                    if (precision <= 18) {
                        return BasicType.LONG_TYPE;
                    }
                }
                return new DecimalType(38, 18);
            case ORACLE_BINARY_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case ORACLE_BINARY_FLOAT:
            case ORACLE_REAL:
                return BasicType.FLOAT_TYPE;
            case ORACLE_CHAR:
            case ORACLE_NCHAR:
            case ORACLE_NVARCHAR2:
            case ORACLE_VARCHAR2:
            case ORACLE_LONG:
            case ORACLE_ROWID:
            case ORACLE_NCLOB:
            case ORACLE_CLOB:
                return BasicType.STRING_TYPE;
            case ORACLE_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case ORACLE_TIMESTAMP:
            case ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case ORACLE_BLOB:
            case ORACLE_RAW:
            case ORACLE_LONG_RAW:
            case ORACLE_BFILE:
                return PrimitiveByteArrayType.INSTANCE;
            //Doesn't support yet
            case ORACLE_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                    String.format(
                        "Doesn't support ORACLE type '%s' on column '%s'  yet.",
                        oracleType, jdbcColumnName));
        }
    }
}
