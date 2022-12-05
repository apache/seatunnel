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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.redshift;

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
public class RedshiftTypeMapper implements JdbcDialectTypeMapper {

    /* ============================ data types ===================== */
    private static final String REDSHIFT_SMALLINT = "SMALLINT";
    private static final String REDSHIFT_INT2 = "INT2";
    private static final String REDSHIFT_INTEGER = "INTEGER";
    private static final String REDSHIFT_INT = "INT";
    private static final String REDSHIFT_INT4 = "INT4";
    private static final String REDSHIFT_BIGINT = "BIGINT";
    private static final String REDSHIFT_INT8 = "INT8";

    private static final String REDSHIFT_DECIMAL = "DECIMAL";
    private static final String REDSHIFT_NUMERIC = "NUMERIC";
    private static final String REDSHIFT_REAL = "REAL";
    private static final String REDSHIFT_FLOAT4 = "FLOAT4";
    private static final String REDSHIFT_DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String REDSHIFT_FLOAT8 = "FLOAT8";
    private static final String REDSHIFT_FLOAT = "FLOAT";

    private static final String REDSHIFT_BOOLEAN = "BOOLEAN";
    private static final String REDSHIFT_BOOL = "BOOL";

    private static final String REDSHIFT_CHAR = "CHAR";
    private static final String REDSHIFT_CHARACTER = "CHARACTER";
    private static final String REDSHIFT_NCHAR = "NCHAR";
    private static final String REDSHIFT_BPCHAR = "BPCHAR";

    private static final String REDSHIFT_VARCHAR = "VARCHAR";
    private static final String REDSHIFT_CHARACTER_VARYING = "CHARACTER VARYING";
    private static final String REDSHIFT_NVARCHAR = "NVARCHAR";
    private static final String REDSHIFT_TEXT = "TEXT";

    private static final String REDSHIFT_DATE = "DATE";
    /*FIXME*/

    private static final String REDSHIFT_GEOMETRY = "GEOMETRY";
    private static final String REDSHIFT_OID = "OID";
    private static final String REDSHIFT_SUPER = "SUPER";

    private static final String REDSHIFT_TIME = "TIME";
    private static final String REDSHIFT_TIME_WITH_TIME_ZONE = "TIME WITH TIME ZONE";

    private static final String REDSHIFT_TIMETZ = "TIMETZ";
    private static final String REDSHIFT_TIMESTAMP = "TIMESTAMP";
    private static final String REDSHIFT_TIMESTAMP_WITH_OUT_TIME_ZONE = "TIMESTAMP WITHOUT TIME ZONE";

    private static final String REDSHIFT_TIMESTAMPTZ = "TIMESTAMPTZ";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String redshiftType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (redshiftType) {
            case REDSHIFT_SMALLINT:
            case REDSHIFT_INT2:
                return BasicType.SHORT_TYPE;
            case REDSHIFT_INTEGER:
            case REDSHIFT_INT:
            case REDSHIFT_INT4:
                return BasicType.INT_TYPE;
            case REDSHIFT_BIGINT:
            case REDSHIFT_INT8:
            case REDSHIFT_OID:
                return BasicType.LONG_TYPE;
            case REDSHIFT_DECIMAL:
            case REDSHIFT_NUMERIC:
                return new DecimalType(precision, scale);
            case REDSHIFT_REAL:
            case REDSHIFT_FLOAT4:
                return BasicType.FLOAT_TYPE;
            case REDSHIFT_DOUBLE_PRECISION:
            case REDSHIFT_FLOAT8:
            case REDSHIFT_FLOAT:
                return BasicType.DOUBLE_TYPE;
            case REDSHIFT_BOOLEAN:
            case REDSHIFT_BOOL:
                return BasicType.BOOLEAN_TYPE;
            case REDSHIFT_CHAR:
            case REDSHIFT_CHARACTER:
            case REDSHIFT_NCHAR:
            case REDSHIFT_BPCHAR:
            case REDSHIFT_VARCHAR:
            case REDSHIFT_CHARACTER_VARYING:
            case REDSHIFT_NVARCHAR:
            case REDSHIFT_TEXT:
            case REDSHIFT_SUPER:
                return BasicType.STRING_TYPE;
            case REDSHIFT_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case REDSHIFT_GEOMETRY:
                return PrimitiveByteArrayType.INSTANCE;
            case REDSHIFT_TIME:
            case REDSHIFT_TIME_WITH_TIME_ZONE:
            case REDSHIFT_TIMETZ:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case REDSHIFT_TIMESTAMP:
            case REDSHIFT_TIMESTAMP_WITH_OUT_TIME_ZONE:
            case REDSHIFT_TIMESTAMPTZ:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                    String.format(
                        "Doesn't support REDSHIFT type '%s' on column '%s'  yet.",
                        redshiftType, jdbcColumnName));
        }
    }
}
