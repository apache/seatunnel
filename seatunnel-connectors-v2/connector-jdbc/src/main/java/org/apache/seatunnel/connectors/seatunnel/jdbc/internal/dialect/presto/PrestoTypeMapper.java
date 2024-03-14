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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.presto;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class PrestoTypeMapper implements JdbcDialectTypeMapper {
    // ============================data types=====================

    private static final String PRESTO_BOOLEAN = "BOOLEAN";

    // -------------------------Structural----------------------------
    private static final String PRESTO_ARRAY = "ARRAY";
    private static final String PRESTO_MAP = "MAP";
    private static final String PRESTO_ROW = "ROW";

    // -------------------------number----------------------------
    private static final String PRESTO_TINYINT = "TINYINT";
    private static final String PRESTO_SMALLINT = "SMALLINT";
    private static final String PRESTO_INTEGER = "INTEGER";
    private static final String PRESTO_BIGINT = "BIGINT";
    private static final String PRESTO_DECIMAL = "DECIMAL";
    private static final String PRESTO_REAL = "REAL";
    private static final String PRESTO_DOUBLE = "DOUBLE";

    // -------------------------string----------------------------
    private static final String PRESTO_CHAR = "CHAR";
    private static final String PRESTO_VARCHAR = "VARCHAR";
    private static final String PRESTO_JSON = "JSON";

    // ------------------------------time-------------------------
    private static final String PRESTO_DATE = "DATE";
    private static final String PRESTO_TIME = "TIME";
    private static final String PRESTO_TIMESTAMP = "TIMESTAMP";

    // ------------------------------blob-------------------------
    private static final String PRESTO_BINARY = "BINARY";
    private static final String PRESTO_VARBINARY = "VARBINARY";

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnType = metadata.getColumnTypeName(colIndex).toUpperCase();
        // VARCHAR(x)      --->      VARCHAR
        if (columnType.indexOf("(") > -1) {
            columnType = columnType.split("\\(")[0];
        }
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (columnType) {
            case PRESTO_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case PRESTO_TINYINT:
                return BasicType.BYTE_TYPE;
            case PRESTO_INTEGER:
                return BasicType.INT_TYPE;
            case PRESTO_SMALLINT:
                return BasicType.SHORT_TYPE;
            case PRESTO_BIGINT:
                return BasicType.LONG_TYPE;
            case PRESTO_DECIMAL:
                return new DecimalType(precision, scale);
            case PRESTO_REAL:
                return BasicType.FLOAT_TYPE;
            case PRESTO_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case PRESTO_CHAR:
            case PRESTO_VARCHAR:
            case PRESTO_JSON:
                return BasicType.STRING_TYPE;
            case PRESTO_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case PRESTO_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case PRESTO_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case PRESTO_VARBINARY:
            case PRESTO_BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            //Doesn't support yet
            case PRESTO_MAP:
            case PRESTO_ARRAY:
            case PRESTO_ROW:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(CommonErrorCode.UNSUPPORTED_OPERATION,
                    String.format(
                        "Doesn't support Presto type '%s' on column '%s'  yet.",
                        columnType, jdbcColumnName));
        }
    }
}
