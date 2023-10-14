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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.gbase8a;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Slf4j
public class Gbase8aTypeMapper implements JdbcDialectTypeMapper {

    // ref http://www.gbase.cn/down/4419.html
    // ============================data types=====================
    private static final String GBASE8A_UNKNOWN = "UNKNOWN";

    // -------------------------number----------------------------
    private static final String GBASE8A_INT = "INT";
    private static final String GBASE8A_TINYINT = "TINYINT";
    private static final String GBASE8A_SMALLINT = "SMALLINT";
    private static final String GBASE8A_BIGINT = "BIGINT";
    private static final String GBASE8A_DECIMAL = "DECIMAL";
    private static final String GBASE8A_FLOAT = "FLOAT";
    private static final String GBASE8A_DOUBLE = "DOUBLE";

    // -------------------------string----------------------------
    private static final String GBASE8A_CHAR = "CHAR";
    private static final String GBASE8A_VARCHAR = "VARCHAR";

    // ------------------------------time-------------------------
    private static final String GBASE8A_DATE = "DATE";
    private static final String GBASE8A_TIME = "TIME";
    private static final String GBASE8A_TIMESTAMP = "TIMESTAMP";
    private static final String GBASE8A_DATETIME = "DATETIME";

    // ------------------------------blob-------------------------
    private static final String GBASE8A_BLOB = "BLOB";
    private static final String GBASE8A_TEXT = "TEXT";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String gbase8aType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (gbase8aType) {
            case GBASE8A_TINYINT:
                return BasicType.BYTE_TYPE;
            case GBASE8A_SMALLINT:
                return BasicType.SHORT_TYPE;
            case GBASE8A_INT:
                return BasicType.INT_TYPE;
            case GBASE8A_BIGINT:
                return BasicType.LONG_TYPE;
            case GBASE8A_DECIMAL:
                if (precision < 38) {
                    return new DecimalType(precision, scale);
                }
                return new DecimalType(38, 18);
            case GBASE8A_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case GBASE8A_FLOAT:
                return BasicType.FLOAT_TYPE;
            case GBASE8A_CHAR:
            case GBASE8A_VARCHAR:
                return BasicType.STRING_TYPE;
            case GBASE8A_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case GBASE8A_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case GBASE8A_TIMESTAMP:
            case GBASE8A_DATETIME:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case GBASE8A_BLOB:
            case GBASE8A_TEXT:
                return PrimitiveByteArrayType.INSTANCE;
                // Doesn't support yet
            case GBASE8A_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Doesn't support GBASE8A type '%s' on column '%s'  yet.",
                                gbase8aType, jdbcColumnName));
        }
    }
}
