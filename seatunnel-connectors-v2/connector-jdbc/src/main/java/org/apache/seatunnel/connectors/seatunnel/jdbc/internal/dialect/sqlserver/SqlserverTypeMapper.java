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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver;

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
public class SqlserverTypeMapper implements JdbcDialectTypeMapper {

    // ============================data types=====================

    private static final String SQLSERVER_UNKNOWN = "UNKNOWN";

    // -------------------------number----------------------------
    private static final String SQLSERVER_BIT = "BIT";
    private static final String SQLSERVER_TINYINT = "TINYINT";
    private static final String SQLSERVER_SMALLINT = "SMALLINT";
    private static final String SQLSERVER_INTEGER = "INTEGER";
    private static final String SQLSERVER_INT = "INT";
    private static final String SQLSERVER_BIGINT = "BIGINT";
    private static final String SQLSERVER_DECIMAL = "DECIMAL";
    private static final String SQLSERVER_FLOAT = "FLOAT";
    private static final String SQLSERVER_REAL = "REAL";
    private static final String SQLSERVER_NUMERIC = "NUMERIC";
    private static final String SQLSERVER_MONEY = "MONEY";
    private static final String SQLSERVER_SMALLMONEY = "SMALLMONEY";
    // -------------------------string----------------------------
    private static final String SQLSERVER_CHAR = "CHAR";
    private static final String SQLSERVER_VARCHAR = "VARCHAR";
    private static final String SQLSERVER_NTEXT = "NTEXT";
    private static final String SQLSERVER_NCHAR = "NCHAR";
    private static final String SQLSERVER_NVARCHAR = "NVARCHAR";
    private static final String SQLSERVER_TEXT = "TEXT";

    // ------------------------------time-------------------------
    private static final String SQLSERVER_DATE = "DATE";
    private static final String SQLSERVER_TIME = "TIME";
    private static final String SQLSERVER_DATETIME = "DATETIME";
    private static final String SQLSERVER_DATETIME2 = "DATETIME2";
    private static final String SQLSERVER_SMALLDATETIME = "SMALLDATETIME";
    private static final String SQLSERVER_DATETIMEOFFSET = "DATETIMEOFFSET";
    private static final String SQLSERVER_TIMESTAMP = "TIMESTAMP";

    // ------------------------------blob-------------------------
    private static final String SQLSERVER_BINARY = "BINARY";
    private static final String SQLSERVER_VARBINARY = "VARBINARY";
    private static final String SQLSERVER_IMAGE = "IMAGE";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String sqlServerType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (sqlServerType) {
            case SQLSERVER_BIT:
                return BasicType.BOOLEAN_TYPE;
            case SQLSERVER_TINYINT:
            case SQLSERVER_SMALLINT:
                return BasicType.SHORT_TYPE;
            case SQLSERVER_INTEGER:
            case SQLSERVER_INT:
                return BasicType.INT_TYPE;
            case SQLSERVER_BIGINT:
                return BasicType.LONG_TYPE;
            case SQLSERVER_DECIMAL:
            case SQLSERVER_NUMERIC:
            case SQLSERVER_MONEY:
            case SQLSERVER_SMALLMONEY:
                return new DecimalType(precision, scale);
            case SQLSERVER_REAL:
                return BasicType.FLOAT_TYPE;
            case SQLSERVER_FLOAT:
                return BasicType.DOUBLE_TYPE;
            case SQLSERVER_CHAR:
            case SQLSERVER_NCHAR:
            case SQLSERVER_VARCHAR:
            case SQLSERVER_NTEXT:
            case SQLSERVER_NVARCHAR:
            case SQLSERVER_TEXT:
                return BasicType.STRING_TYPE;
            case SQLSERVER_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case SQLSERVER_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case SQLSERVER_DATETIME:
            case SQLSERVER_DATETIME2:
            case SQLSERVER_SMALLDATETIME:
            case SQLSERVER_DATETIMEOFFSET:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case SQLSERVER_TIMESTAMP:
            case SQLSERVER_BINARY:
            case SQLSERVER_VARBINARY:
            case SQLSERVER_IMAGE:
                return PrimitiveByteArrayType.INSTANCE;
                // Doesn't support yet
            case SQLSERVER_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Doesn't support SQLSERVER type '%s' on column '%s'  yet.",
                                sqlServerType, jdbcColumnName));
        }
    }
}
