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
    private static final String SQLSERVER_BIGINT = "BIGINT";
    private static final String SQLSERVER_DECIMAL = "DECIMAL";
    private static final String SQLSERVER_INT = "INT";
    private static final String SQLSERVER_FLOAT = "FLOAT";
    private static final String SQLSERVER_NUMERIC = "NUMERIC";

    // -------------------------string----------------------------
    private static final String SQLSERVER_BINARY = "BINARY";
    private static final String SQLSERVER_CHAR = "CHAR";
    private static final String SQLSERVER_NTEXT = "NTEXT";
    private static final String SQLSERVER_NVARCHAR = "NVARCHAR";
    private static final String SQLSERVER_TEXT = "TEXT";
    private static final String SQLSERVER_VARBINARY = "VARBINARY";
    private static final String SQLSERVER_XML = "XML";

    // ------------------------------time-------------------------
    private static final String SQLSERVER_DATE = "DATE";
    private static final String SQLSERVER_TIME = "TIME";
    private static final String SQLSERVER_TIMESTAMP = "TIMESTAMP";
    private static final String SQLSERVER_TIME_WITHOUT_TIME_ZONE = "TIME WITHOUT TIME ZONE";
    private static final String SQLSERVER_TIMESTAMP_WITHOUT_TIME_ZONE = "TIMESTAMP WITHOUT TIME ZONE";

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String sqlServerType = metadata.getColumnTypeName(colIndex).toUpperCase();
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (sqlServerType) {
            case SQLSERVER_BIT:
                return BasicType.BOOLEAN_TYPE;
            case SQLSERVER_FLOAT:
            case SQLSERVER_BINARY_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case SQLSERVER_BINARY_FLOAT:
                return BasicType.FLOAT_TYPE;
            case SQLSERVER_CHAR:
            case SQLSERVER_NCHAR:
            case SQLSERVER_NVARCHAR2:
            case SQLSERVER_VARCHAR2:
            case SQLSERVER_LONG:
            case SQLSERVER_ROWID:
            case SQLSERVER_NCLOB:
            case SQLSERVER_CLOB:
                return BasicType.STRING_TYPE;
            case SQLSERVER_DATE:
            case SQLSERVER_TIMESTAMP:
            case SQLSERVER_TIME_WITHOUT_TIME_ZONE:
            case SQLSERVER_TIMESTAMP_WITHOUT_TIME_ZONE:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case SQLSERVER_BLOB:
            case SQLSERVER_RAW:
            case SQLSERVER_LONG_RAW:
            case SQLSERVER_BFILE:
                return PrimitiveByteArrayType.INSTANCE;
            //Doesn't support yet
            case SQLSERVER_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                    String.format(
                        "Doesn't support SQLSERVER type '%s' on column '%s'  yet.",
                        sqlServerType, jdbcColumnName));
        }
    }
}
