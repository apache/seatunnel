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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlite;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Slf4j
public class SqliteTypeMapper implements JdbcDialectTypeMapper {

    // ============================data types=====================

    private static final String SQLITE_UNKNOWN = "UNKNOWN";
    private static final String SQLITE_BIT = "BIT";
    private static final String SQLITE_BOOLEAN = "BOOLEAN";

    // -------------------------integer----------------------------
    private static final String SQLITE_TINYINT = "TINYINT";
    private static final String SQLITE_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String SQLITE_SMALLINT = "SMALLINT";
    private static final String SQLITE_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String SQLITE_MEDIUMINT = "MEDIUMINT";
    private static final String SQLITE_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String SQLITE_INT = "INT";
    private static final String SQLITE_INT_UNSIGNED = "INT UNSIGNED";
    private static final String SQLITE_INTEGER = "INTEGER";
    private static final String SQLITE_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String SQLITE_BIGINT = "BIGINT";
    private static final String SQLITE_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String SQLITE_DECIMAL = "DECIMAL";
    private static final String SQLITE_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String SQLITE_FLOAT = "FLOAT";
    private static final String SQLITE_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String SQLITE_DOUBLE = "DOUBLE";
    private static final String SQLITE_DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String SQLITE_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    private static final String SQLITE_NUMERIC = "NUMERIC";
    private static final String SQLITE_REAL = "REAL";

    // -------------------------text----------------------------
    private static final String SQLITE_CHAR = "CHAR";
    private static final String SQLITE_CHARACTER = "CHARACTER";
    private static final String SQLITE_VARYING_CHARACTER = "VARYING_CHARACTER";
    private static final String SQLITE_NATIVE_CHARACTER = "NATIVE_CHARACTER";
    private static final String SQLITE_NCHAR = "NCHAR";
    private static final String SQLITE_VARCHAR = "VARCHAR";
    private static final String SQLITE_LONGVARCHAR = "LONGVARCHAR";
    private static final String SQLITE_LONGNVARCHAR = "LONGNVARCHAR";
    private static final String SQLITE_NVARCHAR = "NVARCHAR";
    private static final String SQLITE_TINYTEXT = "TINYTEXT";
    private static final String SQLITE_MEDIUMTEXT = "MEDIUMTEXT";
    private static final String SQLITE_TEXT = "TEXT";
    private static final String SQLITE_LONGTEXT = "LONGTEXT";
    private static final String SQLITE_JSON = "JSON";
    private static final String SQLITE_CLOB = "CLOB";

    // ------------------------------time(text)-------------------------
    private static final String SQLITE_DATE = "DATE";
    private static final String SQLITE_DATETIME = "DATETIME";
    private static final String SQLITE_TIME = "TIME";
    private static final String SQLITE_TIMESTAMP = "TIMESTAMP";

    // ------------------------------blob-------------------------
    private static final String SQLITE_TINYBLOB = "TINYBLOB";
    private static final String SQLITE_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String SQLITE_BLOB = "BLOB";
    private static final String SQLITE_LONGBLOB = "LONGBLOB";
    private static final String SQLITE_BINARY = "BINARY";
    private static final String SQLITE_VARBINARY = "VARBINARY";
    private static final String SQLITE_LONGVARBINARY = "LONGVARBINARY";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnTypeName = metadata.getColumnTypeName(colIndex).toUpperCase().trim();
        switch (columnTypeName) {
            case SQLITE_BIT:
            case SQLITE_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case SQLITE_TINYINT:
            case SQLITE_TINYINT_UNSIGNED:
            case SQLITE_SMALLINT:
            case SQLITE_SMALLINT_UNSIGNED:
                return BasicType.SHORT_TYPE;
            case SQLITE_MEDIUMINT:
            case SQLITE_MEDIUMINT_UNSIGNED:
            case SQLITE_INT:
            case SQLITE_INTEGER:
                return BasicType.INT_TYPE;
            case SQLITE_INT_UNSIGNED:
            case SQLITE_INTEGER_UNSIGNED:
            case SQLITE_BIGINT:
            case SQLITE_BIGINT_UNSIGNED:
            case SQLITE_NUMERIC:
                return BasicType.LONG_TYPE;
            case SQLITE_DECIMAL:
            case SQLITE_DECIMAL_UNSIGNED:
            case SQLITE_DOUBLE:
            case SQLITE_DOUBLE_PRECISION:
            case SQLITE_REAL:
                return BasicType.DOUBLE_TYPE;
            case SQLITE_FLOAT:
                return BasicType.FLOAT_TYPE;
            case SQLITE_FLOAT_UNSIGNED:
                log.warn("{} will probably cause value overflow.", SQLITE_FLOAT_UNSIGNED);
                return BasicType.FLOAT_TYPE;
            case SQLITE_DOUBLE_UNSIGNED:
                log.warn("{} will probably cause value overflow.", SQLITE_DOUBLE_UNSIGNED);
                return BasicType.DOUBLE_TYPE;
            case SQLITE_CHARACTER:
            case SQLITE_VARYING_CHARACTER:
            case SQLITE_NATIVE_CHARACTER:
            case SQLITE_NVARCHAR:
            case SQLITE_NCHAR:
            case SQLITE_LONGNVARCHAR:
            case SQLITE_LONGVARCHAR:
            case SQLITE_CLOB:
            case SQLITE_CHAR:
            case SQLITE_TINYTEXT:
            case SQLITE_MEDIUMTEXT:
            case SQLITE_TEXT:
            case SQLITE_VARCHAR:
            case SQLITE_JSON:
            case SQLITE_LONGTEXT:

            case SQLITE_DATE:
            case SQLITE_TIME:
            case SQLITE_DATETIME:
            case SQLITE_TIMESTAMP:
                return BasicType.STRING_TYPE;

            case SQLITE_TINYBLOB:
            case SQLITE_MEDIUMBLOB:
            case SQLITE_BLOB:
            case SQLITE_LONGBLOB:
            case SQLITE_VARBINARY:
            case SQLITE_BINARY:
            case SQLITE_LONGVARBINARY:
                return PrimitiveByteArrayType.INSTANCE;

            //Doesn't support yet
            case SQLITE_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support SQLite type '%s' on column '%s'  yet.",
                                columnTypeName, jdbcColumnName));
        }
    }
}
