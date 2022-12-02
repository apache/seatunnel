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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class MySqlTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);

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
    private static final String MYSQL_FLOAT = "FLOAT";
    private static final String MYSQL_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String MYSQL_DOUBLE = "DOUBLE";
    private static final String MYSQL_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";

    // -------------------------string----------------------------
    private static final String MYSQL_CHAR = "CHAR";
    private static final String MYSQL_VARCHAR = "VARCHAR";
    private static final String MYSQL_TINYTEXT = "TINYTEXT";
    private static final String MYSQL_MEDIUMTEXT = "MEDIUMTEXT";
    private static final String MYSQL_TEXT = "TEXT";
    private static final String MYSQL_LONGTEXT = "LONGTEXT";
    private static final String MYSQL_JSON = "JSON";

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

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex).toUpperCase();
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (mysqlType) {
            case MYSQL_BIT:
                return BasicType.BOOLEAN_TYPE;
            case MYSQL_TINYINT:
            case MYSQL_TINYINT_UNSIGNED:
            case MYSQL_SMALLINT:
            case MYSQL_SMALLINT_UNSIGNED:
            case MYSQL_MEDIUMINT:
            case MYSQL_MEDIUMINT_UNSIGNED:
            case MYSQL_INT:
            case MYSQL_INTEGER:
            case MYSQL_YEAR:
                return BasicType.INT_TYPE;
            case MYSQL_INT_UNSIGNED:
            case MYSQL_INTEGER_UNSIGNED:
            case MYSQL_BIGINT:
                return BasicType.LONG_TYPE;
            case MYSQL_BIGINT_UNSIGNED:
                return new DecimalType(20, 0);
            case MYSQL_DECIMAL:
                if (precision > 38) {
                    LOG.warn("{} will probably cause value overflow.", MYSQL_DECIMAL);
                    return new DecimalType(38, 18);
                }
                return new DecimalType(precision, scale);
            case MYSQL_DECIMAL_UNSIGNED:
                return new DecimalType(precision + 1, scale);
            case MYSQL_FLOAT:
                return BasicType.FLOAT_TYPE;
            case MYSQL_FLOAT_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", MYSQL_FLOAT_UNSIGNED);
                return BasicType.FLOAT_TYPE;
            case MYSQL_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case MYSQL_DOUBLE_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", MYSQL_DOUBLE_UNSIGNED);
                return BasicType.DOUBLE_TYPE;
            case MYSQL_CHAR:
            case MYSQL_TINYTEXT:
            case MYSQL_MEDIUMTEXT:
            case MYSQL_TEXT:
            case MYSQL_VARCHAR:
            case MYSQL_JSON:
                return BasicType.STRING_TYPE;
            case MYSQL_LONGTEXT:
                LOG.warn(
                    "Type '{}' has a maximum precision of 536870911 in MySQL. "
                        + "Due to limitations in the seatunnel type system, "
                        + "the precision will be set to 2147483647.",
                    MYSQL_LONGTEXT);
                return BasicType.STRING_TYPE;
            case MYSQL_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case MYSQL_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case MYSQL_DATETIME:
            case MYSQL_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;

            case MYSQL_TINYBLOB:
            case MYSQL_MEDIUMBLOB:
            case MYSQL_BLOB:
            case MYSQL_LONGBLOB:
            case MYSQL_VARBINARY:
            case MYSQL_BINARY:
                return PrimitiveByteArrayType.INSTANCE;

            //Doesn't support yet
            case MYSQL_GEOMETRY:
            case MYSQL_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                    String.format(
                        "Doesn't support MySQL type '%s' on column '%s'  yet.",
                        mysqlType, jdbcColumnName));
        }
    }
}
