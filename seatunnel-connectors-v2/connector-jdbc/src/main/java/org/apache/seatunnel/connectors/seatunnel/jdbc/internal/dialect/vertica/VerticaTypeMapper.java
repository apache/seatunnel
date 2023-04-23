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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.vertica;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class VerticaTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);

    // ============================data types=====================
    // refer to :
    // https://www.vertica.com/docs/12.0.x/HTML/Content/Authoring/SQLReferenceManual/DataTypes/SQLDataTypes.htm

    private static final String VERTICA_UNKNOWN = "UNKNOWN";
    private static final String VERTICA_BIT = "BIT";

    // -------------------------number----------------------------
    private static final String VERTICA_TINYINT = "TINYINT";
    private static final String VERTICA_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String VERTICA_SMALLINT = "SMALLINT";
    private static final String VERTICA_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String VERTICA_MEDIUMINT = "MEDIUMINT";
    private static final String VERTICA_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String VERTICA_INT = "INT";
    private static final String VERTICA_INT_UNSIGNED = "INT UNSIGNED";
    private static final String VERTICA_INTEGER = "INTEGER";
    private static final String VERTICA_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String VERTICA_BIGINT = "BIGINT";
    private static final String VERTICA_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String VERTICA_DECIMAL = "DECIMAL";
    private static final String VERTICA_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String VERTICA_FLOAT = "FLOAT";
    private static final String VERTICA_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String VERTICA_DOUBLE = "DOUBLE";
    private static final String VERTICA_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";

    // -------------------------string----------------------------
    private static final String VERTICA_CHAR = "CHAR";
    private static final String VERTICA_VARCHAR = "VARCHAR";
    private static final String VERTICA_TINYTEXT = "TINYTEXT";
    private static final String VERTICA_MEDIUMTEXT = "MEDIUMTEXT";
    private static final String VERTICA_TEXT = "TEXT";
    private static final String VERTICA_LONGTEXT = "LONGTEXT";
    private static final String VERTICA_JSON = "JSON";

    // ------------------------------time-------------------------
    private static final String VERTICA_DATE = "DATE";
    private static final String VERTICA_DATETIME = "DATETIME";
    private static final String VERTICA_TIME = "TIME";
    private static final String VERTICA_TIMESTAMP = "TIMESTAMP";
    private static final String VERTICA_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    private static final String VERTICA_TINYBLOB = "TINYBLOB";
    private static final String VERTICA_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String VERTICA_BLOB = "BLOB";
    private static final String VERTICA_LONGBLOB = "LONGBLOB";
    private static final String VERTICA_BINARY = "BINARY";
    private static final String VERTICA_VARBINARY = "VARBINARY";
    private static final String VERTICA_GEOMETRY = "GEOMETRY";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String type = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (type) {
            case VERTICA_BIT:
                return BasicType.BOOLEAN_TYPE;
            case VERTICA_TINYINT:
            case VERTICA_TINYINT_UNSIGNED:
            case VERTICA_SMALLINT:
            case VERTICA_SMALLINT_UNSIGNED:
            case VERTICA_MEDIUMINT:
            case VERTICA_MEDIUMINT_UNSIGNED:
            case VERTICA_INT:
            case VERTICA_INTEGER:
            case VERTICA_YEAR:
                return BasicType.INT_TYPE;
            case VERTICA_INT_UNSIGNED:
            case VERTICA_INTEGER_UNSIGNED:
            case VERTICA_BIGINT:
                return BasicType.LONG_TYPE;
            case VERTICA_BIGINT_UNSIGNED:
                return new DecimalType(20, 0);
            case VERTICA_DECIMAL:
                if (precision > 38) {
                    LOG.warn("{} will probably cause value overflow.", VERTICA_DECIMAL);
                    return new DecimalType(38, 18);
                }
                return new DecimalType(precision, scale);
            case VERTICA_DECIMAL_UNSIGNED:
                return new DecimalType(precision + 1, scale);
            case VERTICA_FLOAT:
                return BasicType.FLOAT_TYPE;
            case VERTICA_FLOAT_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", VERTICA_FLOAT_UNSIGNED);
                return BasicType.FLOAT_TYPE;
            case VERTICA_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case VERTICA_DOUBLE_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", VERTICA_DOUBLE_UNSIGNED);
                return BasicType.DOUBLE_TYPE;
            case VERTICA_CHAR:
            case VERTICA_TINYTEXT:
            case VERTICA_MEDIUMTEXT:
            case VERTICA_TEXT:
            case VERTICA_VARCHAR:
            case VERTICA_JSON:
                return BasicType.STRING_TYPE;
            case VERTICA_LONGTEXT:
                LOG.warn(
                        "Type '{}' has a maximum precision of 536870911 in Vertica. "
                                + "Due to limitations in the seatunnel type system, "
                                + "the precision will be set to 2147483647.",
                        VERTICA_LONGTEXT);
                return BasicType.STRING_TYPE;
            case VERTICA_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case VERTICA_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case VERTICA_DATETIME:
            case VERTICA_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;

            case VERTICA_TINYBLOB:
            case VERTICA_MEDIUMBLOB:
            case VERTICA_BLOB:
            case VERTICA_LONGBLOB:
            case VERTICA_VARBINARY:
            case VERTICA_BINARY:
                return PrimitiveByteArrayType.INSTANCE;

                // Doesn't support yet
            case VERTICA_GEOMETRY:
            case VERTICA_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Doesn't support Vertica type '%s' on column '%s'  yet.",
                                type, jdbcColumnName));
        }
    }
}
