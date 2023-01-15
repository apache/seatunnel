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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oceanbase;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Slf4j
public class OceanBaseTypeMapper implements JdbcDialectTypeMapper {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);

    private static final int PRECISION_MAX = 38;
    private static final int SCALE_MAX = 10;
    //ref https://www.oceanbase.com/docs/community-observer-cn-10000000000901502
    // ============================data types=====================
    private static final String OCEANBASE_UNKNOWN = "UNKNOWN";
    private static final String OCEANBASE_BIT = "BIT";

    // -------------------------number----------------------------
    private static final String OCEANBASE_BIGINT = "BIGINT";
    private static final String OCEANBASE_INT = "INT";
    private static final String OCEANBASE_INTEGER = "INTEGER";
    private static final String OCEANBASE_SMALLINT = "SMALLINT";
    private static final String OCEANBASE_BOOL = "BOOL";
    private static final String OCEANBASE_BOOLEAN = "BOOLEAN";
    private static final String OCEANBASE_TINYINT = "TINYINT";
    private static final String OCEANBASE_DECIMAL = "DECIMAL";
    private static final String OCEANBASE_FLOAT = "FLOAT";
    private static final String OCEANBASE_DOUBLE = "DOUBLE";
    private static final String OCEANBASE_NUMBER = "NUMBER";

    // ------------------------------time-------------------------
    private static final String OCEANBASE_DATE = "DATE";
    private static final String OCEANBASE_TIME = "TIME";
    private static final String OCEANBASE_DATETIME = "DATETIME";
    private static final String OCEANBASE_TIMESTAMP = "TIMESTAMP";
    private static final String OCEANBASE_YEAR = "YEAR";

    // -------------------------string----------------------------
    private static final String OCEANBASE_VARCHAR = "VARCHAR";
    private static final String OCEANBASE_VARBINARY = "VARBINARY";
    private static final String OCEANBASE_CHAR = "CHAR";
    private static final String OCEANBASE_BINARY = "BINARY";
    private static final String OCEANBASE_TINYTEXT = "TINYTEXT";
    private static final String OCEANBASE_TEXT = "TEXT";
    private static final String OCEANBASE_MEDIUMTEXT = "MEDIUMTEXT";
    private static final String OCEANBASE_LONGTEXT = "LONGTEXT";
    private static final String OCEANBASE_ENUM = "ENUM";
    private static final String OCEANBASE_SET = "SET";
    private static final String OCEANBASE_JSON = "JSON";

    // -------------------------blob------------------------------
    private static final String OCEANBASE_TINYBLOB = "TINYBLOB";
    private static final String OCEANBASE_BLOB = "BLOB";
    private static final String OCEANBASE_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String OCEANBASE_LONGBLOB = "LONGBLOB";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String oceanBaseType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (oceanBaseType) {
            case OCEANBASE_BIT:
                return BasicType.BOOLEAN_TYPE;

            case OCEANBASE_INT:
            case OCEANBASE_INTEGER:
            case OCEANBASE_YEAR:
                return BasicType.INT_TYPE;

            case OCEANBASE_SMALLINT:
                return BasicType.SHORT_TYPE;
            case OCEANBASE_BIGINT:
                return BasicType.LONG_TYPE;

            case OCEANBASE_BOOL:
            case OCEANBASE_BOOLEAN:
            case OCEANBASE_TINYINT:
                return BasicType.BYTE_TYPE;

            case OCEANBASE_DECIMAL:
                if (precision > PRECISION_MAX) {
                    LOG.warn("{} will probably cause value overflow.", OCEANBASE_DECIMAL);
                    return new DecimalType(PRECISION_MAX, SCALE_MAX);
                }
                return new DecimalType(precision, scale);

            case OCEANBASE_FLOAT:
                return BasicType.FLOAT_TYPE;
            case OCEANBASE_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case OCEANBASE_NUMBER:
                if (scale == 0 && precision == 0) {
                    return BasicType.DOUBLE_TYPE;
                }
                if (scale == 0 && precision > 0) {
                    return BasicType.LONG_TYPE;
                }
                if (precision > PRECISION_MAX) {
                    LOG.warn("{} will probably cause value overflow.", OCEANBASE_NUMBER);
                    return new DecimalType(PRECISION_MAX, SCALE_MAX);
                } else {
                    return new DecimalType(precision, scale);
                }

            case OCEANBASE_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case OCEANBASE_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case OCEANBASE_DATETIME:
            case OCEANBASE_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;

            case OCEANBASE_VARCHAR:
            case OCEANBASE_VARBINARY:
            case OCEANBASE_CHAR:
            case OCEANBASE_BINARY:
            case OCEANBASE_TINYTEXT:
            case OCEANBASE_TEXT:
            case OCEANBASE_MEDIUMTEXT:
            case OCEANBASE_ENUM:
            case OCEANBASE_SET:
            case OCEANBASE_JSON:
                return BasicType.STRING_TYPE;
            case OCEANBASE_LONGTEXT:
                LOG.warn(
                    "Type '{}' has a maximum precision of 50331648 in OceanBase. "
                        + "Due to limitations in the seatunnel type system, "
                        + "the precision will be set to 2147483647.",
                    OCEANBASE_LONGTEXT);
                return BasicType.STRING_TYPE;

            case OCEANBASE_TINYBLOB:
            case OCEANBASE_BLOB:
            case OCEANBASE_MEDIUMBLOB:
            case OCEANBASE_LONGBLOB:
                return PrimitiveByteArrayType.INSTANCE;

            case OCEANBASE_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                    String.format(
                        "Doesn't support OceanBase type '%s' on column '%s' yet.",
                        oceanBaseType, jdbcColumnName));
        }
    }
}
