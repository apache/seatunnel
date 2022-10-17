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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kylin;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class KylinTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(KylinTypeMapper.class);

    // ============================data types=====================

    private static final String KYLIN_UNKNOWN = "UNKNOWN";
    private static final String KYLIN_BOOLEAN = "BOOLEAN";

    // -------------------------number----------------------------
    private static final String KYLIN_TINYINT = "TINYINT";
    private static final String KYLIN_SMALLINT = "SMALLINT";
    private static final String KYLIN_INT = "INT";
    private static final String KYLIN_BIGINT = "BIGINT";
    private static final String KYLIN_DECIMAL = "DECIMAL";
    private static final String KYLIN_FLOAT = "FLOAT";
    private static final String KYLIN_DOUBLE = "DOUBLE";

    // -------------------------string----------------------------
    private static final String KYLIN_CHAR = "CHAR";
    private static final String KYLIN_VARCHAR = "VARCHAR";
    private static final String KYLIN_STRING = "STRING";

    // ------------------------------time-------------------------
    private static final String KYLIN_DATE = "DATE";
    private static final String KYLIN_TIME = "TIME";
    private static final String KYLIN_TIMESTAMP = "TIMESTAMP";

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String kylinType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (kylinType) {
            case KYLIN_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case KYLIN_TINYINT:
            case KYLIN_SMALLINT:
            case KYLIN_INT:
                return BasicType.INT_TYPE;
            case KYLIN_BIGINT:
                return BasicType.LONG_TYPE;
            case KYLIN_DECIMAL:
                return new DecimalType(precision, scale);
            case KYLIN_FLOAT:
                return BasicType.FLOAT_TYPE;
            case KYLIN_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case KYLIN_CHAR:
            case KYLIN_VARCHAR:
            case KYLIN_STRING:
                return BasicType.STRING_TYPE;
            case KYLIN_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case KYLIN_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case KYLIN_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            //Doesn't support yet
            case KYLIN_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support KYLIN type '%s' on column '%s'  yet.",
                                kylinType, jdbcColumnName));
        }
    }
}
