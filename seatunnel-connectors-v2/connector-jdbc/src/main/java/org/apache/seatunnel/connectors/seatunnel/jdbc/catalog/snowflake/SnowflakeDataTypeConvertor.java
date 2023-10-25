/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.snowflake;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.commons.collections4.MapUtils;

import com.google.auto.service.AutoService;

import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@AutoService(DataTypeConvertor.class)
public class SnowflakeDataTypeConvertor implements DataTypeConvertor<String> {

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";
    public static final Integer DEFAULT_PRECISION = 10;
    public static final Integer DEFAULT_SCALE = 0;

    /* ============================ data types ===================== */
    private static final String SNOWFLAKE_NUMBER = "NUMBER";
    private static final String SNOWFLAKE_DECIMAL = "DECIMAL";
    private static final String SNOWFLAKE_NUMERIC = "NUMERIC";
    private static final String SNOWFLAKE_INT = "INT";
    private static final String SNOWFLAKE_INTEGER = "INTEGER";
    private static final String SNOWFLAKE_BIGINT = "BIGINT";
    private static final String SNOWFLAKE_SMALLINT = "SMALLINT";
    private static final String SNOWFLAKE_TINYINT = "TINYINT";
    private static final String SNOWFLAKE_BYTEINT = "BYTEINT";

    private static final String SNOWFLAKE_FLOAT = "FLOAT";
    private static final String SNOWFLAKE_FLOAT4 = "FLOAT4";
    private static final String SNOWFLAKE_FLOAT8 = "FLOAT8";
    private static final String SNOWFLAKE_DOUBLE = "DOUBLE";
    private static final String SNOWFLAKE_DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String SNOWFLAKE_REAL = "REAL";

    private static final String SNOWFLAKE_VARCHAR = "VARCHAR";
    private static final String SNOWFLAKE_CHAR = "CHAR";
    private static final String SNOWFLAKE_CHARACTER = "CHARACTER";
    private static final String SNOWFLAKE_STRING = "STRING";
    private static final String SNOWFLAKE_TEXT = "TEXT";
    private static final String SNOWFLAKE_BINARY = "BINARY";
    private static final String SNOWFLAKE_VARBINARY = "VARBINARY";

    private static final String SNOWFLAKE_BOOLEAN = "BOOLEAN";

    private static final String SNOWFLAKE_DATE = "DATE";
    private static final String SNOWFLAKE_DATE_TIME = "DATE_TIME";
    private static final String SNOWFLAKE_TIME = "TIME";
    private static final String SNOWFLAKE_TIMESTAMP = "TIMESTAMP";
    private static final String SNOWFLAKE_TIMESTAMP_LTZ = "TIMESTAMP_LTZ";
    private static final String SNOWFLAKE_TIMESTAMP_NTZ = "TIMESTAMP_NTZ";
    private static final String SNOWFLAKE_TIMESTAMP_TZ = "TIMESTAMP_TZ";

    private static final String SNOWFLAKE_GEOGRAPHY = "GEOGRAPHY";
    private static final String SNOWFLAKE_GEOMETRY = "GEOMETRY";

    private static final String SNOWFLAKE_VARIANT = "VARIANT";
    private static final String SNOWFLAKE_OBJECT = "OBJECT";

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        return toSeaTunnelType(connectorDataType, Collections.emptyMap());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String connectorDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        checkNotNull(connectorDataType, "redshiftType cannot be null");

        switch (connectorDataType) {
            case SNOWFLAKE_SMALLINT:
            case SNOWFLAKE_TINYINT:
            case SNOWFLAKE_BYTEINT:
                return BasicType.SHORT_TYPE;
            case SNOWFLAKE_INTEGER:
            case SNOWFLAKE_INT:
                return BasicType.INT_TYPE;
            case SNOWFLAKE_BIGINT:
                return BasicType.LONG_TYPE;
            case SNOWFLAKE_DECIMAL:
            case SNOWFLAKE_NUMERIC:
            case SNOWFLAKE_NUMBER:
                Integer precision =
                        MapUtils.getInteger(dataTypeProperties, PRECISION, DEFAULT_PRECISION);
                Integer scale = MapUtils.getInteger(dataTypeProperties, SCALE, DEFAULT_SCALE);
                return new DecimalType(precision, scale);
            case SNOWFLAKE_REAL:
            case SNOWFLAKE_FLOAT4:
                return BasicType.FLOAT_TYPE;
            case SNOWFLAKE_DOUBLE:
            case SNOWFLAKE_DOUBLE_PRECISION:
            case SNOWFLAKE_FLOAT8:
            case SNOWFLAKE_FLOAT:
                return BasicType.DOUBLE_TYPE;
            case SNOWFLAKE_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case SNOWFLAKE_CHAR:
            case SNOWFLAKE_CHARACTER:
            case SNOWFLAKE_VARCHAR:
            case SNOWFLAKE_STRING:
            case SNOWFLAKE_TEXT:
            case SNOWFLAKE_VARIANT:
            case SNOWFLAKE_OBJECT:
            case SNOWFLAKE_GEOMETRY:
                return BasicType.STRING_TYPE;
            case SNOWFLAKE_BINARY:
            case SNOWFLAKE_VARBINARY:
            case SNOWFLAKE_GEOGRAPHY:
                return PrimitiveByteArrayType.INSTANCE;
            case SNOWFLAKE_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case SNOWFLAKE_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case SNOWFLAKE_DATE_TIME:
            case SNOWFLAKE_TIMESTAMP:
            case SNOWFLAKE_TIMESTAMP_LTZ:
            case SNOWFLAKE_TIMESTAMP_NTZ:
            case SNOWFLAKE_TIMESTAMP_TZ:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support SNOWFLAKE type '%s' yet.", connectorDataType));
        }
    }

    @Override
    public String toConnectorType(
            SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        checkNotNull(seaTunnelDataType, "seaTunnelDataType cannot be null");
        SqlType sqlType = seaTunnelDataType.getSqlType();

        switch (sqlType) {
            case TINYINT:
            case SMALLINT:
                return SNOWFLAKE_SMALLINT;
            case INT:
                return SNOWFLAKE_INTEGER;
            case BIGINT:
                return SNOWFLAKE_BIGINT;
            case DECIMAL:
                return SNOWFLAKE_DECIMAL;
            case FLOAT:
                return SNOWFLAKE_FLOAT4;
            case DOUBLE:
                return SNOWFLAKE_DOUBLE_PRECISION;
            case BOOLEAN:
                return SNOWFLAKE_BOOLEAN;
            case STRING:
                return SNOWFLAKE_TEXT;
            case DATE:
                return SNOWFLAKE_DATE;
            case BYTES:
                return SNOWFLAKE_GEOMETRY;
            case TIME:
                return SNOWFLAKE_TIME;
            case TIMESTAMP:
                return SNOWFLAKE_TIMESTAMP;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support SeaTunnel type '%s''  yet.", seaTunnelDataType));
        }
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.SNOWFLAKE;
    }
}
