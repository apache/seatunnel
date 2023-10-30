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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.redshift;

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
public class RedshiftDataTypeConvertor implements DataTypeConvertor<String> {

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";

    public static final Integer DEFAULT_PRECISION = 10;

    public static final Integer DEFAULT_SCALE = 0;

    /* ============================ data types ===================== */
    private static final String REDSHIFT_SMALLINT = "smallint";
    private static final String REDSHIFT_INT2 = "int2";
    private static final String REDSHIFT_INTEGER = "integer";
    private static final String REDSHIFT_INT = "int";
    private static final String REDSHIFT_INT4 = "int4";
    private static final String REDSHIFT_BIGINT = "bigint";
    private static final String REDSHIFT_INT8 = "int8";

    private static final String REDSHIFT_DECIMAL = "decimal";
    private static final String REDSHIFT_NUMERIC = "numeric";
    private static final String REDSHIFT_REAL = "real";
    private static final String REDSHIFT_FLOAT4 = "float4";
    private static final String REDSHIFT_DOUBLE_PRECISION = "double precision";
    private static final String REDSHIFT_FLOAT8 = "float8";
    private static final String REDSHIFT_FLOAT = "float";

    private static final String REDSHIFT_BOOLEAN = "boolean";
    private static final String REDSHIFT_BOOL = "bool";

    private static final String REDSHIFT_CHAR = "char";
    private static final String REDSHIFT_CHARACTER = "character";
    private static final String REDSHIFT_NCHAR = "nchar";
    private static final String REDSHIFT_BPCHAR = "bpchar";

    private static final String REDSHIFT_VARCHAR = "varchar";
    private static final String REDSHIFT_CHARACTER_VARYING = "character varying";
    private static final String REDSHIFT_NVARCHAR = "nvarchar";
    private static final String REDSHIFT_TEXT = "text";

    private static final String REDSHIFT_DATE = "date";
    /*FIXME*/

    private static final String REDSHIFT_GEOMETRY = "geometry";
    private static final String REDSHIFT_OID = "oid";
    private static final String REDSHIFT_SUPER = "super";

    private static final String REDSHIFT_TIME = "time";
    private static final String REDSHIFT_TIME_WITH_TIME_ZONE = "time with time zone";

    private static final String REDSHIFT_TIMETZ = "timetz";
    private static final String REDSHIFT_TIMESTAMP = "timestamp";
    private static final String REDSHIFT_TIMESTAMP_WITH_OUT_TIME_ZONE =
            "timestamp without time zone";

    private static final String REDSHIFT_TIMESTAMPTZ = "timestamptz";

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
            case REDSHIFT_SMALLINT:
            case REDSHIFT_INT2:
                return BasicType.SHORT_TYPE;
            case REDSHIFT_INTEGER:
            case REDSHIFT_INT:
            case REDSHIFT_INT4:
                return BasicType.INT_TYPE;
            case REDSHIFT_BIGINT:
            case REDSHIFT_INT8:
            case REDSHIFT_OID:
                return BasicType.LONG_TYPE;
            case REDSHIFT_DECIMAL:
            case REDSHIFT_NUMERIC:
                Integer precision =
                        MapUtils.getInteger(dataTypeProperties, PRECISION, DEFAULT_PRECISION);
                Integer scale = MapUtils.getInteger(dataTypeProperties, SCALE, DEFAULT_SCALE);
                return new DecimalType(precision, scale);
            case REDSHIFT_REAL:
            case REDSHIFT_FLOAT4:
                return BasicType.FLOAT_TYPE;
            case REDSHIFT_DOUBLE_PRECISION:
            case REDSHIFT_FLOAT8:
            case REDSHIFT_FLOAT:
                return BasicType.DOUBLE_TYPE;
            case REDSHIFT_BOOLEAN:
            case REDSHIFT_BOOL:
                return BasicType.BOOLEAN_TYPE;
            case REDSHIFT_CHAR:
            case REDSHIFT_CHARACTER:
            case REDSHIFT_NCHAR:
            case REDSHIFT_BPCHAR:
            case REDSHIFT_VARCHAR:
            case REDSHIFT_CHARACTER_VARYING:
            case REDSHIFT_NVARCHAR:
            case REDSHIFT_TEXT:
            case REDSHIFT_SUPER:
                return BasicType.STRING_TYPE;
            case REDSHIFT_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case REDSHIFT_GEOMETRY:
                return PrimitiveByteArrayType.INSTANCE;
            case REDSHIFT_TIME:
            case REDSHIFT_TIME_WITH_TIME_ZONE:
            case REDSHIFT_TIMETZ:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case REDSHIFT_TIMESTAMP:
            case REDSHIFT_TIMESTAMP_WITH_OUT_TIME_ZONE:
            case REDSHIFT_TIMESTAMPTZ:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support REDSHIFT type '%s''  yet.", connectorDataType));
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
                return REDSHIFT_SMALLINT;
            case INT:
                return REDSHIFT_INTEGER;
            case BIGINT:
                return REDSHIFT_BIGINT;
            case DECIMAL:
                return REDSHIFT_DECIMAL;
            case FLOAT:
                return REDSHIFT_FLOAT4;
            case DOUBLE:
                return REDSHIFT_DOUBLE_PRECISION;
            case BOOLEAN:
                return REDSHIFT_BOOLEAN;
            case STRING:
                return REDSHIFT_TEXT;
            case DATE:
                return REDSHIFT_DATE;
            case BYTES:
                return REDSHIFT_GEOMETRY;
            case TIME:
                return REDSHIFT_TIME;
            case TIMESTAMP:
                return REDSHIFT_TIMESTAMP;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support SeaTunnel type '%s''  yet.", seaTunnelDataType));
        }
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.REDSHIFT;
    }
}
