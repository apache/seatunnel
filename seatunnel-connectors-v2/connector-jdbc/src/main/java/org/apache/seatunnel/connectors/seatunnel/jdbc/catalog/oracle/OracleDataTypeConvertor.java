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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.commons.collections4.MapUtils;

import com.google.auto.service.AutoService;

import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

@AutoService(DataTypeConvertor.class)
public class OracleDataTypeConvertor implements DataTypeConvertor<String> {

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";
    public static final Integer DEFAULT_PRECISION = 38;
    public static final Integer DEFAULT_SCALE = 18;

    // ============================data types=====================
    public static final String ORACLE_UNKNOWN = "UNKNOWN";
    // -------------------------number----------------------------
    public static final String ORACLE_BINARY_DOUBLE = "BINARY_DOUBLE";
    public static final String ORACLE_BINARY_FLOAT = "BINARY_FLOAT";
    public static final String ORACLE_NUMBER = "NUMBER";
    public static final String ORACLE_FLOAT = "FLOAT";
    public static final String ORACLE_REAL = "REAL";
    public static final String ORACLE_INTEGER = "INTEGER";
    // -------------------------string----------------------------
    public static final String ORACLE_CHAR = "CHAR";
    public static final String ORACLE_VARCHAR2 = "VARCHAR2";
    public static final String ORACLE_NCHAR = "NCHAR";
    public static final String ORACLE_NVARCHAR2 = "NVARCHAR2";
    public static final String ORACLE_LONG = "LONG";
    public static final String ORACLE_ROWID = "ROWID";
    public static final String ORACLE_CLOB = "CLOB";
    public static final String ORACLE_NCLOB = "NCLOB";
    // ------------------------------time-------------------------
    public static final String ORACLE_DATE = "DATE";
    public static final String ORACLE_TIMESTAMP = "TIMESTAMP";
    public static final String ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE =
            "TIMESTAMP WITH LOCAL TIME ZONE";
    // ------------------------------blob-------------------------
    public static final String ORACLE_BLOB = "BLOB";
    public static final String ORACLE_BFILE = "BFILE";
    public static final String ORACLE_RAW = "RAW";
    public static final String ORACLE_LONG_RAW = "LONG RAW";

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        return toSeaTunnelType(connectorDataType, Collections.emptyMap());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String connectorDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        checkNotNull(connectorDataType, "Oracle Type cannot be null");
        connectorDataType = normalizeTimestamp(connectorDataType);
        switch (connectorDataType) {
            case ORACLE_INTEGER:
                return BasicType.INT_TYPE;
            case ORACLE_FLOAT:
                // The float type will be converted to DecimalType(10, -127),
                // which will lose precision in the spark engine
                return new DecimalType(38, 18);
            case ORACLE_NUMBER:
                int precision =
                        MapUtils.getInteger(dataTypeProperties, PRECISION, DEFAULT_PRECISION);
                int scale = MapUtils.getInteger(dataTypeProperties, SCALE, DEFAULT_SCALE);
                if (scale == 0) {
                    if (precision == 1) {
                        return BasicType.BOOLEAN_TYPE;
                    }
                    if (precision <= 9) {
                        return BasicType.INT_TYPE;
                    }
                    if (precision <= 18) {
                        return BasicType.LONG_TYPE;
                    }
                }
                return new DecimalType(38, 18);
            case ORACLE_BINARY_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case ORACLE_BINARY_FLOAT:
            case ORACLE_REAL:
                return BasicType.FLOAT_TYPE;
            case ORACLE_CHAR:
            case ORACLE_NCHAR:
            case ORACLE_NVARCHAR2:
            case ORACLE_VARCHAR2:
            case ORACLE_LONG:
            case ORACLE_ROWID:
            case ORACLE_NCLOB:
            case ORACLE_CLOB:
                return BasicType.STRING_TYPE;
            case ORACLE_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case ORACLE_TIMESTAMP:
            case ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case ORACLE_BLOB:
            case ORACLE_RAW:
            case ORACLE_LONG_RAW:
            case ORACLE_BFILE:
                return PrimitiveByteArrayType.INSTANCE;
                // Doesn't support yet
            case ORACLE_UNKNOWN:
            default:
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format("Doesn't support ORACLE type '%s' yet.", connectorDataType));
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
            case INT:
                return ORACLE_INTEGER;
            case BIGINT:
                return ORACLE_NUMBER;
            case FLOAT:
                return ORACLE_FLOAT;
            case DOUBLE:
                return ORACLE_BINARY_DOUBLE;
            case DECIMAL:
                return ORACLE_NUMBER;
            case BOOLEAN:
                return ORACLE_NUMBER;
            case STRING:
                return ORACLE_VARCHAR2;
            case DATE:
                return ORACLE_DATE;
            case TIMESTAMP:
                return ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE;
            case BYTES:
                return ORACLE_BLOB;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support SeaTunnel type '%s' yet.", seaTunnelDataType));
        }
    }

    public static String normalizeTimestamp(String oracleType) {
        // Create a pattern to match TIMESTAMP followed by an optional (0-9)
        String pattern = "^TIMESTAMP(\\([0-9]\\))?$";
        // Create a Pattern object
        Pattern r = Pattern.compile(pattern);
        // Now create matcher object.
        Matcher m = r.matcher(oracleType);
        if (m.find()) {
            return "TIMESTAMP";
        } else {
            return oracleType;
        }
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.ORACLE;
    }
}
