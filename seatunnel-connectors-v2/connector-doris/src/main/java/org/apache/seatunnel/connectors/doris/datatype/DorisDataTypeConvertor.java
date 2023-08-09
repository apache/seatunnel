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

package org.apache.seatunnel.connectors.doris.datatype;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;

import org.apache.commons.collections4.MapUtils;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@AutoService(DataTypeConvertor.class)
public class DorisDataTypeConvertor implements DataTypeConvertor<String> {

    public static final String NULL = "NULL";
    public static final String BOOLEAN = "BOOLEAN";
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
    public static final String BIGINT = "BIGINT";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String DECIMAL = "DECIMAL";
    public static final String DATE = "DATE";
    public static final String DATETIME = "DATETIME";
    public static final String CHAR = "CHAR";
    public static final String VARCHAR = "VARCHAR";
    public static final String BINARY = "BINARY";
    public static final String VARBINARY = "VARBINARY";
    public static final String ARRAY = "ARRAY";
    public static final String MAP = "MAP";
    public static final String STRUCT = "STRUCT";
    public static final String UNION = "UNION";
    public static final String INTERVAL = "INTERVAL";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String YEAR = "YEAR";
    public static final String GEOMETRY = "GEOMETRY";
    public static final String IP = "IP";

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";

    public static final Integer DEFAULT_PRECISION = 10;

    public static final Integer DEFAULT_SCALE = 0;

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        checkNotNull(connectorDataType, "connectorDataType can not be null");
        Map<String, Object> dataTypeProperties;
        switch (connectorDataType.toUpperCase(Locale.ROOT)) {
            case DECIMAL:
                // parse precision and scale
                int left = connectorDataType.indexOf("(");
                int right = connectorDataType.indexOf(")");
                int precision = DEFAULT_PRECISION;
                int scale = DEFAULT_SCALE;
                if (left != -1 && right != -1) {
                    String[] precisionAndScale =
                            connectorDataType.substring(left + 1, right).split(",");
                    if (precisionAndScale.length == 2) {
                        precision = Integer.parseInt(precisionAndScale[0]);
                        scale = Integer.parseInt(precisionAndScale[1]);
                    } else if (precisionAndScale.length == 1) {
                        precision = Integer.parseInt(precisionAndScale[0]);
                    }
                }
                dataTypeProperties = ImmutableMap.of(PRECISION, precision, SCALE, scale);
                break;
            default:
                dataTypeProperties = Collections.emptyMap();
                break;
        }
        return toSeaTunnelType(connectorDataType, dataTypeProperties);
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String connectorDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        checkNotNull(connectorDataType, "mysqlType can not be null");
        int precision;
        int scale;
        switch (connectorDataType.toUpperCase(Locale.ROOT)) {
            case NULL:
                return BasicType.VOID_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case TINYINT:
                return BasicType.BYTE_TYPE;
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case INT:
            case YEAR:
                return BasicType.INT_TYPE;
            case BIGINT:
                return BasicType.LONG_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIMESTAMP:
            case DATETIME:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case CHAR:
            case VARCHAR:
                return BasicType.STRING_TYPE;
            case BINARY:
            case VARBINARY:
            case GEOMETRY:
                return PrimitiveByteArrayType.INSTANCE;
            case DECIMAL:
                precision = MapUtils.getInteger(dataTypeProperties, PRECISION, DEFAULT_PRECISION);
                scale = MapUtils.getInteger(dataTypeProperties, SCALE, DEFAULT_SCALE);
                return new DecimalType(precision, scale);
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support DORIS type '%s''  yet.", connectorDataType));
        }
    }

    @Override
    public String toConnectorType(
            SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        checkNotNull(seaTunnelDataType, "seaTunnelDataType cannot be null");
        SqlType sqlType = seaTunnelDataType.getSqlType();
        // todo: verify
        switch (sqlType) {
            case ARRAY:
                return ARRAY;
            case MAP:
            case ROW:
            case STRING:
            case NULL:
                return VARCHAR;
            case BOOLEAN:
                return BOOLEAN;
            case TINYINT:
                return TINYINT;
            case SMALLINT:
                return SMALLINT;
            case INT:
                return INT;
            case BIGINT:
                return BIGINT;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case DECIMAL:
                return DECIMAL;
            case BYTES:
                return BINARY;
            case DATE:
                return DATE;
            case TIME:
            case TIMESTAMP:
                return TIMESTAMP;
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support Doris type '%s''  yet.", sqlType));
        }
    }

    @Override
    public String getIdentity() {
        return "Doris";
    }
}
