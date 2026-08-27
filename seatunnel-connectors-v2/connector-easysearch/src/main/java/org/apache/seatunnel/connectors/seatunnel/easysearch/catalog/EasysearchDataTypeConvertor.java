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

package org.apache.seatunnel.connectors.seatunnel.easysearch.catalog;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;

import com.google.auto.service.AutoService;

import java.util.Map;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

@AutoService(DataTypeConvertor.class)
public class EasysearchDataTypeConvertor implements DataTypeConvertor<String> {

    public static final String STRING = "string";
    public static final String KEYWORD = "keyword";
    public static final String TEXT = "text";
    public static final String BOOLEAN = "boolean";
    public static final String BYTE = "byte";
    public static final String SHORT = "short";
    public static final String INTEGER = "integer";
    public static final String LONG = "long";
    public static final String FLOAT = "float";
    public static final String HALF_FLOAT = "half_float";
    public static final String DOUBLE = "double";
    public static final String DATE = "date";

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType) {
        return toSeaTunnelType(field, connectorDataType, null);
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, String connectorDataType, Map<String, Object> dataTypeProperties) {
        checkNotNull(connectorDataType, "connectorDataType can not be null");
        switch (connectorDataType) {
            case STRING:
                return BasicType.STRING_TYPE;
            case KEYWORD:
                return BasicType.STRING_TYPE;
            case TEXT:
                return BasicType.STRING_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case BYTE:
                return BasicType.BYTE_TYPE;
            case SHORT:
                return BasicType.SHORT_TYPE;
            case INTEGER:
                return BasicType.INT_TYPE;
            case LONG:
                return BasicType.LONG_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case HALF_FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                return BasicType.STRING_TYPE;
        }
    }

    @Override
    public String toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) {
        checkNotNull(seaTunnelDataType, "seaTunnelDataType can not be null");
        SqlType sqlType = seaTunnelDataType.getSqlType();
        switch (sqlType) {
            case STRING:
                return STRING;
            case BOOLEAN:
                return BOOLEAN;
            case BYTES:
                return BYTE;
            case TINYINT:
                return SHORT;
            case INT:
                return INTEGER;
            case BIGINT:
                return LONG;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case TIMESTAMP:
                return DATE;
            default:
                return STRING;
        }
    }

    @Override
    public String getIdentity() {
        return "Easysearch";
    }
}
