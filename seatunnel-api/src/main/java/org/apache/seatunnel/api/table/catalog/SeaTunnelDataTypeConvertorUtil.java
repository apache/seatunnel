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

package org.apache.seatunnel.api.table.catalog;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.utils.JsonUtils;

import java.util.Map;

public class SeaTunnelDataTypeConvertorUtil {

    /**
     * @param columnType column type, should be {@link SeaTunnelDataType##toString}.
     * @return {@link SeaTunnelDataType} instance.
     */
    public static SeaTunnelDataType<?> deserializeSeaTunnelDataType(String columnType) {
        SqlType sqlType = null;
        try {
            sqlType = SqlType.valueOf(columnType.toUpperCase().replace(" ", ""));
        } catch (IllegalArgumentException e) {
            // nothing
        }
        if (sqlType == null) {
            return parseComplexDataType(columnType);
        }
        switch (sqlType) {
            case STRING:
                return BasicType.STRING_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case TINYINT:
                return BasicType.BYTE_TYPE;
            case BYTES:
                return PrimitiveByteArrayType.INSTANCE;
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case INT:
                return BasicType.INT_TYPE;
            case BIGINT:
                return BasicType.LONG_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case NULL:
                return BasicType.VOID_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case MAP:
                return parseMapType(columnType);
            default:
                throw new UnsupportedOperationException(
                        String.format("the type[%s] is not support", columnType));
        }
    }

    private static SeaTunnelDataType<?> parseComplexDataType(String columnStr) {
        String column = columnStr.toUpperCase().replace(" ", "");
        if (column.startsWith(SqlType.MAP.name())) {
            return parseMapType(column);
        }
        if (column.startsWith(SqlType.ARRAY.name())) {
            return parseArrayType(column);
        }
        if (column.startsWith(SqlType.DECIMAL.name())) {
            return parseDecimalType(column);
        }
        return parseRowType(columnStr);
    }

    private static SeaTunnelDataType<?> parseRowType(String columnStr) {
        ObjectNode jsonNodes = JsonUtils.parseObject(columnStr);
        Map<String, String> fieldsMap = JsonUtils.toStringMap(jsonNodes);
        String[] fieldsName = new String[fieldsMap.size()];
        SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType<?>[fieldsMap.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : fieldsMap.entrySet()) {
            fieldsName[i] = entry.getKey();
            seaTunnelDataTypes[i] = deserializeSeaTunnelDataType(entry.getValue());
            i++;
        }
        return new SeaTunnelRowType(fieldsName, seaTunnelDataTypes);
    }

    private static SeaTunnelDataType<?> parseMapType(String columnStr) {
        String genericType = getGenericType(columnStr);
        int index =
                genericType.startsWith(SqlType.DECIMAL.name())
                        ?
                        // if map key is decimal, we should find the index of second ','
                        genericType.indexOf(",", genericType.indexOf(",") + 1)
                        :
                        // if map key is not decimal, we should find the index of first ','
                        genericType.indexOf(",");
        String keyGenericType = genericType.substring(0, index);
        String valueGenericType = genericType.substring(index + 1);
        return new MapType<>(
                deserializeSeaTunnelDataType(keyGenericType),
                deserializeSeaTunnelDataType(valueGenericType));
    }

    private static String getGenericType(String columnStr) {
        // get the content between '<' and '>'
        return columnStr.substring(columnStr.indexOf("<") + 1, columnStr.lastIndexOf(">"));
    }

    private static SeaTunnelDataType<?> parseArrayType(String columnStr) {
        String genericType = getGenericType(columnStr);
        SeaTunnelDataType<?> dataType = deserializeSeaTunnelDataType(genericType);
        switch (dataType.getSqlType()) {
            case STRING:
                return ArrayType.STRING_ARRAY_TYPE;
            case BOOLEAN:
                return ArrayType.BOOLEAN_ARRAY_TYPE;
            case TINYINT:
                return ArrayType.BYTE_ARRAY_TYPE;
            case SMALLINT:
                return ArrayType.SHORT_ARRAY_TYPE;
            case INT:
                return ArrayType.INT_ARRAY_TYPE;
            case BIGINT:
                return ArrayType.LONG_ARRAY_TYPE;
            case FLOAT:
                return ArrayType.FLOAT_ARRAY_TYPE;
            case DOUBLE:
                return ArrayType.DOUBLE_ARRAY_TYPE;
            default:
                String errorMsg =
                        String.format("Array type not support this genericType [%s]", genericType);
                throw new UnsupportedOperationException(errorMsg);
        }
    }

    private static SeaTunnelDataType<?> parseDecimalType(String columnStr) {
        String[] decimalInfos = columnStr.split(",");
        if (decimalInfos.length < 2) {
            throw new RuntimeException(
                    "Decimal type should assign precision and scale information");
        }
        int precision = Integer.parseInt(decimalInfos[0].replaceAll("\\D", ""));
        int scale = Integer.parseInt(decimalInfos[1].replaceAll("\\D", ""));
        return new DecimalType(precision, scale);
    }
}
