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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;

import java.util.LinkedHashMap;

public class HiveTypeConvertor {

    public static SeaTunnelDataType<?> covertHiveTypeToSeaTunnelType(String name, String hiveType) {
        if (hiveType.contains("varchar")) {
            return BasicType.STRING_TYPE;
        }
        if (hiveType.contains("char")) {
            throw CommonError.convertToSeaTunnelTypeError(
                    HiveConstants.CONNECTOR_NAME, PluginType.SOURCE, hiveType, name);
        }
        if (hiveType.contains("binary")) {
            return PrimitiveByteArrayType.INSTANCE;
        }
        if (hiveType.contains("struct")) {
            LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
            int start = hiveType.indexOf("<");
            int end = hiveType.lastIndexOf(">");
            String[] columns = hiveType.substring(start + 1, end).split(",");
            for (String column : columns) {
                String[] splits = column.split(":");
                fields.put(
                        splits[0], covertHiveTypeToSeaTunnelType(splits[0], splits[1]).toString());
            }
            return SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                    name, JsonUtils.toJsonString(fields));
        }
        return SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(name, hiveType);
    }

    /**
     * Convert SeaTunnel data type to Hive type
     *
     * @param seaTunnelType SeaTunnel data type
     * @return Hive type string
     */
    public static String seatunnelToHiveType(SeaTunnelDataType<?> seaTunnelType) {
        switch (seaTunnelType.getSqlType()) {
            case STRING:
                return "string";
            case BOOLEAN:
                return "boolean";
            case TINYINT:
                return "tinyint"; // byte in ORC
            case SMALLINT:
                return "smallint"; // short in ORC
            case INT:
                return "int";
            case BIGINT:
                return "bigint"; // long in ORC
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case DECIMAL:
                if (seaTunnelType instanceof DecimalType) {
                    DecimalType decimalType = (DecimalType) seaTunnelType;
                    return String.format(
                            "decimal(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
                }
                return "decimal(38,18)"; // 设置默认精度
            case BYTES:
                return "binary";
            case DATE:
                // Hive doesn't have native DATE type, use STRING with date format
                return "string";
            case TIME:
                // Hive doesn't have native TIME type, use STRING with time format
                return "string";
            case TIMESTAMP:
                // Use TIMESTAMP for timestamp data with timezone handling
                return "timestamp";
            case ROW:
                return "struct"; // SeaTunnel ROW -> Hive STRUCT
            case ARRAY:
                return "array"; // SeaTunnel ARRAY -> Hive ARRAY
            case MAP:
                return "map"; // SeaTunnel MAP -> Hive MAP
            case NULL:
                throw new UnsupportedOperationException("Orc does not support NULL type");
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported type conversion from %s to Hive ORC type",
                                seaTunnelType.getSqlType()));
        }
    }

    /**
     * Convert SeaTunnel data value to Hive compatible value with timezone handling This method uses
     * HiveTimezoneUtils for proper timezone conversion
     */
    public static Object convertDataValue(Object value, SeaTunnelDataType<?> seaTunnelType) {
        if (value == null) {
            return null;
        }

        switch (seaTunnelType.getSqlType()) {
            case DATE:
                // Convert date to string format 'YYYY-MM-DD'
                if (value instanceof java.time.LocalDate) {
                    return value.toString(); // LocalDate.toString() returns 'YYYY-MM-DD'
                } else if (value instanceof java.util.Date) {
                    return new java.sql.Date(((java.util.Date) value).getTime()).toString();
                }
                return value.toString();

            case TIME:
                // Convert time to string format 'HH:mm:ss'
                if (value instanceof java.time.LocalTime) {
                    return value.toString(); // LocalTime.toString() returns 'HH:mm:ss'
                }
                return value.toString();

            case TIMESTAMP:
                // Use HiveTimezoneUtils for proper timezone conversion
                try {
                    return HiveTimezoneUtils.convertToOffsetDateTime(value);
                } catch (Exception e) {
                    // Fallback to original value if conversion fails
                    return value;
                }

            default:
                // For other types, return as-is
                return value;
        }
    }

    /** Get column comment with timezone information for temporal types */
    public static String getColumnCommentWithTimezone(
            String originalComment, SeaTunnelDataType<?> seaTunnelType) {
        if (originalComment == null) {
            originalComment = "";
        }

        switch (seaTunnelType.getSqlType()) {
            case DATE:
                return originalComment.isEmpty()
                        ? "Date stored as string (YYYY-MM-DD)"
                        : originalComment + " (Date format: YYYY-MM-DD)";
            case TIME:
                return originalComment.isEmpty()
                        ? "Time stored as string (HH:mm:ss)"
                        : originalComment + " (Time format: HH:mm:ss)";
            case TIMESTAMP:
                String timezone = java.time.ZoneId.systemDefault().getId();
                return originalComment.isEmpty()
                        ? "Timestamp with system timezone: " + timezone
                        : originalComment + " (Timezone: " + timezone + ")";
            default:
                return originalComment;
        }
    }
}
