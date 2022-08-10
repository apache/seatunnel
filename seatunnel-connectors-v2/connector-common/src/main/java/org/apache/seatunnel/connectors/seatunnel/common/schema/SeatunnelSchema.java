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

package org.apache.seatunnel.connectors.seatunnel.common.schema;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import java.util.Map;

public class SeatunnelSchema {
    private static final String FIELD_KEY = "fields";
    private final SeaTunnelRowType seaTunnelRowType;

    private SeatunnelSchema(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    private static String[] parseMapGeneric(String type) {
        int start = type.indexOf("<");
        int end = type.lastIndexOf(">");
        String genericType = type
                // get the content between '<' and '>'
                .substring(start + 1, end)
                // replace the space between key and value
                .replace(" ", "");
        int index = genericType.indexOf(",");
        String keyGenericType = genericType.substring(0, index);
        String valueGenericType = genericType.substring(index + 1);
        return new String[]{keyGenericType, valueGenericType};
    }

    private static String parseArrayGeneric(String type) {
        int start = type.indexOf("<");
        int end = type.lastIndexOf(">");
        return type
                // get the content between '<' and '>'
                .substring(start + 1, end)
                // replace the space between key and value
                .replace(" ", "");
    }

    private static int[] parseDecimalPS(String type) {
        int start = type.indexOf("(");
        int end = type.lastIndexOf(")");
        String decimalInfo = type
                // get the content between '(' and ')'
                .substring(start + 1, end)
                // replace the space between precision and scale
                .replace(" ", "");
        String[] split = decimalInfo.split(",");
        if (split.length < 2) {
            throw new RuntimeException("Decimal type should assign precision and scale information");
        }
        int precision = Integer.parseInt(split[0]);
        int scale = Integer.parseInt(split[1]);
        return new int[]{precision, scale};
    }

    private static SeaTunnelDataType<?> parseTypeByString(String type) {
        // init precision (used by decimal type)
        int precision = 0;
        // init scale (used by decimal type)
        int scale = 0;
        // init generic type (used by array type)
        String genericType = "";
        // init key generic type (used by map type)
        String keyGenericType = "";
        // init value generic type (used by map type)
        String valueGenericType = "";
        // convert type to uppercase
        type = type.toUpperCase();
        if (type.contains("<") || type.contains(">")) {
            // Map type or Array type
            if (type.contains(SqlType.MAP.name())) {
                String[] genericTypes = parseMapGeneric(type);
                keyGenericType = genericTypes[0];
                valueGenericType = genericTypes[1];
                type = SqlType.MAP.name();
            } else {
                genericType = parseArrayGeneric(type);
                type = SqlType.ARRAY.name();
            }
        }
        if (type.contains("(")) {
            // Decimal type
            int[] results = parseDecimalPS(type);
            precision = results[0];
            scale = results[1];
            type = SqlType.DECIMAL.name();
        }
        SqlType sqlType;
        try {
            sqlType = SqlType.valueOf(type);
        } catch (IllegalArgumentException e) {
            String errorMsg = String.format("Field type not support [%s], currently only support [array, map, string, boolean, tinyint, smallint, int, bigint, float, double, decimal, null, bytes, date, time, timestamp]", type.toUpperCase());
            throw new RuntimeException(errorMsg);
        }
        switch (sqlType) {
            case ARRAY:
                SeaTunnelDataType<?> dataType = parseTypeByString(genericType);
                if (BasicType.STRING_TYPE.equals(dataType)) {
                    return ArrayType.STRING_ARRAY_TYPE;
                } else if (BasicType.BOOLEAN_TYPE.equals(dataType)) {
                    return ArrayType.BOOLEAN_ARRAY_TYPE;
                } else if (BasicType.BYTE_TYPE.equals(dataType)) {
                    return ArrayType.BYTE_ARRAY_TYPE;
                } else if (BasicType.SHORT_TYPE.equals(dataType)) {
                    return ArrayType.SHORT_ARRAY_TYPE;
                } else if (BasicType.INT_TYPE.equals(dataType)) {
                    return ArrayType.INT_ARRAY_TYPE;
                } else if (BasicType.LONG_TYPE.equals(dataType)) {
                    return ArrayType.LONG_ARRAY_TYPE;
                } else if (BasicType.FLOAT_TYPE.equals(dataType)) {
                    return ArrayType.FLOAT_ARRAY_TYPE;
                } else if (BasicType.DOUBLE_TYPE.equals(dataType)) {
                    return ArrayType.DOUBLE_ARRAY_TYPE;
                } else {
                    String errorMsg = String.format("Array type not support this genericType [%s]", genericType);
                    throw new RuntimeException(errorMsg);
                }
            case MAP:
                return new MapType<>(parseTypeByString(keyGenericType), parseTypeByString(valueGenericType));
            case STRING:
                return BasicType.STRING_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case TINYINT:
            case BYTES:
                return BasicType.BYTE_TYPE;
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
            case DECIMAL:
                return new DecimalType(precision, scale);
            case NULL:
                return BasicType.VOID_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                throw new RuntimeException("Not support [row] type now");
        }
    }

    public static SeatunnelSchema buildWithConfig(Config schemaConfig) {
        CheckResult checkResult = CheckConfigUtil.checkAllExists(schemaConfig, FIELD_KEY);
        if (!checkResult.isSuccess()) {
            String errorMsg = String.format("Schema config need option [%s], please correct your config first", FIELD_KEY);
            throw new RuntimeException(errorMsg);
        }
        Config fields = schemaConfig.getConfig(FIELD_KEY);
        int fieldsNum = fields.entrySet().size();
        int i = 0;
        String[] fieldsName = new String[fieldsNum];
        SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType<?>[fieldsNum];
        for (Map.Entry<String, ConfigValue> entry: fields.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue().unwrapped().toString();
            SeaTunnelDataType<?> dataType = parseTypeByString(value);
            fieldsName[i] = key;
            seaTunnelDataTypes[i] = dataType;
            i++;
        }
        SeaTunnelRowType seaTunnelRowType = new SeaTunnelRowType(fieldsName, seaTunnelDataTypes);
        return new SeatunnelSchema(seaTunnelRowType);
    }

    public SeaTunnelRowType getSeaTunnelRowType() {
        return seaTunnelRowType;
    }
}
