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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.utils.JsonUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class SeaTunnelSchema implements Serializable {

    public static final Option<Schema> SCHEMA = Options.key("schema").objectType(Schema.class).noDefaultValue().withDescription("SeaTunnel Schema");
    private static final String FIELD_KEY = "fields";
    private static final String SIMPLE_SCHEMA_FILED = "content";
    private final SeaTunnelRowType seaTunnelRowType;

    private SeaTunnelSchema(SeaTunnelRowType seaTunnelRowType) {
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
        int index;
        if (genericType.startsWith(SqlType.DECIMAL.name())) {
            // if map key is decimal, we should find the index of second ','
            index = genericType.indexOf(",");
            index = genericType.indexOf(",", index + 1);
        } else {
            // if map key is not decimal, we should find the index of first ','
            index = genericType.indexOf(",");
        }
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
        String originContent = type;
        if (type.contains("{") || type.contains("}")) {
            // Row type
            type = SqlType.ROW.name();
        }
        if (type.contains("<") || type.contains(">")) {
            // Map type or Array type
            if (type.startsWith(SqlType.MAP.name())) {
                String[] genericTypes = parseMapGeneric(type);
                keyGenericType = genericTypes[0];
                valueGenericType = genericTypes[1];
                type = SqlType.MAP.name();
            } else if (type.startsWith(SqlType.ARRAY.name())) {
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
                switch(dataType.getSqlType()) {
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
                        String errorMsg = String.format("Array type not support this genericType [%s]", genericType);
                        throw new UnsupportedOperationException(errorMsg);
                }
            case MAP:
                return new MapType<>(parseTypeByString(keyGenericType), parseTypeByString(valueGenericType));
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
                return mapToSeaTunnelRowType(convertJsonToMap(originContent));
        }
    }

    private static Map<String, String> convertConfigToMap(Config config) {
        // Because the entrySet in typesafe config couldn't keep key-value order
        // So use jackson parsing schema information into a map to keep key-value order
        ConfigRenderOptions options = ConfigRenderOptions.concise();
        String schema = config.root().render(options);
        return convertJsonToMap(schema);
    }

    private static Map<String, String> convertJsonToMap(String json) {
        ObjectNode jsonNodes = JsonUtils.parseObject(json);
        LinkedHashMap<String, String> fieldsMap = new LinkedHashMap<>();
        jsonNodes.fields().forEachRemaining(field -> {
            String key = field.getKey();
            JsonNode value = field.getValue();
            if (value.getNodeType() == JsonNodeType.OBJECT) {
                fieldsMap.put(key, value.toString());
            } else {
                fieldsMap.put(key, value.textValue());
            }
        });
        return fieldsMap;
    }

    private static SeaTunnelRowType mapToSeaTunnelRowType(Map<String, String> fieldsMap) {
        int fieldsNum = fieldsMap.size();
        int i = 0;
        String[] fieldsName = new String[fieldsNum];
        SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType<?>[fieldsNum];
        for (Map.Entry<String, String> entry : fieldsMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            SeaTunnelDataType<?> dataType = parseTypeByString(value);
            fieldsName[i] = key;
            seaTunnelDataTypes[i] = dataType;
            i++;
        }
        return new SeaTunnelRowType(fieldsName, seaTunnelDataTypes);
    }

    public static SeaTunnelSchema buildWithConfig(Config schemaConfig) {
        CheckResult checkResult = CheckConfigUtil.checkAllExists(schemaConfig, FIELD_KEY);
        if (!checkResult.isSuccess()) {
            String errorMsg = String.format("Schema config need option [%s], please correct your config first", FIELD_KEY);
            throw new RuntimeException(errorMsg);
        }
        Config fields = schemaConfig.getConfig(FIELD_KEY);
        Map<String, String> fieldsMap = convertConfigToMap(fields);
        return new SeaTunnelSchema(mapToSeaTunnelRowType(fieldsMap));
    }

    public static SeaTunnelRowType buildSimpleTextSchema() {
        return new SeaTunnelRowType(new String[]{SIMPLE_SCHEMA_FILED}, new SeaTunnelDataType<?>[]{BasicType.STRING_TYPE});
    }

    public SeaTunnelRowType getSeaTunnelRowType() {
        return seaTunnelRowType;
    }
}
