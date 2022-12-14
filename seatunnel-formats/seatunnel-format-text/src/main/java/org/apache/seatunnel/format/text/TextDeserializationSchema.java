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

package org.apache.seatunnel.format.text;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.format.text.exception.SeaTunnelTextFormatException;

import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.Builder;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

@Builder
public class TextDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    @NonNull
    private SeaTunnelRowType seaTunnelRowType;
    @NonNull
    private String delimiter;
    @Builder.Default
    private DateUtils.Formatter dateFormatter = DateUtils.Formatter.YYYY_MM_DD;
    @Builder.Default
    private DateTimeUtils.Formatter dateTimeFormatter = DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
    @Builder.Default
    private TimeUtils.Formatter timeFormatter = TimeUtils.Formatter.HH_MM_SS;

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        String content = new String(message);
        Map<Integer, String> splitsMap = splitLineBySeaTunnelRowType(content, seaTunnelRowType);
        Object[] objects = new Object[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = convert(splitsMap.get(i), seaTunnelRowType.getFieldType(i));
        }
        return new SeaTunnelRow(objects);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return seaTunnelRowType;
    }

    private Map<Integer, String> splitLineBySeaTunnelRowType(String line, SeaTunnelRowType seaTunnelRowType) {
        String[] splits = line.split(delimiter, -1);
        LinkedHashMap<Integer, String> splitsMap = new LinkedHashMap<>();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        int cursor = 0;
        for (int i = 0; i < fieldTypes.length; i++) {
            if (fieldTypes[i].getSqlType() == SqlType.ROW) {
                // row type
                int totalFields = ((SeaTunnelRowType) fieldTypes[i]).getTotalFields();
                // if current field is empty
                if (cursor >= splits.length) {
                    splitsMap.put(i, null);
                } else {
                    ArrayList<String> rowSplits = new ArrayList<>(Arrays.asList(splits).subList(cursor, cursor + totalFields));
                    splitsMap.put(i, String.join(delimiter, rowSplits));
                }
                cursor += totalFields;
            } else {
                // not row type
                // if current field is empty
                if (cursor >= splits.length) {
                    splitsMap.put(i, null);
                    cursor++;
                } else {
                    splitsMap.put(i, splits[cursor++]);
                }
            }
        }
        return splitsMap;
    }

    private Object convert(String field, SeaTunnelDataType<?> fieldType) {
        if (StringUtils.isBlank(field)) {
            return null;
        }
        switch (fieldType.getSqlType()) {
            case ARRAY:
                BasicType<?> elementType = ((ArrayType<?, ?>) fieldType).getElementType();
                ArrayNode jsonNodes = JsonUtils.parseArray(field);
                ArrayList<Object> objectArrayList = new ArrayList<>();
                jsonNodes.forEach(jsonNode -> objectArrayList.add(convert(jsonNode.toString(), elementType)));
                switch (elementType.getSqlType()) {
                    case STRING:
                        return objectArrayList.toArray(new String[0]);
                    case BOOLEAN:
                        return objectArrayList.toArray(new Boolean[0]);
                    case TINYINT:
                        return objectArrayList.toArray(new Byte[0]);
                    case SMALLINT:
                        return objectArrayList.toArray(new Short[0]);
                    case INT:
                        return objectArrayList.toArray(new Integer[0]);
                    case BIGINT:
                        return objectArrayList.toArray(new Long[0]);
                    case FLOAT:
                        return objectArrayList.toArray(new Float[0]);
                    case DOUBLE:
                        return objectArrayList.toArray(new Double[0]);
                    default:
                        throw new SeaTunnelTextFormatException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                String.format("SeaTunnel array not support this data type [%s]", elementType.getSqlType()));
                }
            case MAP:
                SeaTunnelDataType<?> keyType = ((MapType<?, ?>) fieldType).getKeyType();
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) fieldType).getValueType();
                LinkedHashMap<Object, Object> objectMap = new LinkedHashMap<>();
                Map<String, String> fieldsMap = JsonUtils.toMap(field);
                fieldsMap.forEach((key, value) -> objectMap.put(convert(key, keyType), convert(value, valueType)));
                return objectMap;
            case STRING:
                return field;
            case BOOLEAN:
                return Boolean.parseBoolean(field);
            case TINYINT:
                return Byte.parseByte(field);
            case SMALLINT:
                return Short.parseShort(field);
            case INT:
                return Integer.parseInt(field);
            case BIGINT:
                return Long.parseLong(field);
            case FLOAT:
                return Float.parseFloat(field);
            case DOUBLE:
                return Double.parseDouble(field);
            case DECIMAL:
                return new BigDecimal(field);
            case NULL:
                return null;
            case BYTES:
                return field.getBytes();
            case DATE:
                return DateUtils.parse(field, dateFormatter);
            case TIME:
                return TimeUtils.parse(field, timeFormatter);
            case TIMESTAMP:
                return DateTimeUtils.parse(field, dateTimeFormatter);
            case ROW:
                Map<Integer, String> splitsMap = splitLineBySeaTunnelRowType(field, (SeaTunnelRowType) fieldType);
                Object[] objects = new Object[splitsMap.size()];
                for (int i = 0; i < objects.length; i++) {
                    objects[i] = convert(splitsMap.get(i), ((SeaTunnelRowType) fieldType).getFieldType(i));
                }
                return new SeaTunnelRow(objects);
            default:
                throw new SeaTunnelTextFormatException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format("SeaTunnel not support this data type [%s]", fieldType.getSqlType()));
        }
    }
}
