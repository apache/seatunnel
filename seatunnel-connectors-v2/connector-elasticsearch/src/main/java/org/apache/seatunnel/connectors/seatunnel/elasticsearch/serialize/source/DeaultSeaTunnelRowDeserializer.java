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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.source;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class DeaultSeaTunnelRowDeserializer implements SeaTunnelRowDeserializer {

    private final SeaTunnelRowType rowTypeInfo;

    private final DateTimeFormatter esDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ObjectMapper mapper = new ObjectMapper();

    public DeaultSeaTunnelRowDeserializer(SeaTunnelRowType rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
    }

    @Override
    public SeaTunnelRow deserialize(ElasticsearchRecord rowRecord) {
        return convert(rowRecord);
    }

    SeaTunnelRow convert(ElasticsearchRecord rowRecord) {
        Object[] seaTunnelFields = new Object[rowTypeInfo.getTotalFields()];

        for (int i = 0; i < rowTypeInfo.getTotalFields(); i++) {
            String fieldName = rowTypeInfo.getFieldName(i);
            Object value = rowRecord.getDoc().get(fieldName);
            if (value != null) {
                seaTunnelFields[i] = convertValue(rowTypeInfo.getFieldType(i), String.valueOf(value));
            }
        }
        return new SeaTunnelRow(seaTunnelFields);
    }

    Object convertValue(SeaTunnelDataType<?> seaTunnelFieldType, String fieldValue) {
        switch (seaTunnelFieldType.getSqlType()) {
            case ARRAY:
                return JsonUtils.toList(fieldValue, String.class).toArray();
            case MAP:
                Map<String, Object> map = mapper.convertValue(fieldValue, new TypeReference<Map<String, Object>>() {
                });
                return map;
            case STRING:
                return fieldValue;
            case BOOLEAN:
                return Boolean.parseBoolean(fieldValue);
            case TINYINT:
                return Byte.parseByte(fieldValue);
            case SMALLINT:
                return Short.parseShort(fieldValue);
            case INT:
                return Integer.parseInt(fieldValue);
            case BIGINT:
                return Long.parseLong(fieldValue);
            case FLOAT:
                return Float.parseFloat(fieldValue);
            case DOUBLE:
                return Double.parseDouble(fieldValue);
            case BYTES:
                return fieldValue.getBytes();
            case DATE:
                LocalDateTime localDateTime = LocalDateTime.parse(fieldValue.replace("T", " "), esDateFormatter);
                return localDateTime.toLocalDate();
            case TIME:
            case TIMESTAMP:
                localDateTime = LocalDateTime.parse(fieldValue.replace("T", " "), esDateFormatter);
                return localDateTime;
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + seaTunnelFieldType);
        }
    }
}

