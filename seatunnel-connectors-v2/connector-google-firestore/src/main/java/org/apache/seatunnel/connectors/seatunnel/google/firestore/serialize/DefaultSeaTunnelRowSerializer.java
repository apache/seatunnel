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

package org.apache.seatunnel.connectors.seatunnel.google.firestore.serialize;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultSeaTunnelRowSerializer implements SeaTunnelRowSerializer {

    private final SeaTunnelRowType seaTunnelRowType;

    public DefaultSeaTunnelRowSerializer(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public Map<String, Object> serialize(SeaTunnelRow seaTunnelRow) {
        Map<String, Object> data = new HashMap<>();
        for (int index = 0; index < seaTunnelRowType.getFieldNames().length; index++) {
            SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(index);
            Object fieldValue = seaTunnelRow.getField(index);
            data.put(seaTunnelRowType.getFieldName(index), convert(fieldType, fieldValue));
        }
        return data;
    }

    private static Object convert(SeaTunnelDataType<?> seaTunnelDataType, Object fieldValue) {
        if (fieldValue == null) {
            return null;
        }
        switch (seaTunnelDataType.getSqlType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
                return ((Number) fieldValue).intValue();
            case BIGINT:
                return ((Number) fieldValue).longValue();
            case FLOAT:
                Float floatValue = (Float) fieldValue;
                return Double.parseDouble(String.valueOf(floatValue));
            case DOUBLE:
                return Double.parseDouble(String.valueOf(fieldValue));
            case DECIMAL:
                BigDecimal bigDecimal = (BigDecimal) fieldValue;
                return bigDecimal;
            case STRING:
                return String.valueOf(fieldValue);
            case BOOLEAN:
                return Boolean.parseBoolean(String.valueOf(fieldValue));
            case BYTES:
                return Blob.fromBytes((byte[]) fieldValue);
            case DATE:
                LocalDate localDate = (LocalDate) fieldValue;
                return Date.from(localDate.atStartOfDay(ZoneOffset.UTC).toInstant());
            case TIMESTAMP:
                LocalDateTime localDateTime = (LocalDateTime) fieldValue;
                return Timestamp.of(Date.from(localDateTime.toInstant(ZoneOffset.UTC)));
            case ARRAY:
                ArrayType arrayType = (ArrayType) seaTunnelDataType;
                Object[] array = (Object[]) fieldValue;
                List<Object> listValues = new ArrayList();
                for (Object item : array) {
                    listValues.add(convert(arrayType.getElementType(), item));
                }
                return listValues;
            case MAP:
                MapType mapType = (MapType) seaTunnelDataType;
                Map<String, Object> map = (Map) fieldValue;
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    String mapKeyName = entry.getKey();
                    map.put(mapKeyName, convert(mapType.getValueType(), entry.getValue()));
                }
                return map;
            default:
                return fieldValue;
        }
    }
}
