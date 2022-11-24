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

package org.apache.seatunnel.connectors.seatunnel.mongodb.data;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.NonNull;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class DefaultSerializer implements Serializer {

    private final SeaTunnelRowType rowType;

    public DefaultSerializer(@NonNull SeaTunnelRowType rowType) {
        DataTypeValidator.validateDataType(rowType);
        this.rowType = rowType;
    }

    @Override
    public Document serialize(@NonNull SeaTunnelRow row) {
        return convert(rowType, row);
    }

    private static Document convert(SeaTunnelRowType rowType, SeaTunnelRow row) {
        Document document = new Document();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String fieldName = rowType.getFieldName(i);
            SeaTunnelDataType<?> fieldType = rowType.getFieldType(i);
            Object fieldValue = row.getField(i);
            document.append(fieldName, convert(fieldType, fieldValue));
        }
        return document;
    }

    private static Object convert(SeaTunnelDataType<?> fieldType, Object fieldValue) {
        if (fieldValue == null) {
            return null;
        }
        switch (fieldType.getSqlType()) {
            case TINYINT:
            case SMALLINT:
                Number number = (Number) fieldValue;
                return number.intValue();
            case FLOAT:
                Float floatValue = (Float) fieldValue;
                return Double.parseDouble(String.valueOf(floatValue));
            case DECIMAL:
                BigDecimal bigDecimal = (BigDecimal) fieldValue;
                return new Decimal128(bigDecimal);
            case DATE:
                LocalDate localDate = (LocalDate) fieldValue;
                return Date.from(localDate.atStartOfDay(ZoneOffset.UTC).toInstant());
            case TIMESTAMP:
                LocalDateTime localDateTime = (LocalDateTime) fieldValue;
                return new BsonTimestamp(localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());
            case BYTES:
                byte[] bytes = (byte[]) fieldValue;
                return new Binary(bytes);
            case ARRAY:
                ArrayType arrayType = (ArrayType) fieldType;
                Object[] array = (Object[]) fieldValue;
                List<Object> listValues = new ArrayList();
                for (Object item : array) {
                    listValues.add(convert(arrayType.getElementType(), item));
                }
                return listValues;
            case MAP:
                MapType mapType = (MapType) fieldType;
                Map<String, Object> map = (Map) fieldValue;
                Document mapDocument = new Document();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    String mapKeyName = entry.getKey();
                    mapDocument.append(mapKeyName,
                        convert(mapType.getValueType(), entry.getValue()));
                }
                return mapDocument;
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) fieldType;
                SeaTunnelRow row = (SeaTunnelRow) fieldValue;
                Document rowDocument = new Document();
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    String rowFieldName = rowType.getFieldName(i);
                    SeaTunnelDataType rowFieldType = rowType.getFieldType(i);
                    Object rowValue = row.getField(i);
                    rowDocument.append(rowFieldName, convert(rowFieldType, rowValue));
                }
                return rowDocument;
            default:
                return fieldValue;
        }
    }
}
