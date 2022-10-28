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

package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class DefaultDeserializer implements Deserializer {

    @NonNull
    private final SeaTunnelRowType seaTunnelRowType;
    @NonNull
    private final Schema icebergSchema;

    @Override
    public SeaTunnelRow deserialize(@NonNull Record record) {
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(seaTunnelRowType.getTotalFields());
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            String seaTunnelFieldName = seaTunnelRowType.getFieldName(i);
            SeaTunnelDataType<?> seaTunnelFieldType = seaTunnelRowType.getFieldType(i);
            Types.NestedField icebergField = icebergSchema.findField(seaTunnelFieldName);
            Object icebergValue = record.getField(seaTunnelFieldName);

            seaTunnelRow.setField(i, convert(icebergField.type(), icebergValue, seaTunnelFieldType));
        }
        return seaTunnelRow;
    }

    private Object convert(@NonNull Type icebergType,
                           Object icebergValue,
                           @NonNull SeaTunnelDataType<?> seaTunnelType) {
        if (icebergValue == null) {
            return null;
        }
        switch (icebergType.typeId()) {
            case BOOLEAN:
                return Boolean.class.cast(icebergValue);
            case INTEGER:
                return Integer.class.cast(icebergValue);
            case LONG:
                return Long.class.cast(icebergValue);
            case FLOAT:
                return Float.class.cast(icebergValue);
            case DOUBLE:
                return Double.class.cast(icebergValue);
            case DATE:
                return LocalDate.class.cast(icebergValue);
            case TIME:
                return LocalTime.class.cast(icebergValue);
            case TIMESTAMP:
                Types.TimestampType timestampType = (Types.TimestampType) icebergType;
                if (timestampType.shouldAdjustToUTC()) {
                    return OffsetDateTime.class.cast(icebergValue).toLocalDateTime();
                }
                return LocalDateTime.class.cast(icebergValue);
            case STRING:
                return String.class.cast(icebergValue);
            case FIXED:
                return byte[].class.cast(icebergValue);
            case BINARY:
                return ByteBuffer.class.cast(icebergValue).array();
            case DECIMAL:
                return BigDecimal.class.cast(icebergValue);
            case STRUCT:
                Record icebergStruct = Record.class.cast(icebergValue);
                Types.StructType icebergStructType = (Types.StructType) icebergType;
                SeaTunnelRowType seaTunnelRowType = (SeaTunnelRowType) seaTunnelType;
                SeaTunnelRow seatunnelRow = new SeaTunnelRow(seaTunnelRowType.getTotalFields());
                for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
                    String seatunnelFieldName = seaTunnelRowType.getFieldName(i);
                    Object seatunnelFieldValue = convert(icebergStructType.fieldType(seatunnelFieldName),
                        icebergStruct.getField(seatunnelFieldName),
                        seaTunnelRowType.getFieldType(i));
                    seatunnelRow.setField(i, seatunnelFieldValue);
                }
                return seatunnelRow;
            case LIST:
                List icebergList = List.class.cast(icebergValue);
                Types.ListType icebergListType = (Types.ListType) icebergType;
                List seatunnelList = new ArrayList(icebergList.size());
                ArrayType seatunnelListType = (ArrayType) seaTunnelType;
                for (int i = 0; i < icebergList.size(); i++) {
                    seatunnelList.add(convert(icebergListType.elementType(),
                        icebergList.get(i), seatunnelListType.getElementType()));
                }
                return seatunnelList.toArray();
            case MAP:
                Map<Object, Object> icebergMap = Map.class.cast(icebergValue);
                Types.MapType icebergMapType = (Types.MapType) icebergType;
                Map seatunnelMap = new HashMap();
                MapType seatunnelMapType = (MapType) seaTunnelType;
                for (Map.Entry entry : icebergMap.entrySet()) {
                    seatunnelMap.put(
                        convert(icebergMapType.keyType(), entry.getKey(), seatunnelMapType.getKeyType()),
                        convert(icebergMapType.valueType(), entry.getValue(), seatunnelMapType.getValueType()));
                }
                return seatunnelMap;
            default:
                throw new UnsupportedOperationException("Unsupported iceberg type: " + icebergType);
        }
    }
}
