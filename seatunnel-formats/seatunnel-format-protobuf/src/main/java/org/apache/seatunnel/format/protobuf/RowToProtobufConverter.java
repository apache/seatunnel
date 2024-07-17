/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.protobuf.exception.ProtobufFormatErrorCode;
import org.apache.seatunnel.format.protobuf.exception.SeaTunnelProtobufFormatException;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class RowToProtobufConverter implements Serializable {

    private static final long serialVersionUID = -576124379280229724L;

    private final Descriptors.Descriptor descriptor;
    private final SeaTunnelRowType rowType;
    public RowToProtobufConverter(SeaTunnelRowType rowType, Descriptors.FileDescriptor[] fileDescriptors
    ,String protobufMessageName ) {
        this.rowType = rowType;
        descriptor = fileDescriptors[0].findMessageTypeByName(protobufMessageName);
    }


    public byte[] convertRowToGenericRecord(SeaTunnelRow element) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        String[] fieldNames = rowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = rowType.getFieldName(i);
            Object value = element.getField(i);
            Object o = resolveObject(fieldName, value, rowType.getFieldType(i),builder);
            if(o!=null) {
                builder.setField(descriptor.findFieldByName(fieldName.toLowerCase()), o);
            }
        }
        return builder.build().toByteArray();
    }

    private Object resolveObject(String field,Object data, SeaTunnelDataType<?> seaTunnelDataType,DynamicMessage.Builder builder2) {
        if (data == null) {
            return null;
        }
        switch (seaTunnelDataType.getSqlType()) {
            case STRING:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case DECIMAL:
            case DATE:
            case TIMESTAMP:
                return data;
            case TINYINT:
                Class<?> typeClass = seaTunnelDataType.getTypeClass();
                if (typeClass == Byte.class) {
                    if (data instanceof Byte) {
                        Byte aByte = (Byte) data;
                        return Byte.toUnsignedInt(aByte);
                    }
                }
                return data;
            case BYTES:
                return ByteBuffer.wrap((byte[]) data);
            case MAP:
                Descriptors.Descriptor mapEntryDescriptor = descriptor.findFieldByName(field).getMessageType();
                Class<?> mapTypeClass = seaTunnelDataType.getTypeClass();
                if (mapTypeClass == Map.class && data instanceof Map){
                    Map newData = (Map)data;
                    newData.forEach((x,y)->{
                        DynamicMessage mapEntry2 = DynamicMessage.newBuilder(mapEntryDescriptor)
                                .setField(mapEntryDescriptor.findFieldByName("key"),x )
                                .setField(mapEntryDescriptor.findFieldByName("value"), y)
                                .build();
                        builder2.addRepeatedField(descriptor.findFieldByName(field),mapEntry2);
                    });
                }
                return null;
            case ARRAY:
                return Arrays.asList((Object[])data);
            case ROW:
                SeaTunnelRow seaTunnelRow = (SeaTunnelRow) data;
                SeaTunnelDataType<?>[] fieldTypes =
                        ((SeaTunnelRowType) seaTunnelDataType).getFieldTypes();
                String[] fieldNames = ((SeaTunnelRowType) seaTunnelDataType).getFieldNames();
                Descriptors.Descriptor nestedTypeByName = descriptor.findNestedTypeByName(field);
                DynamicMessage.Builder builder = DynamicMessage.newBuilder(nestedTypeByName);
                for (int i = 0; i < fieldNames.length; i++) {
                    builder.setField(nestedTypeByName.findFieldByName(fieldNames[i])
                            ,resolveObject(fieldNames[i]
                                    ,seaTunnelRow.getField(i)
                                    , fieldTypes[i],builder));
                }
                return builder.build();
            default:
                String errorMsg =
                        String.format(
                                "SeaTunnel protobuf format is not supported for this data type [%s]",
                                seaTunnelDataType.getSqlType());
                throw new SeaTunnelProtobufFormatException(
                        ProtobufFormatErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }
}
