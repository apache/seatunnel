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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.protobuf.exception.ProtobufFormatErrorCode;
import org.apache.seatunnel.format.protobuf.exception.SeaTunnelProtobufFormatException;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

public class RowToProtobufConverter implements Serializable {

    private static final long serialVersionUID = -576124379280229724L;
    private final Descriptors.Descriptor descriptor;
    private final SeaTunnelRowType rowType;

    public RowToProtobufConverter(SeaTunnelRowType rowType, Descriptors.Descriptor descriptor) {
        this.rowType = rowType;
        this.descriptor = descriptor;
    }

    public byte[] convertRowToGenericRecord(SeaTunnelRow element) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            resolveAndSetField(
                    rowType.getFieldName(i), element.getField(i), rowType.getFieldType(i), builder);
        }

        return builder.build().toByteArray();
    }

    /** Resolves each field against the message that owns it, including nested rows and maps. */
    private void resolveAndSetField(
            String fieldName,
            Object value,
            SeaTunnelDataType<?> seaTunnelDataType,
            DynamicMessage.Builder builder) {
        if (value == null) {
            return;
        }

        Descriptors.Descriptor messageDescriptor = builder.getDescriptorForType();
        Descriptors.FieldDescriptor fieldDescriptor =
                ProtobufFieldResolver.findField(messageDescriptor, fieldName);
        if (fieldDescriptor == null) {
            throw new SeaTunnelProtobufFormatException(
                    ProtobufFormatErrorCode.FIELD_NOT_FOUND,
                    String.format(
                            "Field [%s] is not defined in the protobuf message [%s].",
                            fieldName, messageDescriptor.getFullName()));
        }

        Object resolvedValue = resolveObject(value, seaTunnelDataType, fieldDescriptor, builder);
        if (resolvedValue != null) {
            if (resolvedValue instanceof byte[]) {
                resolvedValue = ByteString.copyFrom((byte[]) resolvedValue);
            }
            builder.setField(fieldDescriptor, resolvedValue);
        }
    }

    private Object resolveObject(
            Object data,
            SeaTunnelDataType<?> seaTunnelDataType,
            Descriptors.FieldDescriptor fieldDescriptor,
            DynamicMessage.Builder builder) {
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
            case BYTES:
                return data;
            case TINYINT:
                if (data instanceof Byte) {
                    return Byte.toUnsignedInt((Byte) data);
                }
                return data;
            case MAP:
                return handleMapType(data, fieldDescriptor, builder);
            case ARRAY:
                return Arrays.asList((Object[]) data);
            case ROW:
                return handleRowType(data, seaTunnelDataType, fieldDescriptor);
            default:
                throw new SeaTunnelProtobufFormatException(
                        ProtobufFormatErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format(
                                "SeaTunnel protobuf format is not supported for this data type [%s]",
                                seaTunnelDataType.getSqlType()));
        }
    }

    /** Adds map entries directly to the owning message builder. */
    private Object handleMapType(
            Object data,
            Descriptors.FieldDescriptor fieldDescriptor,
            DynamicMessage.Builder builder) {
        if (data instanceof Map) {
            Descriptors.Descriptor mapEntryDescriptor = fieldDescriptor.getMessageType();
            Descriptors.FieldDescriptor keyFieldDescriptor =
                    mapEntryDescriptor.findFieldByName("key");
            Descriptors.FieldDescriptor valueFieldDescriptor =
                    mapEntryDescriptor.findFieldByName("value");
            ((Map<?, ?>) data)
                    .forEach(
                            (key, value) -> {
                                DynamicMessage mapEntry =
                                        DynamicMessage.newBuilder(mapEntryDescriptor)
                                                .setField(keyFieldDescriptor, key)
                                                .setField(valueFieldDescriptor, value)
                                                .build();
                                builder.addRepeatedField(fieldDescriptor, mapEntry);
                            });
        }

        return null;
    }

    /** Uses the field descriptor to determine the nested message type. */
    private Object handleRowType(
            Object data,
            SeaTunnelDataType<?> seaTunnelDataType,
            Descriptors.FieldDescriptor fieldDescriptor) {
        SeaTunnelRow seaTunnelRow = (SeaTunnelRow) data;
        SeaTunnelRowType nestedRowType = (SeaTunnelRowType) seaTunnelDataType;
        DynamicMessage.Builder nestedBuilder =
                DynamicMessage.newBuilder(fieldDescriptor.getMessageType());

        for (int i = 0; i < nestedRowType.getTotalFields(); i++) {
            resolveAndSetField(
                    nestedRowType.getFieldName(i),
                    seaTunnelRow.getField(i),
                    nestedRowType.getFieldType(i),
                    nestedBuilder);
        }

        return nestedBuilder.build();
    }
}
