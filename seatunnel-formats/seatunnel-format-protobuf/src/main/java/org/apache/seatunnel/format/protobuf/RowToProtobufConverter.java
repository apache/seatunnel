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
        String[] fieldNames = rowType.getFieldNames();

        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = rowType.getFieldName(i);
            Object value = element.getField(i);
            Object resolvedValue =
                    resolveObject(fieldName, value, rowType.getFieldType(i), builder);
            if (resolvedValue != null) {
                if (resolvedValue instanceof byte[]) {
                    resolvedValue = ByteString.copyFrom((byte[]) resolvedValue);
                }
                builder.setField(
                        descriptor.findFieldByName(fieldName.toLowerCase()), resolvedValue);
            }
        }

        return builder.build().toByteArray();
    }

    private Object resolveObject(
            String fieldName,
            Object data,
            SeaTunnelDataType<?> seaTunnelDataType,
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
                return handleMapType(fieldName, data, seaTunnelDataType, builder);
            case ARRAY:
                return Arrays.asList((Object[]) data);
            case ROW:
                return handleRowType(fieldName, data, seaTunnelDataType);
            default:
                throw new SeaTunnelProtobufFormatException(
                        ProtobufFormatErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format(
                                "SeaTunnel protobuf format is not supported for this data type [%s]",
                                seaTunnelDataType.getSqlType()));
        }
    }

    private Object handleMapType(
            String fieldName,
            Object data,
            SeaTunnelDataType<?> seaTunnelDataType,
            DynamicMessage.Builder builder) {
        Descriptors.Descriptor mapEntryDescriptor =
                descriptor.findFieldByName(fieldName).getMessageType();

        if (data instanceof Map) {
            Map<?, ?> mapData = (Map<?, ?>) data;
            mapData.forEach(
                    (key, value) -> {
                        DynamicMessage mapEntry =
                                DynamicMessage.newBuilder(mapEntryDescriptor)
                                        .setField(mapEntryDescriptor.findFieldByName("key"), key)
                                        .setField(
                                                mapEntryDescriptor.findFieldByName("value"), value)
                                        .build();
                        builder.addRepeatedField(descriptor.findFieldByName(fieldName), mapEntry);
                    });
        }

        return null;
    }

    private Object handleRowType(
            String fieldName, Object data, SeaTunnelDataType<?> seaTunnelDataType) {
        SeaTunnelRow seaTunnelRow = (SeaTunnelRow) data;
        SeaTunnelDataType<?>[] fieldTypes = ((SeaTunnelRowType) seaTunnelDataType).getFieldTypes();
        String[] fieldNames = ((SeaTunnelRowType) seaTunnelDataType).getFieldNames();
        Descriptors.Descriptor nestedTypeDescriptor = descriptor.findNestedTypeByName(fieldName);
        DynamicMessage.Builder nestedBuilder = DynamicMessage.newBuilder(nestedTypeDescriptor);

        for (int i = 0; i < fieldNames.length; i++) {
            Object resolvedValue =
                    resolveObject(
                            fieldNames[i], seaTunnelRow.getField(i), fieldTypes[i], nestedBuilder);
            nestedBuilder.setField(
                    nestedTypeDescriptor.findFieldByName(fieldNames[i]), resolvedValue);
        }

        return nestedBuilder.build();
    }
}
