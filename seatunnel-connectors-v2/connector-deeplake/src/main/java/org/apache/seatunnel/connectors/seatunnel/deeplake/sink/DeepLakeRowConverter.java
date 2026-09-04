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

package org.apache.seatunnel.connectors.seatunnel.deeplake.sink;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.VectorUtils;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

final class DeepLakeRowConverter {

    private DeepLakeRowConverter() {}

    static List<Object> convert(SeaTunnelRow row, SeaTunnelRowType rowType) {
        List<Object> values = new ArrayList<>(rowType.getTotalFields());
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            values.add(convertValue(row.getField(i), rowType.getFieldType(i)));
        }
        return values;
    }

    private static Object convertValue(Object value, SeaTunnelDataType<?> type) {
        if (value == null) {
            return null;
        }
        switch (type.getSqlType()) {
            case BYTES:
                return Base64.getEncoder().encodeToString((byte[]) value);
            case BINARY_VECTOR:
                return Base64.getEncoder().encodeToString(readBytes((ByteBuffer) value));
            case FLOAT_VECTOR:
                ByteBuffer vectorBuffer = ((ByteBuffer) value).duplicate();
                vectorBuffer.rewind();
                Float[] vector = VectorUtils.toFloatArray(vectorBuffer);
                List<Float> vectorValues = new ArrayList<>(vector.length);
                for (Float item : vector) {
                    vectorValues.add(item);
                }
                return vectorValues;
            case ARRAY:
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) type).getElementType();
                int length = Array.getLength(value);
                List<Object> arrayValues = new ArrayList<>(length);
                for (int i = 0; i < length; i++) {
                    arrayValues.add(convertValue(Array.get(value, i), elementType));
                }
                return arrayValues;
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_TZ:
                return value.toString();
            default:
                return value;
        }
    }

    private static byte[] readBytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        copy.rewind();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
