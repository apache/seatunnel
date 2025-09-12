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

package org.apache.seatunnel.common.utils;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class VectorUtils {

    public static ByteBuffer toByteBuffer(Short[] shortArray) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(shortArray.length * 2);

        for (Short value : shortArray) {
            byteBuffer.putShort(value);
        }

        // Compatible compilation and running versions are not consistent
        // Flip the buffer to prepare for reading
        ((Buffer) byteBuffer).flip();

        return byteBuffer;
    }

    public static Short[] toShortArray(ByteBuffer byteBuffer) {
        Short[] shortArray = new Short[byteBuffer.capacity() / 2];

        for (int i = 0; i < shortArray.length; i++) {
            shortArray[i] = byteBuffer.getShort();
        }

        return shortArray;
    }

    public static ByteBuffer toByteBuffer(Float[] floatArray) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(floatArray.length * 4);

        for (float value : floatArray) {
            byteBuffer.putFloat(value);
        }

        ((Buffer) byteBuffer).flip();

        return byteBuffer;
    }

    public static Float[] toFloatArray(ByteBuffer byteBuffer) {
        Float[] floatArray = new Float[byteBuffer.capacity() / 4];

        for (int i = 0; i < floatArray.length; i++) {
            floatArray[i] = byteBuffer.getFloat();
        }

        return floatArray;
    }

    public static ByteBuffer toByteBuffer(Double[] doubleArray) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(doubleArray.length * 8);

        for (double value : doubleArray) {
            byteBuffer.putDouble(value);
        }

        ((Buffer) byteBuffer).flip();

        return byteBuffer;
    }

    public static Double[] toDoubleArray(ByteBuffer byteBuffer) {
        Double[] doubleArray = new Double[byteBuffer.capacity() / 8];

        for (int i = 0; i < doubleArray.length; i++) {
            doubleArray[i] = byteBuffer.getDouble();
        }

        return doubleArray;
    }

    public static ByteBuffer toByteBuffer(Integer[] intArray) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(intArray.length * 4);

        for (int value : intArray) {
            byteBuffer.putInt(value);
        }

        ((Buffer) byteBuffer).flip();

        return byteBuffer;
    }

    public static Integer[] toIntArray(ByteBuffer byteBuffer) {
        Integer[] intArray = new Integer[byteBuffer.capacity() / 4];

        for (int i = 0; i < intArray.length; i++) {
            intArray[i] = byteBuffer.getInt();
        }

        return intArray;
    }

    public static Float[] convertSparseVectorToFloatArray(Map<?, ?> sparseVector) {
        if (sparseVector.isEmpty()) {
            return new Float[0];
        }
        int maxIndex = -1;
        for (Map.Entry<?, ?> entry : sparseVector.entrySet()) {
            Object key = entry.getKey();
            if (!(key instanceof Integer)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Sparse vector key must be Integer, but got: %s,",
                                key.getClass().getName()));
            }
            int index = (Integer) key;
            if (index < 0) {
                throw new IllegalArgumentException(
                        String.format("Sparse vector index cannot be negative: %d", index));
            }
            // prevent OOM
            if (index > 1000000) {
                throw new IllegalArgumentException(
                        String.format("Sparse vector index too large: %d", index));
            }
            maxIndex = Math.max(maxIndex, index);
        }
        Float[] denseVector = new Float[maxIndex + 1];
        Arrays.fill(denseVector, 0.0f);
        for (Map.Entry<?, ?> entry : sparseVector.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            if (!(value instanceof Number)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Sparse vector value must be a Number, but got: %s",
                                value.getClass().getName()));
            }
            int index = (Integer) key;
            denseVector[index] = ((Number) value).floatValue();
        }
        return denseVector;
    }
}
