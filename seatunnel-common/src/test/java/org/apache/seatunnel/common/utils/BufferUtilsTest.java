/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.seatunnel.common.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class BufferUtilsTest {

    @Test
    public void testToByteBufferAndToShortArray() {
        Short[] shortArray = {1, 2, 3, 4, 5};
        ByteBuffer byteBuffer = BufferUtils.toByteBuffer(shortArray);
        Short[] resultArray = BufferUtils.toShortArray(byteBuffer);

        Assertions.assertArrayEquals(shortArray, resultArray, "Short array conversion failed");
    }

    @Test
    public void testToByteBufferAndToFloatArray() {
        Float[] floatArray = {1.1f, 2.2f, 3.3f, 4.4f, 5.5f};
        ByteBuffer byteBuffer = BufferUtils.toByteBuffer(floatArray);
        Float[] resultArray = BufferUtils.toFloatArray(byteBuffer);

        Assertions.assertArrayEquals(floatArray, resultArray, "Float array conversion failed");
    }

    @Test
    public void testToByteBufferAndToDoubleArray() {
        Double[] doubleArray = {1.1, 2.2, 3.3, 4.4, 5.5};
        ByteBuffer byteBuffer = BufferUtils.toByteBuffer(doubleArray);
        Double[] resultArray = BufferUtils.toDoubleArray(byteBuffer);

        Assertions.assertArrayEquals(doubleArray, resultArray, "Double array conversion failed");
    }

    @Test
    public void testToByteBufferAndToIntArray() {
        Integer[] intArray = {1, 2, 3, 4, 5};
        ByteBuffer byteBuffer = BufferUtils.toByteBuffer(intArray);
        Integer[] resultArray = BufferUtils.toIntArray(byteBuffer);

        Assertions.assertArrayEquals(intArray, resultArray, "Integer array conversion failed");
    }

    @Test
    public void testEmptyArrayConversion() {
        // Test empty arrays
        Short[] shortArray = {};
        ByteBuffer shortBuffer = BufferUtils.toByteBuffer(shortArray);
        Short[] shortResultArray = BufferUtils.toShortArray(shortBuffer);
        Assertions.assertArrayEquals(
                shortArray, shortResultArray, "Empty Short array conversion failed");

        Float[] floatArray = {};
        ByteBuffer floatBuffer = BufferUtils.toByteBuffer(floatArray);
        Float[] floatResultArray = BufferUtils.toFloatArray(floatBuffer);
        Assertions.assertArrayEquals(
                floatArray, floatResultArray, "Empty Float array conversion failed");

        Double[] doubleArray = {};
        ByteBuffer doubleBuffer = BufferUtils.toByteBuffer(doubleArray);
        Double[] doubleResultArray = BufferUtils.toDoubleArray(doubleBuffer);
        Assertions.assertArrayEquals(
                doubleArray, doubleResultArray, "Empty Double array conversion failed");

        Integer[] intArray = {};
        ByteBuffer intBuffer = BufferUtils.toByteBuffer(intArray);
        Integer[] intResultArray = BufferUtils.toIntArray(intBuffer);
        Assertions.assertArrayEquals(
                intArray, intResultArray, "Empty Integer array conversion failed");
    }
}
