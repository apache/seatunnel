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

package org.apache.seatunnel.api.table.type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ArrayTypeTest {

    @Test
    void testSingleDimensionArray() {
        ArrayType<String[], String> stringArrayType = ArrayType.of(BasicType.STRING_TYPE);
        Assertions.assertEquals(1, stringArrayType.getDimensions());
        Assertions.assertEquals(BasicType.STRING_TYPE, stringArrayType.getElementType());
        Assertions.assertEquals(String[].class, stringArrayType.getTypeClass());
    }

    @Test
    void testMultiDimensionArray() {
        ArrayType<?, ?> nestedArrayType1 = ArrayType.of(ArrayType.of(BasicType.STRING_TYPE));
        Assertions.assertEquals(2, nestedArrayType1.getDimensions());
        Assertions.assertEquals(String[].class, nestedArrayType1.getElementType().getTypeClass());
        Assertions.assertEquals(String[][].class, nestedArrayType1.getTypeClass());

        ArrayType<String[][], String[]> nestedArrayType2 =
                ArrayType.of(ArrayType.STRING_ARRAY_TYPE);
        Assertions.assertEquals(2, nestedArrayType2.getDimensions());
        Assertions.assertEquals(String[].class, nestedArrayType2.getElementType().getTypeClass());
        Assertions.assertEquals(String[][].class, nestedArrayType2.getTypeClass());

        ArrayType<String[][], String[]> nestedArrayType3 =
                new ArrayType(String[][].class, ArrayType.STRING_ARRAY_TYPE);
        Assertions.assertEquals(2, nestedArrayType3.getDimensions());
        Assertions.assertEquals(String[].class, nestedArrayType3.getElementType().getTypeClass());
        Assertions.assertEquals(String[][].class, nestedArrayType3.getTypeClass());
    }

    @Test
    void testMultiDimensionArrayWithDimensions() {
        ArrayType<?, ?> threeDimensionArrayType = ArrayType.of(BasicType.STRING_TYPE, 3);
        Assertions.assertEquals(3, threeDimensionArrayType.getDimensions());
        Assertions.assertEquals(
                String[][].class, threeDimensionArrayType.getElementType().getTypeClass());
        Assertions.assertEquals(String[][][].class, threeDimensionArrayType.getTypeClass());

        ArrayType<?, ?> fourDimensionArrayType = ArrayType.of(BasicType.STRING_TYPE, 4);
        Assertions.assertEquals(4, fourDimensionArrayType.getDimensions());
        Assertions.assertEquals(
                String[][][].class, fourDimensionArrayType.getElementType().getTypeClass());
        Assertions.assertEquals(String[][][][].class, fourDimensionArrayType.getTypeClass());
    }

    @Test
    void testAddDimension() {
        ArrayType<String[], String> stringArrayType = ArrayType.of(BasicType.STRING_TYPE);
        ArrayType<?, ?> multiDimArrayType = stringArrayType.addDimension();
        Assertions.assertEquals(2, multiDimArrayType.getDimensions());
        Assertions.assertEquals(String[].class, multiDimArrayType.getElementType().getTypeClass());
        Assertions.assertEquals(String[][].class, multiDimArrayType.getTypeClass());
    }

    @Test
    void testToString() {
        ArrayType<String[], String> stringArrayType = ArrayType.of(BasicType.STRING_TYPE);
        Assertions.assertEquals("ARRAY<STRING>", stringArrayType.toString());

        ArrayType<?, ?> nestedArrayType = ArrayType.of(BasicType.STRING_TYPE, 2);
        Assertions.assertEquals("ARRAY<ARRAY<STRING>>", nestedArrayType.toString());

        ArrayType<?, ?> nestedArrayType2 = ArrayType.of(BasicType.STRING_TYPE, 3);
        Assertions.assertEquals("ARRAY<ARRAY<ARRAY<STRING>>>", nestedArrayType2.toString());
    }
}
