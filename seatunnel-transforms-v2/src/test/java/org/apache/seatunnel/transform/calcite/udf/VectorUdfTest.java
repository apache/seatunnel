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

package org.apache.seatunnel.transform.calcite.udf;

import org.apache.seatunnel.common.utils.VectorUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

class VectorUdfTest {

    @ParameterizedTest
    @ValueSource(
            floats = {Float.MAX_VALUE, 1e20f, 1e-30f, Float.MIN_NORMAL, Float.MIN_VALUE, 3f, 0.1f})
    void testFiniteVectorArithmetic(float value) {
        byte[] left = toBytes(new Float[] {value, -value});
        byte[] right = toBytes(new Float[] {-value, value});
        BigDecimal exact = new BigDecimal((double) value);
        double squaredNorm = exact.multiply(exact).multiply(BigDecimal.valueOf(2)).doubleValue();
        double norm = Math.hypot(value, value);
        double distance = exact.multiply(BigDecimal.valueOf(4)).doubleValue();
        Assertions.assertAll(
                () -> Assertions.assertEquals(norm, VectorNormFunction.eval(left), norm * 1e-14),
                () ->
                        Assertions.assertEquals(
                                -squaredNorm,
                                InnerProductFunction.eval(left, right),
                                squaredNorm * 1e-14),
                () -> Assertions.assertEquals(2.0, CosineDistanceFunction.eval(left, right), 1e-14),
                () ->
                        Assertions.assertEquals(
                                distance, L1DistanceFunction.eval(left, right), distance * 1e-14),
                () ->
                        Assertions.assertEquals(
                                2 * norm, L2DistanceFunction.eval(left, right), norm * 1e-14),
                () -> {
                    Float[] normalized = fromBytes(VectorNormalizeFunction.eval(left));
                    Assertions.assertEquals(1 / Math.sqrt(2), normalized[0], 1e-7);
                    Assertions.assertEquals(-1 / Math.sqrt(2), normalized[1], 1e-7);
                });
    }

    @Test
    void testLargeProductsCancelWithoutOverflow() {
        byte[] left = toBytes(new Float[] {Float.MAX_VALUE, Float.MAX_VALUE});
        byte[] right = toBytes(new Float[] {Float.MAX_VALUE, -Float.MAX_VALUE});
        Assertions.assertEquals(0.0, InnerProductFunction.eval(left, right));
        Assertions.assertEquals(1.0, CosineDistanceFunction.eval(left, right));
    }

    @Test
    void testVectorArithmeticSpecialValues() {
        byte[] zero = toBytes(new Float[] {0f, -0f});
        byte[] empty = toBytes(new Float[] {});
        Assertions.assertEquals(0.0, VectorNormFunction.eval(zero));
        Assertions.assertEquals(1.0, CosineDistanceFunction.eval(zero, zero));
        Assertions.assertEquals(1.0, CosineDistanceFunction.eval(empty, empty));
        Assertions.assertSame(zero, VectorNormalizeFunction.eval(zero));
        Assertions.assertSame(empty, VectorNormalizeFunction.eval(empty));
        Assertions.assertTrue(
                Double.isNaN(
                        CosineDistanceFunction.eval(
                                toBytes(new Float[] {Float.MIN_VALUE}),
                                toBytes(new Float[] {Float.POSITIVE_INFINITY}))));
        for (float value :
                new float[] {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}) {
            byte[] vector = toBytes(new Float[] {value});
            Assertions.assertTrue(Double.isNaN(CosineDistanceFunction.eval(vector, vector)));
            Assertions.assertTrue(Double.isNaN(L1DistanceFunction.eval(vector, vector)));
            Assertions.assertTrue(Double.isNaN(L2DistanceFunction.eval(vector, vector)));
            double norm = VectorNormFunction.eval(vector);
            Assertions.assertTrue(
                    Float.isNaN(value) ? Double.isNaN(norm) : norm == Double.POSITIVE_INFINITY);
            Assertions.assertTrue(Float.isNaN(fromBytes(VectorNormalizeFunction.eval(vector))[0]));
        }
    }

    private static byte[] toBytes(Float[] floats) {
        ByteBuffer buf = VectorUtils.toByteBuffer(floats);
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }

    private static Float[] fromBytes(byte[] bytes) {
        return VectorUtils.toFloatArray(ByteBuffer.wrap(bytes));
    }

    @Test
    void testCosineDistanceIdenticalVectors() {
        byte[] v = toBytes(new Float[] {1.0f, 2.0f, 3.0f});
        Double result = CosineDistanceFunction.eval(v, v);
        Assertions.assertEquals(0.0, result, 1e-9);
    }

    @Test
    void testCosineDistanceOrthogonalVectors() {
        byte[] v1 = toBytes(new Float[] {1.0f, 0.0f});
        byte[] v2 = toBytes(new Float[] {0.0f, 1.0f});
        Double result = CosineDistanceFunction.eval(v1, v2);
        Assertions.assertEquals(1.0, result, 1e-9);
    }

    @Test
    void testCosineDistanceNull() {
        Assertions.assertNull(CosineDistanceFunction.eval(null, toBytes(new Float[] {1.0f})));
        Assertions.assertNull(CosineDistanceFunction.eval(toBytes(new Float[] {1.0f}), null));
    }

    @Test
    void testCosineDistanceDimensionMismatch() {
        byte[] v1 = toBytes(new Float[] {1.0f, 2.0f});
        byte[] v2 = toBytes(new Float[] {1.0f, 2.0f, 3.0f});
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> CosineDistanceFunction.eval(v1, v2));
    }

    @Test
    void testCosineDistanceFunctionName() {
        Assertions.assertEquals("COSINE_DISTANCE", new CosineDistanceFunction().functionName());
    }

    @Test
    void testL1Distance() {
        byte[] v1 = toBytes(new Float[] {2.0f, 4.0f, 6.0f});
        byte[] v2 = toBytes(new Float[] {1.0f, 2.0f, 3.0f});
        Double result = L1DistanceFunction.eval(v1, v2);
        Assertions.assertEquals(6.0, result, 1e-9);
    }

    @Test
    void testL1DistanceNull() {
        Assertions.assertNull(L1DistanceFunction.eval(null, toBytes(new Float[] {1.0f})));
        Assertions.assertNull(L1DistanceFunction.eval(toBytes(new Float[] {1.0f}), null));
    }

    @Test
    void testL1DistanceFunctionName() {
        Assertions.assertEquals("L1_DISTANCE", new L1DistanceFunction().functionName());
    }

    @Test
    void testL2Distance() {
        byte[] v1 = toBytes(new Float[] {2.0f, 4.0f, 4.0f});
        byte[] v2 = toBytes(new Float[] {1.0f, 2.0f, 2.0f});
        Double result = L2DistanceFunction.eval(v1, v2);
        Assertions.assertEquals(3.0, result, 1e-9);
    }

    @Test
    void testL2DistanceNull() {
        Assertions.assertNull(L2DistanceFunction.eval(null, toBytes(new Float[] {1.0f})));
        Assertions.assertNull(L2DistanceFunction.eval(toBytes(new Float[] {1.0f}), null));
    }

    @Test
    void testL2DistanceFunctionName() {
        Assertions.assertEquals("L2_DISTANCE", new L2DistanceFunction().functionName());
    }

    @Test
    void testVectorDims() {
        byte[] v = toBytes(new Float[] {1.0f, 2.0f, 3.0f});
        Assertions.assertEquals(3, VectorDimsFunction.eval(v));
    }

    @Test
    void testVectorDimsNull() {
        Assertions.assertNull(VectorDimsFunction.eval(null));
    }

    @Test
    void testVectorDimsFunctionName() {
        Assertions.assertEquals("VECTOR_DIMS", new VectorDimsFunction().functionName());
    }

    @Test
    void testVectorNorm() {
        byte[] v = toBytes(new Float[] {1.0f, 2.0f, 2.0f});
        Double result = VectorNormFunction.eval(v);
        Assertions.assertEquals(3.0, result, 1e-9);
    }

    @Test
    void testVectorNormNull() {
        Assertions.assertNull(VectorNormFunction.eval(null));
    }

    @Test
    void testVectorNormFunctionName() {
        Assertions.assertEquals("VECTOR_NORM", new VectorNormFunction().functionName());
    }

    @Test
    void testInnerProduct() {
        byte[] v1 = toBytes(new Float[] {1.0f, 2.0f, 3.0f});
        byte[] v2 = toBytes(new Float[] {7.0f, 8.0f, 9.0f});
        Double result = InnerProductFunction.eval(v1, v2);
        Assertions.assertEquals(50.0, result, 1e-9);
    }

    @Test
    void testInnerProductNull() {
        Assertions.assertNull(InnerProductFunction.eval(null, toBytes(new Float[] {1.0f})));
        Assertions.assertNull(InnerProductFunction.eval(toBytes(new Float[] {1.0f}), null));
    }

    @Test
    void testInnerProductFunctionName() {
        Assertions.assertEquals("INNER_PRODUCT", new InnerProductFunction().functionName());
    }

    @Test
    void testVectorReduceTruncate() {
        byte[] v = toBytes(new Float[] {1.0f, 2.0f, 3.0f, 4.0f});
        byte[] result = VectorReduceFunction.eval(v, 2, "TRUNCATE");
        Float[] reduced = fromBytes(result);
        Assertions.assertArrayEquals(new Float[] {1.0f, 2.0f}, reduced);
    }

    @Test
    void testVectorReduceNoTruncateNeeded() {
        byte[] v = toBytes(new Float[] {1.0f, 2.0f});
        byte[] result = VectorReduceFunction.eval(v, 10, "TRUNCATE");
        Assertions.assertSame(v, result);
    }

    @Test
    void testVectorReduceRandomProjection() {
        byte[] v = toBytes(new Float[] {1.0f, 2.0f, 3.0f, 4.0f});
        byte[] result = VectorReduceFunction.eval(v, 2, "RANDOM_PROJECTION");
        Float[] reduced = fromBytes(result);
        Assertions.assertEquals(2, reduced.length);
    }

    @Test
    void testVectorReduceSparseProjection() {
        byte[] v = toBytes(new Float[] {1.0f, 2.0f, 3.0f, 4.0f});
        byte[] result = VectorReduceFunction.eval(v, 2, "SPARSE_RANDOM_PROJECTION");
        Float[] reduced = fromBytes(result);
        Assertions.assertEquals(2, reduced.length);
    }

    @Test
    void testVectorReduceNull() {
        Assertions.assertNull(VectorReduceFunction.eval(null, 2, "TRUNCATE"));
        byte[] v = toBytes(new Float[] {1.0f});
        Assertions.assertNull(VectorReduceFunction.eval(v, null, "TRUNCATE"));
        Assertions.assertNull(VectorReduceFunction.eval(v, 2, null));
    }

    @Test
    void testVectorReduceUnknownMethod() {
        byte[] v = toBytes(new Float[] {1.0f, 2.0f, 3.0f, 4.0f});
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> VectorReduceFunction.eval(v, 2, "UNKNOWN"));
    }

    @Test
    void testVectorReduceFunctionName() {
        Assertions.assertEquals("VECTOR_REDUCE", new VectorReduceFunction().functionName());
    }

    @Test
    void testVectorNormalize() {
        byte[] v = toBytes(new Float[] {3.0f, 4.0f});
        byte[] result = VectorNormalizeFunction.eval(v);
        Float[] normalized = fromBytes(result);
        Assertions.assertEquals(2, normalized.length);
        double norm = Math.sqrt(normalized[0] * normalized[0] + normalized[1] * normalized[1]);
        Assertions.assertEquals(1.0, norm, 1e-6);
    }

    @Test
    void testVectorNormalizeZeroVector() {
        byte[] v = toBytes(new Float[] {0.0f, 0.0f});
        byte[] result = VectorNormalizeFunction.eval(v);
        Assertions.assertSame(v, result);
    }

    @Test
    void testVectorNormalizeNull() {
        Assertions.assertNull(VectorNormalizeFunction.eval(null));
    }

    @Test
    void testVectorNormalizeFunctionName() {
        Assertions.assertEquals("VECTOR_NORMALIZE", new VectorNormalizeFunction().functionName());
    }
}
