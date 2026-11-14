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

package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.VectorUtils;
import org.apache.seatunnel.transform.sql.SQLEngine;
import org.apache.seatunnel.transform.sql.SQLEngineFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VectorFunctionTest {

    @ParameterizedTest
    @ValueSource(strings = {"RANDOM_PROJECTION", "SPARSE_RANDOM_PROJECTION"})
    public void testProjectionDoesNotDependOnCallOrder(String method) {
        Float[] source = {1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f};
        Float[] expected = project(source, 4, method);
        for (int i = 0; i < 5; i++) {
            project(new Float[] {8f, 7f, 6f, 5f, 4f, 3f, 2f, 1f}, 3, method);
            project(source, 2, "RANDOM_PROJECTION");
            project(source, 3, "SPARSE_RANDOM_PROJECTION");
            Assertions.assertArrayEquals(expected, project(source, 4, method));
        }
        // A projection must use the same linear map for different input rows.
        Float[] doubled = Arrays.stream(source).map(value -> value * 2).toArray(Float[]::new);
        Float[] doubledExpected =
                Arrays.stream(expected).map(value -> value * 2).toArray(Float[]::new);
        Assertions.assertArrayEquals(doubledExpected, project(doubled, 4, method));
    }

    @ParameterizedTest
    @ValueSource(strings = {"RANDOM_PROJECTION", "SPARSE_RANDOM_PROJECTION"})
    public void testProjectionMatchesSeededReference(String method) {
        Float[] source = {1f, -2f, 0f, 4f, 5f, -6f, 7f, 8f};
        Random rng = new Random(42L);
        Float[] expected = new Float[4];
        for (int i = 0; i < expected.length; i++) {
            float sum = 0;
            for (Float value : source) {
                float coefficient;
                if ("RANDOM_PROJECTION".equals(method)) {
                    coefficient = (float) rng.nextGaussian() * (float) Math.sqrt(1.0 / 4);
                } else {
                    double sample = rng.nextDouble();
                    coefficient =
                            sample < 1.0 / 6
                                    ? (float) Math.sqrt(3.0)
                                    : sample < 2.0 / 6 ? -(float) Math.sqrt(3.0) : 0f;
                }
                if (coefficient != 0) {
                    sum += value * coefficient;
                }
            }
            expected[i] = sum;
        }
        Assertions.assertArrayEquals(expected, project(source, 4, method));
    }

    @ParameterizedTest
    @ValueSource(strings = {"RANDOM_PROJECTION", "SPARSE_RANDOM_PROJECTION"})
    public void testProjectionIsStableAcrossThreads(String method) throws Exception {
        Float[] source = {1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f};
        Float[] expected = project(source, 4, method);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<Float[]>> results = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                results.add(executor.submit(() -> project(source, 4, method)));
            }
            for (Future<Float[]> result : results) {
                Assertions.assertArrayEquals(expected, result.get(10, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"RANDOM_PROJECTION", "SPARSE_RANDOM_PROJECTION"})
    public void testProjectionIsStableAcrossProcesses(String method) throws Exception {
        String classpath =
                System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path"));
        Process process =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "java")
                                        .toString(),
                                "-cp",
                                classpath,
                                ProjectionProcessProbe.class.getName(),
                                method)
                        .redirectError(ProcessBuilder.Redirect.INHERIT)
                        .start();
        try {
            Assertions.assertTrue(process.waitFor(30, TimeUnit.SECONDS));
            String output;
            try (BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    process.getInputStream(), StandardCharsets.UTF_8))) {
                output = reader.lines().collect(Collectors.joining("\n")).trim();
            }
            Assertions.assertEquals(0, process.exitValue(), output);
            // Advance the parent process independently before comparing it with the fresh JVM.
            project(new Float[] {1f, 2f, 3f, 4f, 5f}, 2, method);
            Assertions.assertEquals(
                    output, Arrays.toString(project(new Float[] {1f, 2f, 3f, 4f}, 2, method)));
        } finally {
            process.destroyForcibly();
            Assertions.assertTrue(process.waitFor(10, TimeUnit.SECONDS));
        }
    }

    private static Float[] project(Float[] input, int dimension, String method) {
        return VectorUtils.toFloatArray(
                (ByteBuffer)
                        VectorFunction.vectorReduce(
                                VectorUtils.toByteBuffer(input), dimension, method));
    }

    /** Executes the projection in a fresh JVM, without relying on the test runner's call order. */
    public static class ProjectionProcessProbe {
        public static void main(String[] args) {
            System.err.println("Projection probe initialized");
            System.out.println(Arrays.toString(project(new Float[] {1f, 2f, 3f, 4f}, 2, args[0])));
        }
    }

    @ParameterizedTest
    @ValueSource(
            floats = {Float.MAX_VALUE, 1e20f, 1e-30f, Float.MIN_NORMAL, Float.MIN_VALUE, 3f, 0.1f})
    public void testFiniteVectorArithmetic(float value) {
        Float[] left = {value, -value};
        Float[] right = {-value, value};
        BigDecimal exact = new BigDecimal((double) value);
        double squaredNorm = exact.multiply(exact).multiply(BigDecimal.valueOf(2)).doubleValue();
        double norm = Math.hypot(value, value);
        double distance = exact.multiply(BigDecimal.valueOf(4)).doubleValue();
        Assertions.assertAll(
                () ->
                        Assertions.assertEquals(
                                norm,
                                (double) VectorFunction.vectorNorm(Collections.singletonList(left)),
                                norm * 1e-14),
                () ->
                        Assertions.assertEquals(
                                -squaredNorm,
                                (double) VectorFunction.innerProduct(Arrays.asList(left, right)),
                                squaredNorm * 1e-14),
                () ->
                        Assertions.assertEquals(
                                2.0,
                                (double) VectorFunction.cosineDistance(Arrays.asList(left, right)),
                                1e-14),
                () ->
                        Assertions.assertEquals(
                                distance,
                                (double) VectorFunction.l1Distance(Arrays.asList(left, right)),
                                distance * 1e-14),
                () ->
                        Assertions.assertEquals(
                                2 * norm,
                                (double) VectorFunction.l2Distance(Arrays.asList(left, right)),
                                norm * 1e-14),
                () -> {
                    Float[] normalized =
                            VectorUtils.toFloatArray(
                                    (ByteBuffer) VectorFunction.vectorNormalize(left));
                    Assertions.assertEquals(1 / Math.sqrt(2), normalized[0], 1e-7);
                    Assertions.assertEquals(-1 / Math.sqrt(2), normalized[1], 1e-7);
                });
    }

    @Test
    public void testLargeProductsCancelWithoutOverflow() {
        Float[] left = {Float.MAX_VALUE, Float.MAX_VALUE};
        Float[] right = {Float.MAX_VALUE, -Float.MAX_VALUE};
        Assertions.assertEquals(0.0, VectorFunction.innerProduct(Arrays.asList(left, right)));
        Assertions.assertEquals(1.0, VectorFunction.cosineDistance(Arrays.asList(left, right)));
    }

    @Test
    public void testVectorArithmeticSpecialValues() {
        Float[] zero = {0f, -0f};
        Float[] empty = {};
        Assertions.assertEquals(0.0, VectorFunction.vectorNorm(Collections.singletonList(zero)));
        Assertions.assertEquals(1.0, VectorFunction.cosineDistance(Arrays.asList(zero, zero)));
        Assertions.assertEquals(1.0, VectorFunction.cosineDistance(Arrays.asList(empty, empty)));
        Assertions.assertSame(zero, VectorFunction.vectorNormalize(zero));
        Assertions.assertSame(empty, VectorFunction.vectorNormalize(empty));
        Assertions.assertTrue(
                Double.isNaN(
                        (double)
                                VectorFunction.cosineDistance(
                                        Arrays.asList(
                                                new Float[] {Float.MIN_VALUE},
                                                new Float[] {Float.POSITIVE_INFINITY}))));
        Assertions.assertNull(VectorFunction.vectorNorm(Collections.singletonList(null)));
        Assertions.assertNull(VectorFunction.cosineDistance(Arrays.asList(zero, null)));
        Assertions.assertNull(VectorFunction.innerProduct(Arrays.asList(null, zero)));
        Assertions.assertNull(VectorFunction.l1Distance(Arrays.asList(zero, null)));
        Assertions.assertNull(VectorFunction.l2Distance(Arrays.asList(null, zero)));
        for (float value :
                new float[] {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}) {
            Float[] vector = {value};
            Assertions.assertTrue(
                    Double.isNaN(
                            (double) VectorFunction.cosineDistance(Arrays.asList(vector, vector))));
            Assertions.assertTrue(
                    Double.isNaN(
                            (double) VectorFunction.l1Distance(Arrays.asList(vector, vector))));
            Assertions.assertTrue(
                    Double.isNaN(
                            (double) VectorFunction.l2Distance(Arrays.asList(vector, vector))));
            double norm = (double) VectorFunction.vectorNorm(Collections.singletonList(vector));
            Assertions.assertTrue(
                    Float.isNaN(value) ? Double.isNaN(norm) : norm == Double.POSITIVE_INFINITY);
            Float[] normalized =
                    VectorUtils.toFloatArray((ByteBuffer) VectorFunction.vectorNormalize(vector));
            Assertions.assertTrue(Float.isNaN(normalized[0]));
        }
    }

    @ParameterizedTest
    @MethodSource("vectorBuffers")
    public void testVectorReadsPreserveBuffer(ByteBuffer buffer) {
        ByteOrder order = buffer.order();
        int position = buffer.position();
        int limit = buffer.limit();
        buffer.mark();
        for (int i = 0; i < 2; i++) {
            Assertions.assertEquals(
                    2, VectorFunction.vectorDims(Collections.singletonList(buffer)));
            Assertions.assertEquals(
                    5.0, VectorFunction.vectorNorm(Collections.singletonList(buffer)));
            Assertions.assertEquals(
                    0.0, VectorFunction.cosineDistance(Arrays.asList(buffer, buffer)));
            Assertions.assertEquals(0.0, VectorFunction.l1Distance(Arrays.asList(buffer, buffer)));
            Assertions.assertEquals(0.0, VectorFunction.l2Distance(Arrays.asList(buffer, buffer)));
            Assertions.assertEquals(
                    25.0, VectorFunction.innerProduct(Arrays.asList(buffer, buffer)));
            Assertions.assertArrayEquals(
                    new Float[] {3.0f},
                    VectorUtils.toFloatArray(
                            (ByteBuffer) VectorFunction.vectorTruncate(buffer, 1)));
            Assertions.assertArrayEquals(
                    new Float[] {0.6f, 0.8f},
                    VectorUtils.toFloatArray((ByteBuffer) VectorFunction.vectorNormalize(buffer)));
            Assertions.assertEquals(
                    1,
                    VectorUtils.toFloatArray(
                                    (ByteBuffer) VectorFunction.vectorRandomProjection(buffer, 1))
                            .length);
            Assertions.assertEquals(
                    1,
                    VectorUtils.toFloatArray(
                                    (ByteBuffer) VectorFunction.vectorSparseProjection(buffer, 1))
                            .length);
            Assertions.assertSame(buffer, VectorFunction.vectorTruncate(buffer, 2));
            Assertions.assertSame(buffer, VectorFunction.vectorRandomProjection(buffer, 2));
            Assertions.assertSame(buffer, VectorFunction.vectorSparseProjection(buffer, 2));
            Assertions.assertEquals(position, buffer.position());
            Assertions.assertEquals(limit, buffer.limit());
            Assertions.assertEquals(order, buffer.order());
            Assertions.assertEquals(position, buffer.reset().position());
            Assertions.assertArrayEquals(
                    new Float[] {3.0f, 4.0f},
                    VectorUtils.toFloatArray(buffer.duplicate().order(order)));
        }
    }

    private static Stream<ByteBuffer> vectorBuffers() {
        return Stream.of(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN)
                .flatMap(
                        order -> {
                            ByteBuffer heap = ByteBuffer.allocate(8).order(order);
                            heap.putFloat(0, 3.0f).putFloat(4, 4.0f);
                            ByteBuffer direct = ByteBuffer.allocateDirect(8).order(order);
                            direct.putFloat(0, 3.0f).putFloat(4, 4.0f);
                            ByteBuffer parent = ByteBuffer.allocate(16).order(order);
                            parent.putFloat(4, 3.0f).putFloat(8, 4.0f);
                            parent.position(4);
                            parent.limit(12);
                            ByteBuffer offset = ByteBuffer.allocate(9).order(order);
                            offset.putFloat(1, 3.0f).putFloat(5, 4.0f);
                            offset.position(1);
                            return Stream.of(
                                    heap,
                                    direct,
                                    heap.asReadOnlyBuffer().order(order),
                                    parent.slice().order(order),
                                    offset);
                        });
    }

    @Test
    public void testFailedVectorReadPreservesBuffer() {
        ByteBuffer buffer = VectorUtils.toByteBuffer(new Float[] {1.0f, 3.0f, 4.0f});
        buffer.position(4);
        buffer.mark();
        Assertions.assertThrows(
                BufferUnderflowException.class,
                () -> VectorFunction.vectorNorm(Collections.singletonList(buffer)));
        Assertions.assertEquals(4, buffer.position());
        Assertions.assertEquals(12, buffer.limit());
        Assertions.assertEquals(4, buffer.reset().position());
        buffer.position(0);
        buffer.limit(8);
        buffer.mark();
        Assertions.assertThrows(
                BufferUnderflowException.class,
                () -> VectorFunction.vectorNorm(Collections.singletonList(buffer)));
        Assertions.assertEquals(0, buffer.position());
        Assertions.assertEquals(8, buffer.limit());
        Assertions.assertEquals(0, buffer.reset().position());
    }

    @Test
    public void testZeroVectorNormalizationPreservesBuffer() {
        ByteBuffer buffer = VectorUtils.toByteBuffer(new Float[] {0.0f, 0.0f});
        Assertions.assertSame(buffer, VectorFunction.vectorNormalize(buffer));
        Assertions.assertEquals(2, VectorFunction.vectorDims(Collections.singletonList(buffer)));
        Assertions.assertEquals(0, buffer.position());
    }

    @Test
    public void testCosineDistanceFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"vector_float1", "vector_float2"},
                        new SeaTunnelDataType[] {
                            VectorType.VECTOR_FLOAT_TYPE, VectorType.VECTOR_SPARSE_FLOAT_TYPE
                        });
        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            VectorUtils.toByteBuffer(new Float[] {1.0f, 2.0f, 3.0f}),
                            VectorUtils.toByteBuffer(new Float[] {1.0f, 2.0f, 3.0f})
                        });

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select COSINE_DISTANCE(vector_float1, vector_float2) as cosineDistance from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object f1Object = outRow.getField(0);
        Assertions.assertEquals(0.0, f1Object);
    }

    @Test
    public void testL1DistanceFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"vector_float1", "vector_float2"},
                        new SeaTunnelDataType[] {
                            VectorType.VECTOR_FLOAT_TYPE, VectorType.VECTOR_FLOAT_TYPE
                        });
        HashMap<Integer, Float> sparseVector = Maps.newHashMap();
        sparseVector.put(0, 1.0f);
        sparseVector.put(1, 2.0f);
        sparseVector.put(2, 3.0f);
        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            VectorUtils.toByteBuffer(new Float[] {2.0f, 4.0f, 6.0f}), sparseVector
                        });

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select L1_DISTANCE(vector_float1, vector_float2) as l1Distance from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object f1Object = outRow.getField(0);
        Assertions.assertEquals(6.0, f1Object);
    }

    @Test
    public void testL2DistanceFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"vector_float1", "vector_float2"},
                        new SeaTunnelDataType[] {
                            VectorType.VECTOR_FLOAT_TYPE, VectorType.VECTOR_FLOAT_TYPE
                        });

        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            VectorUtils.toByteBuffer(new Float[] {2.0f, 4.0f, 4.0f}),
                            VectorUtils.toByteBuffer(new Float[] {1.0f, 2.0f, 2.0f})
                        });

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select L2_DISTANCE(vector_float1, vector_float2) as l2Distance from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object f1Object = outRow.getField(0);
        Assertions.assertEquals(3.0, f1Object);
    }

    @Test
    public void testVectorNormFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"vector_float1", "vector_float2"},
                        new SeaTunnelDataType[] {
                            VectorType.VECTOR_FLOAT_TYPE, VectorType.VECTOR_FLOAT_TYPE
                        });

        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            VectorUtils.toByteBuffer(new Float[] {1.0f, 2.0f, 2.0f}),
                            VectorUtils.toByteBuffer(new Float[] {1.0f, 2.0f, 3.0f})
                        });

        sqlEngine.init(
                "test", null, rowType, "select VECTOR_NORM(vector_float1) as norm from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object f1Object = outRow.getField(0);
        Assertions.assertEquals(3.0, f1Object);
    }

    @Test
    public void testVectorDimsFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"vector_float1"},
                        new SeaTunnelDataType[] {VectorType.VECTOR_FLOAT_TYPE});

        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            VectorUtils.toByteBuffer(new Float[] {1.0f, 2.0f, 3.0f}),
                        });

        sqlEngine.init("test", null, rowType, "select VECTOR_DIMS(vector_float1) as dim from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object f1Object = outRow.getField(0);
        Assertions.assertEquals(3, f1Object);
    }

    @Test
    public void testInnerProductFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"vector_float1", "vector_float2"},
                        new SeaTunnelDataType[] {
                            VectorType.VECTOR_FLOAT_TYPE, VectorType.VECTOR_FLOAT_TYPE
                        });

        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {
                            VectorUtils.toByteBuffer(new Float[] {1.0f, 2.0f, 3.0f}),
                            VectorUtils.toByteBuffer(new Float[] {7.0f, 8.0f, 9.0f})
                        });

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select INNER_PRODUCT(vector_float1, vector_float2) as innerProduct from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object f1Object = outRow.getField(0);
        Assertions.assertEquals(50.0, f1Object);
    }

    @Test
    public void testVectorReduceTruncateRandomAndSparseProjection() {
        Float[] source = new Float[] {1.0f, 2.0f, 3.0f, 4.0f};

        ByteBuffer bufferForTruncate = VectorUtils.toByteBuffer(source);
        Object truncated = VectorFunction.vectorTruncate(bufferForTruncate, 2);
        Float[] truncatedArray = VectorUtils.toFloatArray((ByteBuffer) truncated);
        Assertions.assertArrayEquals(new Float[] {1.0f, 2.0f}, truncatedArray);

        ByteBuffer bufferForNoTruncate = VectorUtils.toByteBuffer(source);
        Object noTruncate = VectorFunction.vectorTruncate(bufferForNoTruncate, 10);
        Assertions.assertSame(bufferForNoTruncate, noTruncate);

        ByteBuffer bufferForRandom = VectorUtils.toByteBuffer(source);
        Object randomProj = VectorFunction.vectorRandomProjection(bufferForRandom, 2);
        Float[] rpArray = VectorUtils.toFloatArray((ByteBuffer) randomProj);
        Assertions.assertEquals(2, rpArray.length);

        ByteBuffer bufferForSparse = VectorUtils.toByteBuffer(source);
        Object sparseProj = VectorFunction.vectorSparseProjection(bufferForSparse, 2);
        Float[] spArray = VectorUtils.toFloatArray((ByteBuffer) sparseProj);
        Assertions.assertEquals(2, spArray.length);

        Assertions.assertNull(VectorFunction.vectorTruncate(null, 2));
        Assertions.assertNull(
                VectorFunction.vectorRandomProjection(VectorUtils.toByteBuffer(source), null));
        Assertions.assertNull(VectorFunction.vectorSparseProjection(null, null));

        ByteBuffer bufferForReduce = VectorUtils.toByteBuffer(source);
        Object reducedTruncate = VectorFunction.vectorReduce(bufferForReduce, 2, "TRUNCATE");
        Float[] rtArray = VectorUtils.toFloatArray((ByteBuffer) reducedTruncate);
        Assertions.assertArrayEquals(new Float[] {1.0f, 2.0f}, rtArray);

        ByteBuffer bufferForReduceRandom = VectorUtils.toByteBuffer(source);
        Object reducedRandom =
                VectorFunction.vectorReduce(bufferForReduceRandom, 2, "RANDOM_PROJECTION");
        Assertions.assertEquals(2, VectorUtils.toFloatArray((ByteBuffer) reducedRandom).length);

        ByteBuffer bufferForReduceSparse = VectorUtils.toByteBuffer(source);
        Object reducedSparse =
                VectorFunction.vectorReduce(bufferForReduceSparse, 2, "SPARSE_RANDOM_PROJECTION");
        Assertions.assertEquals(2, VectorUtils.toFloatArray((ByteBuffer) reducedSparse).length);

        Assertions.assertNull(VectorFunction.vectorReduce(null, 2, "TRUNCATE"));
        Assertions.assertNull(
                VectorFunction.vectorReduce(VectorUtils.toByteBuffer(source), null, "TRUNCATE"));
        Assertions.assertNull(
                VectorFunction.vectorReduce(VectorUtils.toByteBuffer(source), 2, null));

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> VectorFunction.vectorReduce(VectorUtils.toByteBuffer(source), 2, "UNKNOWN"));
    }

    @Test
    public void testVectorNormalize() {
        Float[] source = new Float[] {3.0f, 4.0f};
        ByteBuffer buffer = VectorUtils.toByteBuffer(source);

        Object normalizedObj = VectorFunction.vectorNormalize(buffer);
        Float[] normalized = VectorUtils.toFloatArray((ByteBuffer) normalizedObj);
        Assertions.assertEquals(2, normalized.length);

        double norm = Math.sqrt(normalized[0] * normalized[0] + normalized[1] * normalized[1]);
        Assertions.assertEquals(1.0, norm, 1e-6);

        Float[] zeroVector = new Float[] {0.0f, 0.0f};
        ByteBuffer zeroBuffer = VectorUtils.toByteBuffer(zeroVector);
        Object zeroResult = VectorFunction.vectorNormalize(zeroBuffer);
        Assertions.assertSame(zeroBuffer, zeroResult);

        Assertions.assertNull(VectorFunction.vectorNormalize(null));
    }

    @Test
    public void testVectorReduceMethodIsLocaleIndependent() {
        Locale original = Locale.getDefault();
        try {
            // In tr-TR "random_projection".toUpperCase() is "RANDOM_PROJECT\u0130ON".
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));

            Float[] source = new Float[] {1.0f, 2.0f, 3.0f, 4.0f};
            Object reduced =
                    VectorFunction.vectorReduce(
                            VectorUtils.toByteBuffer(source), 2, "random_projection");
            Assertions.assertEquals(2, VectorUtils.toFloatArray((ByteBuffer) reduced).length);
        } finally {
            Locale.setDefault(original);
        }
    }
}
