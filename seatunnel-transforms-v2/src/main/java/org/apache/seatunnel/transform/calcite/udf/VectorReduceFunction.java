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

import com.google.auto.service.AutoService;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Generic vector dimension reduction function. Supports TRUNCATE, RANDOM_PROJECTION, and
 * SPARSE_RANDOM_PROJECTION methods.
 *
 * <p>Projection matrices are cached per (sourceDimension, targetDimension, method) triple so that
 * all rows within the same job use the same matrix, ensuring mathematically consistent results.
 *
 * <p>Usage: {@code VECTOR_REDUCE(vector_field, target_dimension, method)}
 */
@AutoService(CalciteUdf.class)
public class VectorReduceFunction implements CalciteUdf {

    private static final long PROJECTION_SEED = 42L;

    private static final ConcurrentMap<String, float[][]> MATRIX_CACHE = new ConcurrentHashMap<>();

    @Override
    public String functionName() {
        return "VECTOR_REDUCE";
    }

    public static byte[] eval(byte[] vectorData, Integer targetDimension, String method) {
        if (vectorData == null || targetDimension == null || method == null) {
            return null;
        }
        Float[] source = VectorUtils.toFloatArray(ByteBuffer.wrap(vectorData));
        if (source.length <= targetDimension) {
            return vectorData;
        }

        Float[] result;
        switch (method.toUpperCase()) {
            case "TRUNCATE":
                result = new Float[targetDimension];
                System.arraycopy(source, 0, result, 0, targetDimension);
                break;
            case "RANDOM_PROJECTION":
                result =
                        applyProjection(
                                source,
                                getOrCreateMatrix("GAUSSIAN", source.length, targetDimension),
                                targetDimension);
                break;
            case "SPARSE_RANDOM_PROJECTION":
                result =
                        applyProjection(
                                source,
                                getOrCreateMatrix("SPARSE", source.length, targetDimension),
                                targetDimension);
                break;
            default:
                throw new IllegalArgumentException("Unknown reduction method: " + method);
        }
        ByteBuffer buf = VectorUtils.toByteBuffer(result);
        byte[] out = new byte[buf.remaining()];
        buf.get(out);
        return out;
    }

    private static float[][] getOrCreateMatrix(
            String type, int sourceDimension, int targetDimension) {
        String key = type + ":" + sourceDimension + ":" + targetDimension;
        return MATRIX_CACHE.computeIfAbsent(
                key,
                k -> {
                    Random rng = new Random(PROJECTION_SEED);
                    if ("GAUSSIAN".equals(type)) {
                        return createGaussianProjectionMatrix(
                                rng, sourceDimension, targetDimension);
                    } else {
                        return createSparseProjectionMatrix(rng, sourceDimension, targetDimension);
                    }
                });
    }

    private static Float[] applyProjection(
            Float[] sourceVector, float[][] projectionMatrix, int targetDimension) {
        Float[] result = new Float[targetDimension];
        for (int i = 0; i < targetDimension; i++) {
            float sum = 0.0f;
            for (int j = 0; j < sourceVector.length; j++) {
                if (projectionMatrix[i][j] != 0 && sourceVector[j] != null) {
                    sum += sourceVector[j] * projectionMatrix[i][j];
                }
            }
            result[i] = sum;
        }
        return result;
    }

    private static float[][] createGaussianProjectionMatrix(
            Random rng, int sourceDimension, int targetDimension) {
        float[][] matrix = new float[targetDimension][sourceDimension];
        float scale = (float) Math.sqrt(1.0 / targetDimension);
        for (int i = 0; i < targetDimension; i++) {
            for (int j = 0; j < sourceDimension; j++) {
                matrix[i][j] = (float) rng.nextGaussian() * scale;
            }
        }
        return matrix;
    }

    private static float[][] createSparseProjectionMatrix(
            Random rng, int sourceDimension, int targetDimension) {
        float[][] matrix = new float[targetDimension][sourceDimension];
        float scale = (float) Math.sqrt(3.0);
        double p1 = 1.0 / 6.0;
        double p2 = 2.0 / 6.0;
        for (int i = 0; i < targetDimension; i++) {
            for (int j = 0; j < sourceDimension; j++) {
                double rand = rng.nextDouble();
                if (rand < p1) {
                    matrix[i][j] = scale;
                } else if (rand < p2) {
                    matrix[i][j] = -scale;
                } else {
                    matrix[i][j] = 0;
                }
            }
        }
        return matrix;
    }
}
