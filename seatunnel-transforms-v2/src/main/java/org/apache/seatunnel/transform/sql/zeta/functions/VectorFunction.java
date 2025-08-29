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

import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.VectorUtils;
import org.apache.seatunnel.transform.exception.TransformException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Map;
import java.util.stream.IntStream;

/** Vector functions for SQL engine, providing vector operations like dimension reduction */
public class VectorFunction {

    private static final Random random = new Random(42);

    /** Truncate vector to target dimension Usage: VECTOR_REDUCE(embedding, 256, 'TRUNCATE') */
    public static Object vectorTruncate(Object vectorData, Integer targetDimension) {
        if (vectorData == null || targetDimension == null) {
            return null;
        }

        Float[] sourceVector = extractFloatArray(vectorData);
        if (sourceVector.length <= targetDimension) {
            return vectorData; // No need to truncate
    public static Object cosineDistance(List<Object> args) {
        if (args.size() != 2) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format(
                            "COSINE_DISTANCE() requires 2 arguments, but %d were provided",
                            args.size()));
        }
        Object arg1 = args.get(0);
        Object arg2 = args.get(1);
        if (arg1 == null || arg2 == null) {

        Float[] result = new Float[targetDimension];
        System.arraycopy(sourceVector, 0, result, 0, targetDimension);
        return BufferUtils.toByteBuffer(result);
    }

    /**
     * Random projection for dimension reduction Usage: VECTOR_REDUCE(embedding, 128,
     * 'RANDOM_PROJECTION')
     */
    public static Object vectorRandomProjection(Object vectorData, Integer targetDimension) {
        if (vectorData == null || targetDimension == null) {
            return null;
        }
        Float[] vector1 = convertToFloatArray(arg1);
        Float[] vector2 = convertToFloatArray(arg2);
        if (vector1.length != vector2.length) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    String.format(
                            "Vectors must have the same dimension: %d vs %d",
                            vector1.length, vector2.length));
        }
        double dotProduct =
                IntStream.range(0, vector1.length).mapToDouble(i -> vector1[i] * vector2[i]).sum();
        double norm1 = Arrays.stream(vector1).mapToDouble(v -> v * v).sum();
        double norm2 = Arrays.stream(vector2).mapToDouble(v -> v * v).sum();
        if (norm1 == 0.0 || norm2 == 0.0) {
            return 1.0;

        Float[] sourceVector = extractFloatArray(vectorData);
        if (sourceVector.length <= targetDimension) {
            return vectorData; // No need to reduce
        }

        float[][] projectionMatrix =
                createGaussianProjectionMatrix(sourceVector.length, targetDimension);
        Float[] result = applyProjection(sourceVector, projectionMatrix, targetDimension);
        return BufferUtils.toByteBuffer(result);
        // calculate cosine similarity
        double cosineSimilarity = dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
        return 1.0 - cosineSimilarity;
    }

    /**
     * Sparse random projection for dimension reduction Usage: VECTOR_REDUCE(embedding, 64,
     * 'SPARSE_RANDOM_PROJECTION')
     */
    public static Object vectorSparseProjection(Object vectorData, Integer targetDimension) {
        if (vectorData == null || targetDimension == null) {
    public static Object l1Distance(List<Object> args) {
        if (args.size() != 2) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format(
                            "L1_DISTANCE() requires exactly 2 arguments, but %d were provided",
                            args.size()));
        }
        Object arg1 = args.get(0);
        Object arg2 = args.get(1);
        if (arg1 == null || arg2 == null) {
            return null;
        }

        Float[] sourceVector = extractFloatArray(vectorData);
        if (sourceVector.length <= targetDimension) {
            return vectorData; // No need to reduce
        Float[] v1 = convertToFloatArray(arg1);
        Float[] v2 = convertToFloatArray(arg2);
        if (v1.length != v2.length) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    String.format(
                            "Vectors must have the same dimension: %d vs %d",
                            v1.length, v2.length));
        }
        return IntStream.range(0, v1.length).mapToDouble(i -> Math.abs(v1[i] - v2[i])).sum();
    }

    public static Object l2Distance(List<Object> args) {
        if (args.size() != 2) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format(
                            "L2_DISTANCE() requires exactly 2 arguments, but %d were provided",
                            args.size()));
        }
        Object arg1 = args.get(0);
        Object arg2 = args.get(1);
        if (arg1 == null || arg2 == null) {
            return null;
        }
        Float[] v1 = convertToFloatArray(arg1);
        Float[] v2 = convertToFloatArray(arg2);
        if (v1.length != v2.length) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    String.format(
                            "Vectors must have the same dimension: %d vs %d",
                            v1.length, v2.length));
        }
        double sum =
                IntStream.range(0, v1.length)
                        .mapToDouble(
                                i -> {
                                    double diff = v1[i] - v2[i];
                                    return diff * diff;
                                })
                        .sum();
        return Math.sqrt(sum);
    }

            return null;
        }
        return vector.length;
    }

            return null;
        }
    }

            return null;
        }
        }

        }

    }
        } else {
        }
        return result;
    }

    private static float[][] createGaussianProjectionMatrix(
            int sourceDimension, int targetDimension) {
        float[][] matrix = new float[targetDimension][sourceDimension];
        float scale = (float) Math.sqrt(1.0 / targetDimension);

        for (int i = 0; i < targetDimension; i++) {
            for (int j = 0; j < sourceDimension; j++) {
                matrix[i][j] = (float) random.nextGaussian() * scale;
            }
        }
        return matrix;
    }

    private static float[][] createSparseProjectionMatrix(
            int sourceDimension, int targetDimension) {
        float[][] matrix = new float[targetDimension][sourceDimension];
        float scale = (float) Math.sqrt(3.0);
        double p1 = 1.0 / 6.0;
        double p2 = 2.0 / 6.0;

        for (int i = 0; i < targetDimension; i++) {
            for (int j = 0; j < sourceDimension; j++) {
                double rand = random.nextDouble();
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
