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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

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
        }

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

        Float[] sourceVector = extractFloatArray(vectorData);
        if (sourceVector.length <= targetDimension) {
            return vectorData; // No need to reduce
        }

        float[][] projectionMatrix =
                createGaussianProjectionMatrix(sourceVector.length, targetDimension);
        Float[] result = applyProjection(sourceVector, projectionMatrix, targetDimension);
        return BufferUtils.toByteBuffer(result);
    }

    /**
     * Sparse random projection for dimension reduction Usage: VECTOR_REDUCE(embedding, 64,
     * 'SPARSE_RANDOM_PROJECTION')
     */
    public static Object vectorSparseProjection(Object vectorData, Integer targetDimension) {
        if (vectorData == null || targetDimension == null) {
            return null;
        }

        Float[] sourceVector = extractFloatArray(vectorData);
        if (sourceVector.length <= targetDimension) {
            return vectorData; // No need to reduce
        }

        float[][] projectionMatrix =
                createSparseProjectionMatrix(sourceVector.length, targetDimension);
        Float[] result = applyProjection(sourceVector, projectionMatrix, targetDimension);
        return BufferUtils.toByteBuffer(result);
    }

    /**
     * Generic vector dimension reduction function Usage: VECTOR_REDUCE(vector_field,
     * target_dimension, method) method: 'TRUNCATE', 'RANDOM_PROJECTION', 'SPARSE_RANDOM_PROJECTION'
     */
    public static Object vectorReduce(Object vectorData, Integer targetDimension, String method) {
        if (vectorData == null || targetDimension == null || method == null) {
            return null;
        }

        switch (method.toUpperCase()) {
            case "TRUNCATE":
                return vectorTruncate(vectorData, targetDimension);
            case "RANDOM_PROJECTION":
                return vectorRandomProjection(vectorData, targetDimension);
            case "SPARSE_RANDOM_PROJECTION":
                return vectorSparseProjection(vectorData, targetDimension);
            default:
                throw new IllegalArgumentException("Unknown reduction method: " + method);
        }
    }

    /** Get vector dimension Usage: VECTOR_DIMENSION(vector_field) */
    public static Integer vectorDimension(Object vectorData) {
        if (vectorData == null) {
            return null;
        }

        Float[] vector = extractFloatArray(vectorData);
        return vector.length;
    }

    /** Calculate vector magnitude (L2 norm) Usage: VECTOR_MAGNITUDE(vector_field) */
    public static Double vectorMagnitude(Object vectorData) {
        if (vectorData == null) {
            return null;
        }

        Float[] vector = extractFloatArray(vectorData);
        double sum = 0.0;
        for (Float value : vector) {
            if (value != null) {
                sum += value * value;
            }
        }
        return Math.sqrt(sum);
    }

    /** Normalize vector to unit length Usage: VECTOR_NORMALIZE(vector_field) */
    public static Object vectorNormalize(Object vectorData) {
        if (vectorData == null) {
            return null;
        }

        Float[] vector = extractFloatArray(vectorData);
        double magnitude = 0.0;
        for (Float value : vector) {
            if (value != null) {
                magnitude += value * value;
            }
        }
        magnitude = Math.sqrt(magnitude);

        if (magnitude == 0.0) {
            return vectorData; // Return original if zero vector
        }

        Float[] normalized = new Float[vector.length];
        for (int i = 0; i < vector.length; i++) {
            normalized[i] = vector[i] == null ? null : (float) (vector[i] / magnitude);
        }

        return BufferUtils.toByteBuffer(normalized);
    }

    /**
     * Calculate cosine similarity between two vectors Usage: VECTOR_COSINE_SIMILARITY(vector1,
     * vector2)
     */
    public static Double vectorCosineSimilarity(Object vector1Data, Object vector2Data) {
        if (vector1Data == null || vector2Data == null) {
            return null;
        }

        Float[] vector1 = extractFloatArray(vector1Data);
        Float[] vector2 = extractFloatArray(vector2Data);

        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimension");
        }

        double dotProduct = 0.0;
        double magnitude1 = 0.0;
        double magnitude2 = 0.0;

        for (int i = 0; i < vector1.length; i++) {
            if (vector1[i] != null && vector2[i] != null) {
                dotProduct += vector1[i] * vector2[i];
                magnitude1 += vector1[i] * vector1[i];
                magnitude2 += vector2[i] * vector2[i];
            }
        }

        if (magnitude1 == 0.0 || magnitude2 == 0.0) {
            return 0.0;
        }

        return dotProduct / (Math.sqrt(magnitude1) * Math.sqrt(magnitude2));
    }

    // Helper methods

    private static Float[] extractFloatArray(Object vectorData) {
        if (vectorData instanceof ByteBuffer) {
            return BufferUtils.toFloatArray((ByteBuffer) vectorData);
        } else if (vectorData instanceof Float[]) {
            return (Float[]) vectorData;
        } else if (vectorData instanceof List) {
            @SuppressWarnings("unchecked")
            List<Number> list = (List<Number>) vectorData;
            Float[] array = new Float[list.size()];
            for (int i = 0; i < list.size(); i++) {
                array[i] = list.get(i).floatValue();
            }
            return array;
        } else {
            throw new IllegalArgumentException(
                    "Unsupported vector data type: " + vectorData.getClass());
        }
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
