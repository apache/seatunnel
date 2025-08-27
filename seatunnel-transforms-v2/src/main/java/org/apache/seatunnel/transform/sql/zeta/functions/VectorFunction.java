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

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.VectorUtils;
import org.apache.seatunnel.transform.exception.TransformException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class VectorFunction {

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
        }
        // calculate cosine similarity
        double cosineSimilarity = dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
        return 1.0 - cosineSimilarity;
    }

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

    public static Object vectorDims(List<Object> args) {
        if (args.size() != 1) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format(
                            "VECTOR_DIMS() requires exactly 1 argument, but %d were provided",
                            args.size()));
        }
        Object arg = args.get(0);
        if (arg == null) {
            return null;
        }
        Float[] vector = convertToFloatArray(arg);
        return vector.length;
    }

    public static Object vectorNorm(List<Object> args) {
        if (args.size() != 1) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format(
                            "VECTOR_NORM() requires exactly 1 argument, but %d were provided",
                            args.size()));
        }
        Object arg = args.get(0);
        if (arg == null) {
            return null;
        }
        Float[] vector = convertToFloatArray(arg);
        return Math.sqrt(Arrays.stream(vector).mapToDouble(v -> v * v).sum());
    }

    public static Object innerProduct(List<Object> args) {
        if (args.size() != 2) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format(
                            "INNER_PRODUCT() requires exactly 2 arguments, but %d were provided",
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

        return IntStream.range(0, v1.length).mapToDouble(i -> v1[i] * v2[i]).sum();
    }

    private static Float[] convertToFloatArray(Object obj) {
        if (obj instanceof ByteBuffer) {
            return VectorUtils.toFloatArray((ByteBuffer) obj);
        } else if (obj instanceof Float[]) {
            return (Float[]) obj;
        } else if (obj instanceof float[]) {
            float[] primitiveArray = (float[]) obj;
            Float[] wrapperArray = new Float[primitiveArray.length];
            for (int i = 0; i < primitiveArray.length; i++) {
                wrapperArray[i] = primitiveArray[i];
            }
            return wrapperArray;
        } else if (obj instanceof Map) {
            return VectorUtils.convertSparseVectorToFloatArray((Map<?, ?>) obj);
        } else {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format("Unsupported vector type: %s", obj.getClass().getName()));
        }
    }
}
