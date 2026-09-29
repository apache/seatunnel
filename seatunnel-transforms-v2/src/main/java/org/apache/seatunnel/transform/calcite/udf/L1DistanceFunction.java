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
import java.util.stream.IntStream;

/**
 * Calculates Manhattan (L1) distance between two vectors.
 *
 * <p>Usage: {@code L1_DISTANCE(vector1, vector2)}
 */
@AutoService(CalciteUdf.class)
public class L1DistanceFunction implements CalciteUdf {

    @Override
    public String functionName() {
        return "L1_DISTANCE";
    }

    public static Double eval(byte[] v1, byte[] v2) {
        if (v1 == null || v2 == null) {
            return null;
        }
        Float[] vector1 = VectorUtils.toFloatArray(ByteBuffer.wrap(v1));
        Float[] vector2 = VectorUtils.toFloatArray(ByteBuffer.wrap(v2));
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vectors must have the same dimension: %d vs %d",
                            vector1.length, vector2.length));
        }
        return IntStream.range(0, vector1.length)
                .mapToDouble(i -> Math.abs(vector1[i] - vector2[i]))
                .sum();
    }
}
