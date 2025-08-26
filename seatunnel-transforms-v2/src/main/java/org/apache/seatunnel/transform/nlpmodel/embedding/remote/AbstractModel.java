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

package org.apache.seatunnel.transform.nlpmodel.embedding.remote;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.common.utils.BufferUtils;

import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractModel implements Model {

    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected static final String DIMENSION_EXAMPLE = "dimension example";
    private final Integer singleVectorizedInputNumber;

    protected AbstractModel(Integer singleVectorizedInputNumber) {
        this.singleVectorizedInputNumber = singleVectorizedInputNumber;
    }

    @Override
    public List<ByteBuffer> vectorization(Object[] fields) throws IOException {
        List<ByteBuffer> result = new ArrayList<>();

        List<List<Float>> vectors = batchProcess(fields, singleVectorizedInputNumber);
        for (List<Float> vector : vectors) {
            result.add(BufferUtils.toByteBuffer(vector.toArray(new Float[0])));
        }
        return result;
    }

    protected abstract List<List<Float>> vector(Object[] fields) throws IOException;

    public List<List<Float>> batchProcess(Object[] array, int batchSize) throws IOException {
        List<List<Float>> merged = new ArrayList<>();
        if (array == null || array.length == 0) {
            return merged;
        }
        for (int i = 0; i < array.length; i += batchSize) {
            Object[] batch = ArrayUtils.subarray(array, i, i + batchSize);
            List<List<Float>> vector = vector(batch);
            merged.addAll(vector);
        }
        if (array.length != merged.size()) {
            throw new RuntimeException(
                    "The number of vectors is not equal to the number of inputs, Please verify the configuration of the input field and the result returned.");
        }
        return merged;
    }
}
