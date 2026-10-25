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

package org.apache.seatunnel.transform.nlpmodel;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ModelInvocationRuntimeCacheTest {

    private static final String PROVIDER = "OPENAI";
    private static final String MODEL = "text-embedding-3-small";
    private static final int DIMENSION = 1536;
    private static final String MODALITY = "text";
    private static final String FORMAT = "text";

    @Test
    void fullCacheHitSkipsRemoteInvocation() throws IOException {
        RecordingModelInvocationCache cache = new RecordingModelInvocationCache();
        cache.put(cacheKey("alpha"), vector(1.0f, 2.0f));
        cache.put(cacheKey("beta"), vector(3.0f, 4.0f));

        CountingVectorAdapter adapter = new CountingVectorAdapter();
        ModelInvocationRuntime runtime =
                new ModelInvocationRuntime(
                        ModelInvocationOptions.defaults(), ModelInvocationMetrics.NOOP, cache);

        List<List<Float>> result = runtime.invoke(new Object[] {"alpha", "beta"}, adapter);

        Assertions.assertEquals(0, adapter.getInvocationCount());
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(vector(1.0f, 2.0f), result.get(0));
        Assertions.assertEquals(vector(3.0f, 4.0f), result.get(1));
    }

    @Test
    void partialCacheHitReassemblesCachedAndGeneratedVectorsInOrder() throws IOException {
        RecordingModelInvocationCache cache = new RecordingModelInvocationCache();
        cache.put(cacheKey("alpha"), vector(1.0f, 2.0f));

        CountingVectorAdapter adapter =
                new CountingVectorAdapter(new Object[] {"beta"}, vectors(vector(3.0f, 4.0f)));
        ModelInvocationRuntime runtime =
                new ModelInvocationRuntime(
                        ModelInvocationOptions.defaults(), ModelInvocationMetrics.NOOP, cache);

        List<List<Float>> result = runtime.invoke(new Object[] {"alpha", "beta"}, adapter);

        Assertions.assertEquals(1, adapter.getInvocationCount());
        Assertions.assertEquals(1, adapter.getLastInvocationInputCount());
        Assertions.assertEquals("beta", adapter.getLastInvocationInputs()[0]);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(vector(1.0f, 2.0f), result.get(0));
        Assertions.assertEquals(vector(3.0f, 4.0f), result.get(1));
    }

    @Test
    void successfulInvocationStoresGeneratedVectorsInCache() throws IOException {
        RecordingModelInvocationCache cache = new RecordingModelInvocationCache();
        CountingVectorAdapter adapter =
                new CountingVectorAdapter(
                        new Object[] {"alpha", "beta"},
                        vectors(vector(1.0f, 2.0f), vector(3.0f, 4.0f)));
        ModelInvocationRuntime runtime =
                new ModelInvocationRuntime(
                        ModelInvocationOptions.defaults(), ModelInvocationMetrics.NOOP, cache);

        List<List<Float>> result = runtime.invoke(new Object[] {"alpha", "beta"}, adapter);

        Assertions.assertEquals(1, adapter.getInvocationCount());
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(vector(1.0f, 2.0f), result.get(0));
        Assertions.assertEquals(vector(3.0f, 4.0f), result.get(1));
        Assertions.assertEquals(2, cache.size());
        Assertions.assertEquals(
                vector(1.0f, 2.0f),
                cache.get(cacheKey("alpha"))
                        .orElseThrow(() -> new AssertionError("missing alpha")));
        Assertions.assertEquals(
                vector(3.0f, 4.0f),
                cache.get(cacheKey("beta")).orElseThrow(() -> new AssertionError("missing beta")));
    }

    private static String cacheKey(String input) {
        return ModelInvocationCacheKey.builder()
                .provider(PROVIDER)
                .model(MODEL)
                .dimension(DIMENSION)
                .modality(MODALITY)
                .format(FORMAT)
                .input(input)
                .build();
    }

    private static List<Float> vector(float... values) {
        List<Float> result = new ArrayList<>();
        for (float value : values) {
            result.add(value);
        }
        return result;
    }

    @SafeVarargs
    private static List<List<Float>> vectors(List<Float>... vectors) {
        List<List<Float>> result = new ArrayList<>();
        for (List<Float> vector : vectors) {
            result.add(vector);
        }
        return result;
    }

    private static class RecordingModelInvocationCache implements ModelInvocationCache {

        private final Map<String, Object> values = new HashMap<>();

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<T> get(String key) {
            return Optional.ofNullable((T) values.get(key));
        }

        @Override
        public <T> void put(String key, T value) {
            values.put(key, value);
        }

        private int size() {
            return values.size();
        }
    }

    private static class CountingVectorAdapter implements ProviderAdapter<List<List<Float>>> {

        private final Object[] expectedInputs;
        private final List<List<Float>> response;
        private int invocationCount;
        private Object[] lastInvocationInputs = new Object[0];

        private CountingVectorAdapter() {
            this.expectedInputs = null;
            this.response = null;
        }

        private CountingVectorAdapter(Object[] expectedInputs, List<List<Float>> response) {
            this.expectedInputs = expectedInputs;
            this.response = response;
        }

        private int getInvocationCount() {
            return invocationCount;
        }

        private int getLastInvocationInputCount() {
            return lastInvocationInputs.length;
        }

        private Object[] getLastInvocationInputs() {
            return lastInvocationInputs;
        }

        @Override
        public List<List<Float>> invoke(Object[] inputs, ModelInvocationContext context)
                throws IOException {
            invocationCount++;
            lastInvocationInputs = inputs == null ? new Object[0] : inputs.clone();
            if (expectedInputs != null) {
                Assertions.assertArrayEquals(expectedInputs, lastInvocationInputs);
            } else {
                throw new AssertionError(
                        "Remote invocation should not be called on a full cache hit");
            }
            return response;
        }

        @Override
        public int getOutputCount(List<List<Float>> output) {
            return output == null ? 0 : output.size();
        }

        @Override
        public String getProvider() {
            return PROVIDER;
        }

        @Override
        public String getModel() {
            return MODEL;
        }

        @Override
        public Integer getDimension() {
            return DIMENSION;
        }

        @Override
        public String getInputModality(Object input) {
            return MODALITY;
        }

        @Override
        public String getInputFormat(Object input) {
            return FORMAT;
        }
    }
}
