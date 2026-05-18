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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

public class ModelInvocationRuntimeMetricsTest {

    private static final String PROVIDER = "OPENAI";
    private static final String MODEL = "text-embedding-3-small";

    @Test
    void successfulInvocationRecordsRequestGeneratedOutputsAndLatency() throws IOException {
        RecordingMetrics metrics = new RecordingMetrics();
        MetricVectorAdapter adapter = new MetricVectorAdapter(vectors(vector(1.0f, 2.0f)));
        ModelInvocationRuntime runtime =
                new ModelInvocationRuntime(
                        ModelInvocationOptions.defaults(), metrics, ModelInvocationCache.NOOP);

        List<List<Float>> result = runtime.invoke(new Object[] {"chunk"}, adapter);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(
                Arrays.asList(
                        "request:OPENAI:text-embedding-3-small",
                        "generated:OPENAI:text-embedding-3-small:1",
                        "latency:OPENAI:text-embedding-3-small"),
                metrics.eventPrefixes());
    }

    @Test
    void retryableFailureThenSuccessRecordsFailureRetryAndLatency() throws IOException {
        RecordingMetrics metrics = new RecordingMetrics();
        MetricVectorAdapter adapter =
                new MetricVectorAdapter(
                        ModelInvocationException.fromHttpStatus(
                                PROVIDER, MODEL, 429, "rate limited"));
        adapter.setSuccessResponse(vectors(vector(1.0f, 2.0f)));
        ModelInvocationRuntime runtime =
                new ModelInvocationRuntime(testRetryOptions(), metrics, ModelInvocationCache.NOOP);

        List<List<Float>> result = runtime.invoke(new Object[] {"chunk"}, adapter);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(2, metrics.countEvents("request:OPENAI:text-embedding-3-small"));
        Assertions.assertEquals(
                1, metrics.countEvents("failure:OPENAI:text-embedding-3-small:RATE_LIMIT"));
        Assertions.assertEquals(
                1, metrics.countEvents("retry:OPENAI:text-embedding-3-small:RATE_LIMIT"));
        Assertions.assertEquals(
                1, metrics.countEvents("generated:OPENAI:text-embedding-3-small:1"));
        Assertions.assertEquals(2, metrics.countEvents("latency:OPENAI:text-embedding-3-small"));
    }

    @Test
    void retryExhaustionRecordsRetryExhaustedMetric() {
        RecordingMetrics metrics = new RecordingMetrics();
        MetricVectorAdapter adapter =
                new MetricVectorAdapter(
                        ModelInvocationException.fromHttpStatus(
                                PROVIDER, MODEL, 429, "rate limited"),
                        ModelInvocationException.fromHttpStatus(
                                PROVIDER, MODEL, 429, "rate limited"));
        ModelInvocationRuntime runtime =
                new ModelInvocationRuntime(testRetryOptions(), metrics, ModelInvocationCache.NOOP);

        ModelInvocationException exception =
                Assertions.assertThrows(
                        ModelInvocationException.class,
                        () -> runtime.invoke(new Object[] {"chunk"}, adapter));

        Assertions.assertEquals(ModelInvocationErrorType.RATE_LIMIT, exception.getErrorType());
        Assertions.assertEquals(2, metrics.countEvents("request:OPENAI:text-embedding-3-small"));
        Assertions.assertEquals(
                2, metrics.countEvents("failure:OPENAI:text-embedding-3-small:RATE_LIMIT"));
        Assertions.assertEquals(
                1, metrics.countEvents("retry:OPENAI:text-embedding-3-small:RATE_LIMIT"));
        Assertions.assertEquals(
                1, metrics.countEvents("retryExhausted:OPENAI:text-embedding-3-small:RATE_LIMIT"));
    }

    @Test
    void responseCountMismatchRecordsMismatchMetric() {
        RecordingMetrics metrics = new RecordingMetrics();
        MetricVectorAdapter adapter = new MetricVectorAdapter(vectors(vector(1.0f, 2.0f)));
        ModelInvocationRuntime runtime =
                new ModelInvocationRuntime(testRetryOptions(), metrics, ModelInvocationCache.NOOP);

        ModelInvocationException exception =
                Assertions.assertThrows(
                        ModelInvocationException.class,
                        () -> runtime.invoke(new Object[] {"chunk-1", "chunk-2"}, adapter));

        Assertions.assertEquals(
                ModelInvocationErrorType.RESPONSE_COUNT_MISMATCH, exception.getErrorType());
        Assertions.assertEquals(
                1, metrics.countEvents("responseCountMismatch:OPENAI:text-embedding-3-small"));
        Assertions.assertEquals(
                1,
                metrics.countEvents(
                        "failure:OPENAI:text-embedding-3-small:RESPONSE_COUNT_MISMATCH"));
    }

    @Test
    void cacheMetricsRecordHitAndMissCounts() throws IOException {
        RecordingMetrics metrics = new RecordingMetrics();
        RecordingModelInvocationCache cache = new RecordingModelInvocationCache();
        cache.put(cacheKey("alpha"), vector(1.0f, 2.0f));
        MetricVectorAdapter adapter = new MetricVectorAdapter(vectors(vector(3.0f, 4.0f)));
        ModelInvocationRuntime runtime =
                new ModelInvocationRuntime(testRetryOptions(), metrics, cache);

        List<List<Float>> result = runtime.invoke(new Object[] {"alpha", "beta"}, adapter);

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(1, metrics.countEvents("cacheHit:OPENAI:text-embedding-3-small:1"));
        Assertions.assertEquals(
                1, metrics.countEvents("cacheMiss:OPENAI:text-embedding-3-small:1"));
    }

    private static ModelInvocationOptions testRetryOptions() {
        return new ModelInvocationOptions(2, 0L, 0L, 20000);
    }

    private static String cacheKey(String input) {
        return ModelInvocationCacheKey.builder()
                .provider(PROVIDER)
                .model(MODEL)
                .dimension(1536)
                .modality("text")
                .format("text")
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

    private static class RecordingMetrics implements ModelInvocationMetrics {

        private final List<String> events = new ArrayList<>();

        @Override
        public void recordRequest(String provider, String model) {
            events.add("request:" + provider + ":" + model);
        }

        @Override
        public void recordFailure(
                String provider, String model, ModelInvocationErrorType errorType) {
            events.add("failure:" + provider + ":" + model + ":" + errorType);
        }

        @Override
        public void recordRetry(String provider, String model, ModelInvocationErrorType errorType) {
            events.add("retry:" + provider + ":" + model + ":" + errorType);
        }

        @Override
        public void recordRetryExhausted(
                String provider, String model, ModelInvocationErrorType errorType) {
            events.add("retryExhausted:" + provider + ":" + model + ":" + errorType);
        }

        @Override
        public void recordResponseCountMismatch(String provider, String model) {
            events.add("responseCountMismatch:" + provider + ":" + model);
        }

        @Override
        public void recordGeneratedOutputs(String provider, String model, int outputCount) {
            events.add("generated:" + provider + ":" + model + ":" + outputCount);
        }

        @Override
        public void recordLatency(String provider, String model, long elapsedMs) {
            events.add("latency:" + provider + ":" + model);
        }

        @Override
        public void recordCacheHit(String provider, String model, int hitCount) {
            events.add("cacheHit:" + provider + ":" + model + ":" + hitCount);
        }

        @Override
        public void recordCacheMiss(String provider, String model, int missCount) {
            events.add("cacheMiss:" + provider + ":" + model + ":" + missCount);
        }

        private int countEvents(String prefix) {
            int count = 0;
            for (String event : events) {
                if (event.startsWith(prefix)) {
                    count++;
                }
            }
            return count;
        }

        private List<String> eventPrefixes() {
            return events;
        }
    }

    private static class RecordingModelInvocationCache implements ModelInvocationCache {

        private final java.util.Map<String, Object> values = new java.util.HashMap<>();

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<T> get(String key) {
            return Optional.ofNullable((T) values.get(key));
        }

        @Override
        public <T> void put(String key, T value) {
            values.put(key, value);
        }
    }

    private static class MetricVectorAdapter implements ProviderAdapter<List<List<Float>>> {

        private final Queue<IOException> failures = new LinkedList<>();
        private List<List<Float>> successResponse = vectors(vector(1.0f, 2.0f));

        private MetricVectorAdapter(List<List<Float>> successResponse) {
            this.successResponse = successResponse;
        }

        private MetricVectorAdapter(IOException... failures) {
            for (IOException failure : failures) {
                this.failures.add(failure);
            }
        }

        private void setSuccessResponse(List<List<Float>> successResponse) {
            this.successResponse = successResponse;
        }

        @Override
        public List<List<Float>> invoke(Object[] inputs, ModelInvocationContext context)
                throws IOException {
            if (!failures.isEmpty()) {
                throw failures.remove();
            }
            return successResponse;
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
            return 1536;
        }

        @Override
        public String getInputModality(Object input) {
            return "text";
        }

        @Override
        public String getInputFormat(Object input) {
            return "text";
        }
    }
}
