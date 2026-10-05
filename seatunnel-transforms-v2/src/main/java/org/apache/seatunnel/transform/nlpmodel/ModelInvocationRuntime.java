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

import org.apache.http.conn.ConnectTimeoutException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class ModelInvocationRuntime {

    private final ModelInvocationOptions options;
    private final ModelInvocationMetrics metrics;
    private final ModelInvocationCache cache;

    public ModelInvocationRuntime(ModelInvocationOptions options) {
        this(options, ModelInvocationMetrics.NOOP, ModelInvocationCache.NOOP);
    }

    public ModelInvocationRuntime(
            ModelInvocationOptions options,
            ModelInvocationMetrics metrics,
            ModelInvocationCache cache) {
        this.options = options == null ? ModelInvocationOptions.defaults() : options;
        this.metrics = metrics == null ? ModelInvocationMetrics.NOOP : metrics;
        this.cache = cache == null ? ModelInvocationCache.NOOP : cache;
    }

    public <T> T invoke(Object[] inputs, ProviderAdapter<T> adapter) throws IOException {
        int inputCount = inputs == null ? 0 : inputs.length;
        if (cache != ModelInvocationCache.NOOP && inputCount > 0) {
            CacheLookup cacheLookup = lookupCache(inputs, adapter);
            if (cacheLookup.isCompleteHit()) {
                metrics.recordCacheHit(adapter.getProvider(), adapter.getModel(), inputCount);
                return castOutput(cacheLookup.getCachedOutput());
            }

            int hitCount = inputCount - cacheLookup.getMissingCount();
            if (hitCount > 0) {
                metrics.recordCacheHit(adapter.getProvider(), adapter.getModel(), hitCount);
            }
            if (cacheLookup.getMissingCount() > 0) {
                metrics.recordCacheMiss(
                        adapter.getProvider(), adapter.getModel(), cacheLookup.getMissingCount());
            }

            T output =
                    invokeWithRetries(
                            cacheLookup.getMissingInputs(), adapter, cacheLookup.getMissingCount());
            List<List<Float>> remoteVectors = asVectors(output);
            cacheLookup.putMissing(remoteVectors, cache);
            return castOutput(cacheLookup.merge(remoteVectors));
        }

        return invokeWithRetries(inputs, adapter, inputCount);
    }

    public ModelInvocationCache getCache() {
        return cache;
    }

    private <T> T invokeWithRetries(Object[] inputs, ProviderAdapter<T> adapter, int inputCount)
            throws IOException {
        for (int attempt = 1; attempt <= options.getRetryMaxAttempts(); attempt++) {
            ModelInvocationContext context =
                    new ModelInvocationContext(
                            adapter.getProvider(),
                            adapter.getModel(),
                            inputCount,
                            attempt,
                            options.getRequestTimeoutMs());
            long start = System.currentTimeMillis();
            metrics.recordRequest(context.getProvider(), context.getModel());
            try {
                T output = adapter.invoke(inputs, context);
                validateOutputCount(inputCount, adapter, output, context);
                metrics.recordGeneratedOutputs(
                        context.getProvider(), context.getModel(), adapter.getOutputCount(output));
                metrics.recordLatency(
                        context.getProvider(),
                        context.getModel(),
                        System.currentTimeMillis() - start);
                return output;
            } catch (IOException e) {
                handleInvocationException(normalize(e, context), context, attempt, start);
            } catch (RuntimeException e) {
                handleInvocationException(normalize(e, context), context, attempt, start);
            }
        }
        throw ModelInvocationException.nonRetryable(
                ModelInvocationErrorType.UNKNOWN_REMOTE_ERROR,
                "UNKNOWN",
                "UNKNOWN",
                "No model invocation attempt was executed",
                null);
    }

    private CacheLookup lookupCache(Object[] inputs, ProviderAdapter<?> adapter) {
        List<String> keys = new ArrayList<>(inputs.length);
        List<List<Float>> cachedOutput = new ArrayList<>(inputs.length);
        List<Integer> missingIndices = new ArrayList<>();
        List<Object> missingInputs = new ArrayList<>();
        for (int i = 0; i < inputs.length; i++) {
            Object input = inputs[i];
            String key = buildCacheKey(adapter, input);
            keys.add(key);
            Optional<?> cachedValue = cache.get(key);
            if (cachedValue.isPresent()) {
                cachedOutput.add(asVector(cachedValue.get()));
                continue;
            }
            cachedOutput.add(null);
            missingIndices.add(i);
            missingInputs.add(input);
        }
        return new CacheLookup(
                keys, cachedOutput, missingIndices, missingInputs.toArray(new Object[0]));
    }

    private String buildCacheKey(ProviderAdapter<?> adapter, Object input) {
        ModelInvocationCacheKey.Builder builder =
                ModelInvocationCacheKey.builder()
                        .provider(adapter.getProvider())
                        .model(adapter.getModel())
                        .dimension(adapter.getDimension())
                        .modality(adapter.getInputModality(input))
                        .format(adapter.getInputFormat(input))
                        .input(input);
        Map<String, Object> metadata = adapter.getCacheMetadata(input);
        if (metadata != null) {
            metadata.forEach(builder::metadata);
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private List<List<Float>> asVectors(Object output) {
        return (List<List<Float>>) output;
    }

    @SuppressWarnings("unchecked")
    private List<Float> asVector(Object value) {
        return (List<Float>) value;
    }

    @SuppressWarnings("unchecked")
    private <T> T castOutput(List<List<Float>> output) {
        return (T) output;
    }

    private <T> void validateOutputCount(
            int inputCount, ProviderAdapter<T> adapter, T output, ModelInvocationContext context)
            throws ModelInvocationException {
        if (!adapter.validateOutputCount()) {
            return;
        }
        int outputCount = adapter.getOutputCount(output);
        if (inputCount != outputCount) {
            metrics.recordResponseCountMismatch(context.getProvider(), context.getModel());
            throw ModelInvocationException.nonRetryable(
                    ModelInvocationErrorType.RESPONSE_COUNT_MISMATCH,
                    context.getProvider(),
                    context.getModel(),
                    "Expected " + inputCount + " outputs, but got " + outputCount,
                    null);
        }
    }

    private void handleInvocationException(
            ModelInvocationException invocationException,
            ModelInvocationContext context,
            int attempt,
            long start)
            throws ModelInvocationException {
        metrics.recordFailure(
                context.getProvider(), context.getModel(), invocationException.getErrorType());
        metrics.recordLatency(
                context.getProvider(), context.getModel(), System.currentTimeMillis() - start);

        boolean canRetry =
                invocationException.isRetryable() && attempt < options.getRetryMaxAttempts();
        logInvocationFailure(
                context, invocationException, canRetry, System.currentTimeMillis() - start);
        if (!canRetry) {
            if (invocationException.isRetryable()) {
                metrics.recordRetryExhausted(
                        context.getProvider(),
                        context.getModel(),
                        invocationException.getErrorType());
            }
            throw invocationException;
        }
        metrics.recordRetry(
                context.getProvider(), context.getModel(), invocationException.getErrorType());
        sleepBeforeRetry(attempt);
    }

    private ModelInvocationException normalize(
            Exception exception, ModelInvocationContext context) {
        if (exception instanceof ModelInvocationException) {
            return (ModelInvocationException) exception;
        }
        if (exception instanceof SocketTimeoutException
                || exception instanceof ConnectTimeoutException) {
            return ModelInvocationException.retryable(
                    ModelInvocationErrorType.TIMEOUT,
                    context.getProvider(),
                    context.getModel(),
                    "Request timeout",
                    null,
                    exception);
        }
        return ModelInvocationException.nonRetryable(
                ModelInvocationErrorType.UNKNOWN_REMOTE_ERROR,
                context.getProvider(),
                context.getModel(),
                "Unexpected model invocation failure",
                null,
                exception);
    }

    private void logInvocationFailure(
            ModelInvocationContext context,
            ModelInvocationException exception,
            boolean willRetry,
            long elapsedMs) {
        log.warn(
                "Model invocation failed: provider={}, model={}, batchSize={}, attempt={}, errorType={}, retryable={}, willRetry={}, elapsedMs={}",
                context.getProvider(),
                context.getModel(),
                context.getInputCount(),
                context.getAttempt(),
                exception.getErrorType(),
                exception.isRetryable(),
                willRetry,
                elapsedMs);
    }

    private void sleepBeforeRetry(int attempt) throws ModelInvocationException {
        long backoff = options.getRetryBackoffMs();
        if (backoff <= 0) {
            return;
        }
        long cappedBackoff =
                options.getRetryMaxBackoffMs() <= 0
                        ? backoff
                        : Math.min(backoff * attempt, options.getRetryMaxBackoffMs());
        try {
            Thread.sleep(cappedBackoff);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ModelInvocationException.nonRetryable(
                    ModelInvocationErrorType.UNKNOWN_REMOTE_ERROR,
                    "UNKNOWN",
                    "UNKNOWN",
                    "Interrupted while waiting to retry model invocation",
                    e);
        }
    }

    private static final class CacheLookup {

        private final List<String> keys;
        private final List<List<Float>> cachedOutput;
        private final List<Integer> missingIndices;
        private final Object[] missingInputs;

        private CacheLookup(
                List<String> keys,
                List<List<Float>> cachedOutput,
                List<Integer> missingIndices,
                Object[] missingInputs) {
            this.keys = keys;
            this.cachedOutput = cachedOutput;
            this.missingIndices = missingIndices;
            this.missingInputs = missingInputs;
        }

        private boolean isCompleteHit() {
            return missingIndices.isEmpty();
        }

        private List<List<Float>> getCachedOutput() {
            return cachedOutput;
        }

        private Object[] getMissingInputs() {
            return missingInputs;
        }

        private int getMissingCount() {
            return missingInputs.length;
        }

        private void putMissing(List<List<Float>> remoteVectors, ModelInvocationCache cache) {
            for (int i = 0; i < missingIndices.size(); i++) {
                int index = missingIndices.get(i);
                cache.put(keys.get(index), remoteVectors.get(i));
            }
        }

        private List<List<Float>> merge(List<List<Float>> remoteVectors) {
            for (int i = 0; i < missingIndices.size(); i++) {
                cachedOutput.set(missingIndices.get(i), remoteVectors.get(i));
            }
            return cachedOutput;
        }
    }
}
