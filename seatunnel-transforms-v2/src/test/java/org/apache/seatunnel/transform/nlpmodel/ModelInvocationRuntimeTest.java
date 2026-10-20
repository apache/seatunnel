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
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class ModelInvocationRuntimeTest {

    @Test
    void defaultRetryPolicyAttemptsRequestOnce() {
        ModelInvocationRuntime runtime =
                new ModelInvocationRuntime(ModelInvocationOptions.defaults());
        CountingVectorAdapter adapter =
                new CountingVectorAdapter(
                        ModelInvocationException.fromHttpStatus(
                                "OPENAI", "text-embedding-3-small", 429, "rate limited"));

        ModelInvocationException exception =
                Assertions.assertThrows(
                        ModelInvocationException.class,
                        () -> runtime.invoke(new Object[] {"chunk"}, adapter));

        Assertions.assertEquals(ModelInvocationErrorType.RATE_LIMIT, exception.getErrorType());
        Assertions.assertEquals(1, adapter.getAttempts());
    }

    @Test
    void retryableRateLimitFailureSucceedsAfterRetry() throws IOException {
        ModelInvocationRuntime runtime = new ModelInvocationRuntime(testRetryOptions());
        CountingVectorAdapter adapter =
                new CountingVectorAdapter(
                        ModelInvocationException.fromHttpStatus(
                                "OPENAI", "text-embedding-3-small", 429, "rate limited"));
        adapter.setSuccessResponse(vectors(1));

        List<List<Float>> result = runtime.invoke(new Object[] {"chunk"}, adapter);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(2, adapter.getAttempts());
    }

    @Test
    void retryableServerFailureSucceedsAfterRetry() throws IOException {
        ModelInvocationRuntime runtime = new ModelInvocationRuntime(testRetryOptions());
        CountingVectorAdapter adapter =
                new CountingVectorAdapter(
                        ModelInvocationException.fromHttpStatus(
                                "DOUBAO", "doubao-embedding", 500, "temporary failure"));
        adapter.setSuccessResponse(vectors(1));

        List<List<Float>> result = runtime.invoke(new Object[] {"chunk"}, adapter);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(2, adapter.getAttempts());
    }

    @Test
    void retryableFailureIsRetriedUntilAttemptsAreExhausted() {
        ModelInvocationRuntime runtime = new ModelInvocationRuntime(testRetryOptions());
        CountingVectorAdapter adapter =
                new CountingVectorAdapter(
                        ModelInvocationException.fromHttpStatus(
                                "OPENAI", "text-embedding-3-small", 429, "rate limited"),
                        ModelInvocationException.fromHttpStatus(
                                "OPENAI", "text-embedding-3-small", 429, "rate limited"),
                        ModelInvocationException.fromHttpStatus(
                                "OPENAI", "text-embedding-3-small", 429, "rate limited"));

        ModelInvocationException exception =
                Assertions.assertThrows(
                        ModelInvocationException.class,
                        () -> runtime.invoke(new Object[] {"chunk"}, adapter));

        Assertions.assertEquals(ModelInvocationErrorType.RATE_LIMIT, exception.getErrorType());
        Assertions.assertEquals(3, adapter.getAttempts());
    }

    @Test
    void socketTimeoutIsNormalizedAsRetryableTimeout() throws IOException {
        ModelInvocationRuntime runtime = new ModelInvocationRuntime(testRetryOptions());
        CountingVectorAdapter adapter = new CountingVectorAdapter(new SocketTimeoutException());
        adapter.setSuccessResponse(vectors(1));

        List<List<Float>> result = runtime.invoke(new Object[] {"chunk"}, adapter);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(2, adapter.getAttempts());
    }

    @Test
    void authenticationFailureIsNotRetried() {
        ModelInvocationRuntime runtime = new ModelInvocationRuntime(testRetryOptions());
        CountingVectorAdapter adapter =
                new CountingVectorAdapter(
                        ModelInvocationException.fromHttpStatus(
                                "OPENAI", "text-embedding-3-small", 401, "bad key"));

        ModelInvocationException exception =
                Assertions.assertThrows(
                        ModelInvocationException.class,
                        () -> runtime.invoke(new Object[] {"chunk"}, adapter));

        Assertions.assertEquals(
                ModelInvocationErrorType.AUTHENTICATION_ERROR, exception.getErrorType());
        Assertions.assertEquals(1, adapter.getAttempts());
    }

    @Test
    void responseCountMismatchFailsWithoutEmittingMisalignedVectors() {
        ModelInvocationRuntime runtime = new ModelInvocationRuntime(testRetryOptions());
        CountingVectorAdapter adapter = new CountingVectorAdapter();
        adapter.setSuccessResponse(vectors(1));

        ModelInvocationException exception =
                Assertions.assertThrows(
                        ModelInvocationException.class,
                        () -> runtime.invoke(new Object[] {"chunk-1", "chunk-2"}, adapter));

        Assertions.assertEquals(
                ModelInvocationErrorType.RESPONSE_COUNT_MISMATCH, exception.getErrorType());
        Assertions.assertEquals(1, adapter.getAttempts());
    }

    @Test
    void responseParseFailureIsNotRetried() {
        ModelInvocationRuntime runtime = new ModelInvocationRuntime(testRetryOptions());
        CountingVectorAdapter adapter =
                new CountingVectorAdapter(
                        ModelInvocationException.nonRetryable(
                                ModelInvocationErrorType.RESPONSE_PARSE_ERROR,
                                "CUSTOM",
                                "custom-model",
                                "custom_response_parse did not match",
                                null));

        ModelInvocationException exception =
                Assertions.assertThrows(
                        ModelInvocationException.class,
                        () -> runtime.invoke(new Object[] {"chunk"}, adapter));

        Assertions.assertEquals(
                ModelInvocationErrorType.RESPONSE_PARSE_ERROR, exception.getErrorType());
        Assertions.assertEquals(1, adapter.getAttempts());
    }

    @Test
    void httpStatusExceptionDoesNotExposeProviderResponseBody() {
        ModelInvocationException exception =
                ModelInvocationException.fromHttpStatus(
                        "OPENAI",
                        "text-embedding-3-small",
                        401,
                        "api_key=secret original text chunk");

        Assertions.assertFalse(exception.getMessage().contains("secret"));
        Assertions.assertFalse(exception.getMessage().contains("original text chunk"));
        Assertions.assertTrue(exception.getMessage().contains("HTTP status 401"));
    }

    private static ModelInvocationOptions testRetryOptions() {
        return new ModelInvocationOptions(3, 0L, 0L, 20000);
    }

    private static List<List<Float>> vectors(int count) {
        List<List<Float>> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            List<Float> vector = new ArrayList<>();
            vector.add(1.0f);
            vector.add(2.0f);
            result.add(vector);
        }
        return result;
    }

    private static class CountingVectorAdapter implements ProviderAdapter<List<List<Float>>> {

        private final Queue<IOException> failures = new LinkedList<>();
        private List<List<Float>> successResponse = vectors(1);
        private int attempts;

        private CountingVectorAdapter(IOException... failures) {
            for (IOException failure : failures) {
                this.failures.add(failure);
            }
        }

        private void setSuccessResponse(List<List<Float>> successResponse) {
            this.successResponse = successResponse;
        }

        private int getAttempts() {
            return attempts;
        }

        @Override
        public List<List<Float>> invoke(Object[] inputs, ModelInvocationContext context)
                throws IOException {
            attempts++;
            if (!failures.isEmpty()) {
                throw failures.remove();
            }
            return successResponse;
        }

        @Override
        public int getOutputCount(List<List<Float>> output) {
            return output == null ? 0 : output.size();
        }
    }
}
