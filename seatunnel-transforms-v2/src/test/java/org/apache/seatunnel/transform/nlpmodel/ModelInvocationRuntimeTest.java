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

import org.apache.http.message.BasicHeader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ModelInvocationRuntimeTest {

    @Test
    void parsesRetryAfterSecondsWithoutOverflow() {
        Assertions.assertNull(ModelInvocationException.parseRetryAfter(null, 0));
        Assertions.assertNull(ModelInvocationException.parseRetryAfter("", 0));
        Assertions.assertEquals(0L, ModelInvocationException.parseRetryAfter("0", 0));
        Assertions.assertEquals(2000L, ModelInvocationException.parseRetryAfter(" 002 ", 0));
        Assertions.assertEquals(
                9223372036854775000L,
                ModelInvocationException.parseRetryAfter("9223372036854775", 0));
        Assertions.assertEquals(
                Long.MAX_VALUE, ModelInvocationException.parseRetryAfter("9223372036854776", 0));
        Assertions.assertEquals(
                Long.MAX_VALUE,
                ModelInvocationException.parseRetryAfter("9999999999999999999999999999", 0));
        for (String invalid :
                new String[] {
                    "-1",
                    "+1",
                    "1.5",
                    "not-a-date",
                    "1, 2",
                    "Sun, 06 Nov 1994 08:49:37 GMT trailing",
                    "Sun, 32 Nov 1994 08:49:37 GMT"
                }) {
            Assertions.assertNull(ModelInvocationException.parseRetryAfter(invalid, 0), invalid);
        }
    }

    @Test
    void parsesHttpDatesAndClampsPastDatesToZero() {
        long instant = Instant.parse("1994-11-06T08:49:37Z").toEpochMilli();
        for (String date :
                new String[] {
                    "Sun, 06 Nov 1994 08:49:37 GMT",
                    "Sunday, 06-Nov-94 08:49:37 GMT",
                    "Sun Nov  6 08:49:37 1994"
                }) {
            Assertions.assertEquals(
                    1234L, ModelInvocationException.parseRetryAfter(date, instant - 1234), date);
            Assertions.assertEquals(
                    0L, ModelInvocationException.parseRetryAfter(date, instant + 1), date);
        }
    }

    @Test
    void retryDelayUsesLongerOfLocalAndProviderBackoff() {
        ModelInvocationRuntime runtime =
                new ModelInvocationRuntime(new ModelInvocationOptions(3, 100L, 500L, 20000));
        Assertions.assertEquals(100L, runtime.retryDelayMs(1, null));
        Assertions.assertEquals(200L, runtime.retryDelayMs(2, 150L));
        Assertions.assertEquals(400L, runtime.retryDelayMs(1, 400L));
        Assertions.assertEquals(500L, runtime.retryDelayMs(10, 0L));
        runtime =
                new ModelInvocationRuntime(
                        new ModelInvocationOptions(3, Long.MAX_VALUE, 500L, 20000));
        Assertions.assertEquals(500L, runtime.retryDelayMs(2, null));
        runtime = new ModelInvocationRuntime(new ModelInvocationOptions(3, 100L, 0L, 20000));
        Assertions.assertEquals(100L, runtime.retryDelayMs(2, null));
        Assertions.assertEquals(1000L, runtime.retryDelayMs(2, 1000L));
    }

    @Test
    void providerDelayAbovePositiveCapFailsWithoutAnotherRequest() {
        ModelInvocationRuntime runtime =
                new ModelInvocationRuntime(new ModelInvocationOptions(3, 0L, 1000L, 20000));
        ModelInvocationException failure = httpFailure(429, "2");
        CountingVectorAdapter adapter = new CountingVectorAdapter(failure);
        Assertions.assertSame(
                failure,
                Assertions.assertThrows(
                        ModelInvocationException.class,
                        () -> runtime.invoke(new Object[] {"chunk"}, adapter)));
        Assertions.assertEquals(1, adapter.getAttempts());
        Assertions.assertEquals(ModelInvocationErrorType.RATE_LIMIT, failure.getErrorType());
    }

    @Test
    void invalidOrZeroDelayFallsBackWithoutChangingAttemptBudget() throws IOException {
        for (String header : new String[] {"garbage", "-1", "0"}) {
            CountingVectorAdapter adapter = new CountingVectorAdapter(httpFailure(503, header));
            new ModelInvocationRuntime(testRetryOptions()).invoke(new Object[] {"chunk"}, adapter);
            Assertions.assertEquals(2, adapter.getAttempts());
        }
    }

    @Test
    void defaultSingleAttemptDoesNotWaitForProviderDelay() {
        CountingVectorAdapter adapter = new CountingVectorAdapter(httpFailure(429, "9999999999"));
        Assertions.assertThrows(
                ModelInvocationException.class,
                () ->
                        new ModelInvocationRuntime(ModelInvocationOptions.defaults())
                                .invoke(new Object[] {"chunk"}, adapter));
        Assertions.assertEquals(1, adapter.getAttempts());
        Assertions.assertNull(httpFailure(401, "10").getRetryAfterMs());
        Assertions.assertNull(httpFailure(400, "10").getRetryAfterMs());
    }

    @Test
    void interruptStopsProviderWaitAndPreservesInterruptFlag() throws InterruptedException {
        CountDownLatch firstAttempt = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountingVectorAdapter adapter =
                new CountingVectorAdapter(httpFailure(429, "3600")) {
                    @Override
                    public List<List<Float>> invoke(Object[] inputs, ModelInvocationContext context)
                            throws IOException {
                        firstAttempt.countDown();
                        return super.invoke(inputs, context);
                    }
                };
        Thread worker =
                new Thread(
                        () -> {
                            try {
                                new ModelInvocationRuntime(
                                                new ModelInvocationOptions(3, 0, 0, 20000))
                                        .invoke(new Object[] {"chunk"}, adapter);
                            } catch (Throwable exception) {
                                failure.set(exception);
                                interrupted.set(Thread.currentThread().isInterrupted());
                            }
                        });
        worker.start();
        try {
            Assertions.assertTrue(firstAttempt.await(5, TimeUnit.SECONDS));
            worker.interrupt();
            worker.join(5000);
            Assertions.assertFalse(worker.isAlive());
            Assertions.assertTrue(interrupted.get());
            Assertions.assertTrue(failure.get() instanceof ModelInvocationException);
            Assertions.assertFalse(((ModelInvocationException) failure.get()).isRetryable());
            Assertions.assertEquals(1, adapter.getAttempts());
        } finally {
            worker.interrupt();
            worker.join(5000);
        }
    }

    private static ModelInvocationException httpFailure(int status, String header) {
        return ModelInvocationException.fromHttpStatus(
                "OPENAI",
                "test-model",
                status,
                "secret response",
                new BasicHeader("Retry-After", header));
    }

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
