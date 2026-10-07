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

package org.apache.seatunnel.common.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link RetryUtils#retryWithException(RetryUtils.Execution, RetryUtils.RetryMaterial)}.
 *
 * <p>Every {@link RetryUtils.RetryMaterial} used here is built through a constructor with an
 * implicit sleep time of {@code 0}, so no test in this class sleeps or waits.
 */
class RetryUtilsTest {

    /** Only this exception is classified as retriable by {@link #retriableOnly()}. */
    private static class RetriableException extends Exception {

        private static final long serialVersionUID = 1L;

        RetriableException(String message) {
            super(message);
        }
    }

    private static RetryUtils.RetryCondition<Exception> retriableOnly() {
        return e -> e instanceof RetriableException;
    }

    /**
     * A failure classified as not retriable, with {@code shouldThrowException = false}, must be
     * executed exactly once and answer {@code null}: re-executing it is observable for executions
     * that have side effects (HTTP POST, bulk index/insert).
     */
    @Test
    void testNonRetriableFailureWithoutThrowExecutesExactlyOnce() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        RetryUtils.RetryMaterial retryMaterial =
                new RetryUtils.RetryMaterial(3, false, retriableOnly());
        IllegalStateException notRetriable = new IllegalStateException("not retriable");

        String result =
                RetryUtils.retryWithException(
                        () -> {
                            invocations.incrementAndGet();
                            throw notRetriable;
                        },
                        retryMaterial);

        Assertions.assertNull(result, "shouldThrowException=false must return null");
        Assertions.assertEquals(
                1,
                invocations.get(),
                "a non-retriable failure must not invoke the execution again");
    }

    /**
     * The neighbouring case: a non-retriable failure with {@code shouldThrowException = true} must
     * propagate the original exception instance, again after a single execution.
     */
    @Test
    void testNonRetriableFailureWithThrowPropagatesOriginalException() {
        AtomicInteger invocations = new AtomicInteger();
        RetryUtils.RetryMaterial retryMaterial =
                new RetryUtils.RetryMaterial(3, true, retriableOnly());
        IllegalStateException notRetriable = new IllegalStateException("not retriable");

        IllegalStateException thrown =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                RetryUtils.retryWithException(
                                        () -> {
                                            invocations.incrementAndGet();
                                            throw notRetriable;
                                        },
                                        retryMaterial));

        Assertions.assertSame(notRetriable, thrown);
        Assertions.assertEquals(1, invocations.get());
    }

    /**
     * A retriable failure is still retried: with {@code retryTimes = 3} the execution runs three
     * times and, with {@code shouldThrowException = false}, the call answers {@code null}.
     */
    @Test
    void testRetriableFailureIsRetriedUpToRetryTimes() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        RetryUtils.RetryMaterial retryMaterial =
                new RetryUtils.RetryMaterial(3, false, retriableOnly());

        String result =
                RetryUtils.retryWithException(
                        () -> {
                            invocations.incrementAndGet();
                            throw new RetriableException("retriable");
                        },
                        retryMaterial);

        Assertions.assertNull(result);
        Assertions.assertEquals(3, invocations.get());
    }

    /** A successful first attempt is never retried. */
    @Test
    void testSuccessOnFirstAttemptDoesNotRetry() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        RetryUtils.RetryMaterial retryMaterial =
                new RetryUtils.RetryMaterial(3, true, retriableOnly());

        String result =
                RetryUtils.retryWithException(
                        () -> {
                            invocations.incrementAndGet();
                            return "ok";
                        },
                        retryMaterial);

        Assertions.assertEquals("ok", result);
        Assertions.assertEquals(1, invocations.get());
    }

    /** A retriable failure followed by a success returns the produced value. */
    @Test
    void testRetriableFailureThenSuccessReturnsValue() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        RetryUtils.RetryMaterial retryMaterial =
                new RetryUtils.RetryMaterial(3, false, retriableOnly());

        String result =
                RetryUtils.retryWithException(
                        () -> {
                            if (invocations.incrementAndGet() == 1) {
                                throw new RetriableException("transient");
                            }
                            return "recovered";
                        },
                        retryMaterial);

        Assertions.assertEquals("recovered", result);
        Assertions.assertEquals(2, invocations.get());
    }

    /**
     * Without a retry condition there is nothing that can classify the failure as not retriable, so
     * the historical "always retry" behaviour must stay unchanged.
     */
    @Test
    void testNullRetryConditionStillRetries() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        RetryUtils.RetryMaterial retryMaterial = new RetryUtils.RetryMaterial(2, false, null);

        String result =
                RetryUtils.retryWithException(
                        () -> {
                            invocations.incrementAndGet();
                            throw new IllegalStateException("boom");
                        },
                        retryMaterial);

        Assertions.assertNull(result);
        Assertions.assertEquals(2, invocations.get());
    }

    /** A negative retry count is rejected before anything is executed. */
    @Test
    void testNegativeRetryTimesIsRejected() {
        AtomicInteger invocations = new AtomicInteger();
        RetryUtils.RetryMaterial retryMaterial =
                new RetryUtils.RetryMaterial(-1, false, retriableOnly());

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        RetryUtils.retryWithException(
                                () -> {
                                    invocations.incrementAndGet();
                                    return "never";
                                },
                                retryMaterial));
        Assertions.assertEquals(0, invocations.get());
    }
}
