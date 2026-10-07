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

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class RetryUtils {

    /**
     * Execute the given execution with retry.
     *
     * <p>The given execution is executed {@code max(1, retryTimes)} times at most, and a failure is
     * only retried while the retry condition accepts it.
     *
     * @param execution execution to execute
     * @param retryMaterial retry material, defined the condition to retry
     * @param <T> result type
     * @return result of execution, or {@code null} either when the failure was rejected by the
     *     retry condition or when the attempts are exhausted, in both cases only for {@code
     *     shouldThrowException = false}
     * @throws IllegalArgumentException if the configured retry times is negative
     * @throws Exception the original exception when the failure was rejected by the retry condition
     *     and {@code shouldThrowException = true}
     * @throws RuntimeException when the attempts are exhausted and {@code shouldThrowException =
     *     true}, with the last failure as its cause
     */
    public static <T> T retryWithException(
            Execution<T, Exception> execution, RetryMaterial retryMaterial) throws Exception {
        final RetryCondition<Exception> retryCondition = retryMaterial.getRetryCondition();
        final int retryTimes = retryMaterial.getRetryTimes();

        if (retryMaterial.getRetryTimes() < 0) {
            throw new IllegalArgumentException("Retry times must be greater than 0");
        }
        Exception lastException;
        int i = 0;
        do {
            i++;
            try {
                return execution.execute();
            } catch (Exception e) {
                lastException = e;
                if (retryCondition != null && !retryCondition.canRetry(e)) {
                    if (retryMaterial.shouldThrowException()) {
                        throw e;
                    }
                    log.warn(
                            "Execution failed with {} and is not retriable, giving up after {} attempt(s)",
                            e.getClass().getName(),
                            i);
                    return null;
                } else {
                    // Otherwise it is retriable and we should retry
                    String attemptMessage =
                            "Failed to execute due to {}. Retrying attempt ({}/{}) after backoff of {} ms";
                    if (retryMaterial.getSleepTimeMillis() > 0) {
                        long backoff = retryMaterial.computeRetryWaitTimeMillis(i);
                        log.debug(
                                attemptMessage,
                                ExceptionUtils.getMessage(e),
                                i,
                                retryTimes,
                                backoff);
                        Thread.sleep(backoff);
                    } else {
                        log.info(attemptMessage, ExceptionUtils.getMessage(e), i, retryTimes, 0);
                    }
                }
            }
        } while (i < retryTimes);
        if (retryMaterial.shouldThrowException()) {
            throw new RuntimeException(
                    "Execute given execution failed after retry " + retryTimes + " times",
                    lastException);
        }
        return null;
    }

    public static class RetryMaterial {
        /** An arbitrary absolute maximum practical retry time. */
        public static final long MAX_RETRY_TIME_MS = TimeUnit.SECONDS.toMillis(20);

        /** The maximum retry time. */
        public static final long MAX_RETRY_TIME = 32;

        /**
         * Retry times, the given execution is executed at most {@code max(1, retryTimes)} times:
         * with {@code 0} or {@code 1} it is executed once, with {@code N > 1} at most {@code N}
         * times. A negative value is rejected by {@link RetryUtils#retryWithException}.
         */
        private final int retryTimes;
        /** If set true, the given execution will throw exception if it failed after retry. */
        private final boolean shouldThrowException;
        // this is the exception condition, can add result condition in the future.
        private final RetryCondition<Exception> retryCondition;

        private final boolean sleepTimeIncrease;

        /** The interval between each retry */
        private final long sleepTimeMillis;

        public RetryMaterial(
                int retryTimes,
                boolean shouldThrowException,
                RetryCondition<Exception> retryCondition) {
            this(retryTimes, shouldThrowException, retryCondition, 0);
        }

        public RetryMaterial(
                int retryTimes,
                boolean shouldThrowException,
                RetryCondition<Exception> retryCondition,
                long sleepTimeMillis) {
            this(retryTimes, shouldThrowException, retryCondition, sleepTimeMillis, false);
        }

        public RetryMaterial(
                int retryTimes,
                boolean shouldThrowException,
                RetryCondition<Exception> retryCondition,
                long sleepTimeMillis,
                boolean sleepTimeIncrease) {
            this.retryTimes = retryTimes;
            this.shouldThrowException = shouldThrowException;
            this.retryCondition = retryCondition;
            this.sleepTimeMillis = sleepTimeMillis;
            this.sleepTimeIncrease = sleepTimeIncrease;
        }

        public int getRetryTimes() {
            return retryTimes;
        }

        public boolean shouldThrowException() {
            return shouldThrowException;
        }

        public RetryCondition<Exception> getRetryCondition() {
            return retryCondition;
        }

        public long getSleepTimeMillis() {
            return sleepTimeMillis;
        }

        public long computeRetryWaitTimeMillis(int retryAttempts) {
            if (sleepTimeMillis < 0) {
                return 0;
            }
            if (!sleepTimeIncrease) {
                return sleepTimeMillis;
            }
            if (retryAttempts > MAX_RETRY_TIME) {
                // This would overflow the exponential algorithm ...
                return MAX_RETRY_TIME_MS;
            }
            long result = sleepTimeMillis << retryAttempts;
            return result < 0L ? MAX_RETRY_TIME_MS : Math.min(MAX_RETRY_TIME_MS, result);
        }
    }

    @FunctionalInterface
    public interface Execution<T, E extends Exception> {
        T execute() throws E;
    }

    public interface RetryCondition<T> {
        boolean canRetry(T input);
    }
}
