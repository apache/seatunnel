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

public class RetryUtils {

    /**
     * Execute the given execution with retry
     *
     * @param execution     execution to execute
     * @param retryMaterial retry material, defined the condition to retry
     * @param <T>           result type
     * @return result of execution
     */
    public static <T> T retryWithException(Execution<T, Exception> execution, RetryMaterial retryMaterial) throws Exception {
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
                } else if (retryMaterial.getSleepTimeMillis() > 0) {
                    Thread.sleep(retryMaterial.getSleepTimeMillis());
                }
            }
        } while (i <= retryTimes);
        if (retryMaterial.shouldThrowException()) {
            throw new RuntimeException("Execute given execution failed after retry " + retryTimes + " times", lastException);
        }
        return null;
    }

    public static class RetryMaterial {
        /**
         * Retry times, if you set it to 1, the given execution will be executed twice.
         * Should be greater than 0.
         */
        private final int retryTimes;
        /**
         * If set true, the given execution will throw exception if it failed after retry.
         */
        private final boolean shouldThrowException;
        // this is the exception condition, can add result condition in the future.
        private final RetryCondition<Exception> retryCondition;

        /**
         * The interval between each retry
         */
        private final long sleepTimeMillis;

        public RetryMaterial(int retryTimes, boolean shouldThrowException, RetryCondition<Exception> retryCondition) {
            this(retryTimes, shouldThrowException, retryCondition, 0);
        }

        public RetryMaterial(int retryTimes, boolean shouldThrowException,
                             RetryCondition<Exception> retryCondition, long sleepTimeMillis) {
            this.retryTimes = retryTimes;
            this.shouldThrowException = shouldThrowException;
            this.retryCondition = retryCondition;
            this.sleepTimeMillis = sleepTimeMillis;
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
    }

    @FunctionalInterface
    public interface Execution<T, E extends Exception> {
        T execute() throws E;
    }

    public interface RetryCondition<T> {
        boolean canRetry(T input);
    }

}
