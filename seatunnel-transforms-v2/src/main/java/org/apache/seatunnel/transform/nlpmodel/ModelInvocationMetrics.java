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

public interface ModelInvocationMetrics {

    ModelInvocationMetrics NOOP = new NoopModelInvocationMetrics();

    void recordRequest(String provider, String model);

    void recordFailure(String provider, String model, ModelInvocationErrorType errorType);

    void recordRetry(String provider, String model, ModelInvocationErrorType errorType);

    void recordRetryExhausted(String provider, String model, ModelInvocationErrorType errorType);

    void recordResponseCountMismatch(String provider, String model);

    void recordGeneratedOutputs(String provider, String model, int outputCount);

    void recordLatency(String provider, String model, long elapsedMs);

    void recordCacheHit(String provider, String model, int hitCount);

    void recordCacheMiss(String provider, String model, int missCount);

    class NoopModelInvocationMetrics implements ModelInvocationMetrics {

        @Override
        public void recordRequest(String provider, String model) {}

        @Override
        public void recordFailure(
                String provider, String model, ModelInvocationErrorType errorType) {}

        @Override
        public void recordRetry(
                String provider, String model, ModelInvocationErrorType errorType) {}

        @Override
        public void recordRetryExhausted(
                String provider, String model, ModelInvocationErrorType errorType) {}

        @Override
        public void recordResponseCountMismatch(String provider, String model) {}

        @Override
        public void recordGeneratedOutputs(String provider, String model, int outputCount) {}

        @Override
        public void recordLatency(String provider, String model, long elapsedMs) {}

        @Override
        public void recordCacheHit(String provider, String model, int hitCount) {}

        @Override
        public void recordCacheMiss(String provider, String model, int missCount) {}
    }
}
