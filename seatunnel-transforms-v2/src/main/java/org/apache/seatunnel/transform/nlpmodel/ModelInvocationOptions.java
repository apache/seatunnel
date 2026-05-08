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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

public class ModelInvocationOptions {

    public static final int DEFAULT_RETRY_MAX_ATTEMPTS = 1;
    public static final long DEFAULT_RETRY_BACKOFF_MS = 1000L;
    public static final long DEFAULT_RETRY_MAX_BACKOFF_MS = 10000L;
    public static final int DEFAULT_REQUEST_TIMEOUT_MS = 20000;

    private final int retryMaxAttempts;
    private final long retryBackoffMs;
    private final long retryMaxBackoffMs;
    private final int requestTimeoutMs;

    public ModelInvocationOptions(
            int retryMaxAttempts,
            long retryBackoffMs,
            long retryMaxBackoffMs,
            int requestTimeoutMs) {
        this.retryMaxAttempts = Math.max(1, retryMaxAttempts);
        this.retryBackoffMs = Math.max(0L, retryBackoffMs);
        this.retryMaxBackoffMs = Math.max(0L, retryMaxBackoffMs);
        this.requestTimeoutMs = Math.max(1, requestTimeoutMs);
    }

    public static ModelInvocationOptions defaults() {
        return new ModelInvocationOptions(
                DEFAULT_RETRY_MAX_ATTEMPTS,
                DEFAULT_RETRY_BACKOFF_MS,
                DEFAULT_RETRY_MAX_BACKOFF_MS,
                DEFAULT_REQUEST_TIMEOUT_MS);
    }

    public static ModelInvocationOptions fromConfig(ReadonlyConfig config) {
        return new ModelInvocationOptions(
                config.get(ModelTransformConfig.MODEL_RETRY_MAX_ATTEMPTS),
                config.get(ModelTransformConfig.MODEL_RETRY_BACKOFF_MS),
                config.get(ModelTransformConfig.MODEL_RETRY_MAX_BACKOFF_MS),
                config.get(ModelTransformConfig.MODEL_REQUEST_TIMEOUT_MS));
    }

    public int getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    public long getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public long getRetryMaxBackoffMs() {
        return retryMaxBackoffMs;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }
}
