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

package org.apache.seatunnel.api.source.scheduler;

import org.apache.seatunnel.api.annotation.Experimental;

import java.io.Serializable;
import java.time.Duration;

/** Immutable execution and lifecycle policy for coordinator async work. */
@Experimental
public final class AsyncTaskOptions implements Serializable {

    private final Duration timeout;
    private final AsyncOverlapPolicy overlapPolicy;
    private final AsyncFailurePolicy failurePolicy;
    private final AsyncWorkerClass workerClass;

    private AsyncTaskOptions(Builder builder) {
        this.timeout = builder.timeout;
        this.overlapPolicy = builder.overlapPolicy;
        this.failurePolicy = builder.failurePolicy;
        this.workerClass = builder.workerClass;
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Async task timeout must be positive");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static AsyncTaskOptions defaults() {
        return builder().build();
    }

    public Duration getTimeout() {
        return timeout;
    }

    public AsyncOverlapPolicy getOverlapPolicy() {
        return overlapPolicy;
    }

    public AsyncFailurePolicy getFailurePolicy() {
        return failurePolicy;
    }

    public AsyncWorkerClass getWorkerClass() {
        return workerClass;
    }

    public static final class Builder {
        private Duration timeout = Duration.ofMinutes(1);
        private AsyncOverlapPolicy overlapPolicy = AsyncOverlapPolicy.COALESCE_ONE;
        private AsyncFailurePolicy failurePolicy = AsyncFailurePolicy.FAIL_SOURCE;
        private AsyncWorkerClass workerClass = AsyncWorkerClass.IO_BOUND;

        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder overlapPolicy(AsyncOverlapPolicy overlapPolicy) {
            this.overlapPolicy = overlapPolicy;
            return this;
        }

        public Builder failurePolicy(AsyncFailurePolicy failurePolicy) {
            this.failurePolicy = failurePolicy;
            return this;
        }

        public Builder workerClass(AsyncWorkerClass workerClass) {
            this.workerClass = workerClass;
            return this;
        }

        public AsyncTaskOptions build() {
            if (timeout == null
                    || overlapPolicy == null
                    || failurePolicy == null
                    || workerClass == null) {
                throw new IllegalArgumentException("Async task options must not contain null");
            }
            return new AsyncTaskOptions(this);
        }
    }
}
