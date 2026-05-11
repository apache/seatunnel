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

package org.apache.seatunnel.edge.agent.transport;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Configuration for {@link EdgeTransportClient}. */
public final class EdgeTransportConfig {

    private final long jobId;
    private final String authToken;
    private final int edgeIngressPort;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final int maxBatchSendAttempts;
    private final int maxCommitPollAttempts;
    private final long commitPollSleepMs;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final int maxFullDiscoveryCycles;

    private EdgeTransportConfig(Builder builder) {
        this.jobId = builder.jobId;
        this.authToken = Objects.requireNonNull(builder.authToken, "authToken");
        this.edgeIngressPort = builder.edgeIngressPort;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.readTimeoutMs = builder.readTimeoutMs;
        this.maxBatchSendAttempts = builder.maxBatchSendAttempts;
        this.maxCommitPollAttempts = builder.maxCommitPollAttempts;
        this.commitPollSleepMs = builder.commitPollSleepMs;
        this.initialBackoffMs = builder.initialBackoffMs;
        this.maxBackoffMs = builder.maxBackoffMs;
        this.maxFullDiscoveryCycles = builder.maxFullDiscoveryCycles;
    }

    public long getJobId() {
        return jobId;
    }

    public String getAuthToken() {
        return authToken;
    }

    public int getEdgeIngressPort() {
        return edgeIngressPort;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public int getMaxBatchSendAttempts() {
        return maxBatchSendAttempts;
    }

    public int getMaxCommitPollAttempts() {
        return maxCommitPollAttempts;
    }

    public long getCommitPollSleepMs() {
        return commitPollSleepMs;
    }

    public long getInitialBackoffMs() {
        return initialBackoffMs;
    }

    public long getMaxBackoffMs() {
        return maxBackoffMs;
    }

    public int getMaxFullDiscoveryCycles() {
        return maxFullDiscoveryCycles;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder with conservative defaults for MVP. */
    public static final class Builder {
        private long jobId;
        private String authToken;
        private int edgeIngressPort;
        private int connectTimeoutMs = 5000;
        private int readTimeoutMs = 30000;
        private int maxBatchSendAttempts = 64;
        private int maxCommitPollAttempts = 512;
        private long commitPollSleepMs = 200;
        private long initialBackoffMs = 100;
        private long maxBackoffMs = 5000;
        private int maxFullDiscoveryCycles = 8;

        public Builder jobId(long jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder authToken(String authToken) {
            this.authToken = authToken;
            return this;
        }

        public Builder edgeIngressPort(int edgeIngressPort) {
            this.edgeIngressPort = edgeIngressPort;
            return this;
        }

        public Builder connectTimeoutMs(int connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
            return this;
        }

        public Builder readTimeoutMs(int readTimeoutMs) {
            this.readTimeoutMs = readTimeoutMs;
            return this;
        }

        public Builder maxBatchSendAttempts(int maxBatchSendAttempts) {
            this.maxBatchSendAttempts = maxBatchSendAttempts;
            return this;
        }

        public Builder maxCommitPollAttempts(int maxCommitPollAttempts) {
            this.maxCommitPollAttempts = maxCommitPollAttempts;
            return this;
        }

        public Builder commitPollSleepMs(long commitPollSleepMs) {
            this.commitPollSleepMs = commitPollSleepMs;
            return this;
        }

        public Builder initialBackoffMs(long initialBackoffMs) {
            this.initialBackoffMs = initialBackoffMs;
            return this;
        }

        public Builder maxBackoffMs(long maxBackoffMs) {
            this.maxBackoffMs = maxBackoffMs;
            return this;
        }

        public Builder maxFullDiscoveryCycles(int maxFullDiscoveryCycles) {
            this.maxFullDiscoveryCycles = maxFullDiscoveryCycles;
            return this;
        }

        public EdgeTransportConfig build() {
            if (jobId <= 0) {
                throw new IllegalArgumentException("jobId must be positive");
            }
            if (edgeIngressPort <= 0 || edgeIngressPort > 65535) {
                throw new IllegalArgumentException("edgeIngressPort must be a valid TCP port");
            }
            return new EdgeTransportConfig(this);
        }
    }

    static long computeBackoffMillis(long attempt, long initial, long max) {
        long doubled = initial << attempt;
        if (doubled <= 0) {
            return max;
        }
        return Math.min(max, doubled);
    }

    static void sleepQuiet(long millis) throws InterruptedException {
        if (millis <= 0) {
            return;
        }
        Thread.sleep(millis);
    }

    static long millisElapsed(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }
}
