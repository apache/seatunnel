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

package org.apache.seatunnel.engine.common.config.server;

import lombok.Data;

import java.io.Serializable;

/**
 * Production safety limits and rollout gates for the engine-managed Source runtime.
 *
 * <p>The feature is disabled by default. A running job never changes lane when this configuration
 * is reloaded.
 */
@Data
public class ManagedSourceRuntimeConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int MEBIBYTE = 1024 * 1024;
    public static final int KIBIBYTE = 1024;

    private boolean enabled;
    private int runtimeProtocolVersion = 1;

    private int readerMailboxMaxCommands = 1024;
    private long readerMailboxMaxBytes = 4L * MEBIBYTE;
    private int readerReservedControlCommands = 64;
    private long readerReservedControlBytes = 256L * KIBIBYTE;
    private long workerMailboxMaxBytes = 256L * MEBIBYTE;
    private int maxCommandPayloadBytes = 512 * KIBIBYTE;

    private int pollMaxRecords = 64;
    private long pollMaxBytes = 1024L * KIBIBYTE;
    private long pollSoftDurationMillis = 5L;
    private long pollHardDurationMillis = 1000L;
    private long pollCancellationTimeoutMillis = 30_000L;
    private long idleWaitMillis = 10L;

    private long admissionBudgetMillis = 5L;
    private long retryInitialBackoffMillis = 10L;
    private long retryMaxBackoffMillis = 1000L;
    private long commandRetryDeadlineMillis = 30_000L;

    private int coordinatorAsyncMaxConcurrency = 4;
    private int coordinatorAsyncIoThreads = 32;
    private int coordinatorAsyncCpuThreads = 4;
    private int coordinatorAsyncQueueCapacity = 4096;
    private int assignmentTrackerMaxEntries = 100_000;
    private long assignmentTrackerMaxBytes = 64L * MEBIBYTE;

    /** Validates limits before physical plan generation or task deployment. */
    public void validate() {
        positive(runtimeProtocolVersion, "runtime-protocol-version");
        positive(readerMailboxMaxCommands, "reader-mailbox-max-commands");
        positive(readerMailboxMaxBytes, "reader-mailbox-max-bytes");
        nonNegative(readerReservedControlCommands, "reader-reserved-control-commands");
        nonNegative(readerReservedControlBytes, "reader-reserved-control-bytes");
        positive(workerMailboxMaxBytes, "worker-mailbox-max-bytes");
        positive(maxCommandPayloadBytes, "max-command-payload-bytes");
        positive(pollMaxRecords, "poll-max-records");
        positive(pollMaxBytes, "poll-max-bytes");
        positive(pollSoftDurationMillis, "poll-soft-duration-ms");
        positive(pollHardDurationMillis, "poll-hard-duration-ms");
        positive(pollCancellationTimeoutMillis, "poll-cancellation-timeout-ms");
        positive(idleWaitMillis, "idle-wait-ms");
        positive(admissionBudgetMillis, "admission-budget-ms");
        positive(retryInitialBackoffMillis, "retry-initial-backoff-ms");
        positive(retryMaxBackoffMillis, "retry-max-backoff-ms");
        positive(commandRetryDeadlineMillis, "command-retry-deadline-ms");
        positive(coordinatorAsyncMaxConcurrency, "coordinator-async-max-concurrency");
        positive(coordinatorAsyncIoThreads, "coordinator-async-io-threads");
        positive(coordinatorAsyncCpuThreads, "coordinator-async-cpu-threads");
        positive(coordinatorAsyncQueueCapacity, "coordinator-async-queue-capacity");
        positive(assignmentTrackerMaxEntries, "assignment-tracker-max-entries");
        positive(assignmentTrackerMaxBytes, "assignment-tracker-max-bytes");

        if (readerReservedControlCommands >= readerMailboxMaxCommands) {
            throw new IllegalArgumentException(
                    "reader-reserved-control-commands must be less than reader-mailbox-max-commands");
        }
        if (readerReservedControlBytes >= readerMailboxMaxBytes) {
            throw new IllegalArgumentException(
                    "reader-reserved-control-bytes must be less than reader-mailbox-max-bytes");
        }
        if (readerReservedControlCommands < coordinatorAsyncMaxConcurrency) {
            throw new IllegalArgumentException(
                    "reader-reserved-control-commands must cover coordinator async concurrency");
        }
        if (maxCommandPayloadBytes > readerMailboxMaxBytes - readerReservedControlBytes) {
            throw new IllegalArgumentException(
                    "max-command-payload-bytes exceeds normal reader mailbox capacity");
        }
        if (pollSoftDurationMillis >= pollHardDurationMillis) {
            throw new IllegalArgumentException(
                    "poll-soft-duration-ms must be less than poll-hard-duration-ms");
        }
        if (retryInitialBackoffMillis > retryMaxBackoffMillis) {
            throw new IllegalArgumentException(
                    "retry-initial-backoff-ms must not exceed retry-max-backoff-ms");
        }
        if (retryMaxBackoffMillis > commandRetryDeadlineMillis) {
            throw new IllegalArgumentException(
                    "retry-max-backoff-ms must not exceed command-retry-deadline-ms");
        }
    }

    private static void positive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static void nonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
    }
}
