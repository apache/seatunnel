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

package org.apache.seatunnel.edge.agent.starter.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class EdgeAgentRuntimeOptions {

    public static final Option<String> AGENT_ID =
            Options.key("id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Logical identifier for the edge agent process. Auto-generated when"
                                    + " omitted.");

    public static final Option<String> DELIVERY_GUARANTEE =
            Options.key("delivery-guarantee")
                    .stringType()
                    .defaultValue("BEST_EFFORT")
                    .withDescription(
                            "Agent-wide outbound delivery mode. Default BEST_EFFORT. Only BEST_EFFORT"
                                    + " is supported in this release: local SQLite WAL with retry"
                                    + " until RECEIVED; the same row may be sent more than once"
                                    + " (downstream should be idempotent).");

    public static final Option<String> QUEUE_SQLITE_PATH =
            Options.key("sqlite-path")
                    .stringType()
                    .defaultValue("data/wal.db")
                    .withDescription(
                            "Path to the SQLite WAL database file. Default: data/wal.db (parent"
                                    + " directory data/ is created automatically; relative to"
                                    + " agent working directory).");

    public static final Option<Integer> QUEUE_POLL_BATCH_SIZE =
            Options.key("poll-batch-size")
                    .intType()
                    .defaultValue(128)
                    .withDescription("Number of WAL rows claimed per poll iteration.");

    public static final Option<Long> QUEUE_ACKED_RETENTION_MS =
            Options.key("acked-retention-ms")
                    .longType()
                    .defaultValue(0L)
                    .withDescription(
                            "Retention window for acknowledged WAL entries (ms); 0 = no retention.");

    public static final Option<Integer> QUEUE_CLEANUP_BATCH_SIZE =
            Options.key("cleanup-batch-size")
                    .intType()
                    .defaultValue(128)
                    .withDescription("Maximum rows to delete per WAL cleanup pass.");

    public static final Option<Long> RUNTIME_IDLE_SLEEP_MS =
            Options.key("idle-sleep-ms")
                    .longType()
                    .defaultValue(200L)
                    .withDescription(
                            "Scheduler sleep (ms) when a loop iteration makes no progress.");

    public static final Option<Integer> QUEUE_RESURRECT_BATCH_SIZE =
            Options.key("resurrect-batch-size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Maximum SENDING WAL rows reset to PENDING per resurrection pass.");

    public static final Option<Long> QUEUE_RESURRECT_INTERVAL_MS =
            Options.key("resurrect-interval-ms")
                    .longType()
                    .defaultValue(60000L)
                    .withDescription(
                            "Interval in milliseconds between SENDING row resurrection passes.");

    public static final Option<Integer> RUNTIME_BULK_MAX_SIZE =
            Options.key("bulk-max-size")
                    .intType()
                    .defaultValue(256)
                    .withDescription(
                            "Maximum records in the in-memory batch before flushing to WAL.");

    public static final Option<Long> RUNTIME_FLUSH_INTERVAL_MS =
            Options.key("flush-interval-ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("Maximum time (ms) before the batch is flushed to WAL.");

    public static final Option<Integer> RETRY_MAX_ATTEMPTS =
            Options.key("max-attempts")
                    .intType()
                    .defaultValue(16)
                    .withDescription("Maximum retry attempts per WAL row before dead-lettering.");

    public static final Option<Long> RETRY_BACKOFF_MS =
            Options.key("backoff-ms")
                    .longType()
                    .defaultValue(250L)
                    .withDescription(
                            "Base backoff delay (ms) in the scheduler after a failed WAL batch"
                                    + " send (distinct from transport reconnect backoff).");

    public static final Option<Long> RETRY_BACKOFF_MAX_MS =
            Options.key("backoff-max-ms")
                    .longType()
                    .defaultValue(300000L)
                    .withDescription("Upper bound (ms) for exponential backoff; omit for no cap.");
}
