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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;

import lombok.Getter;

import java.io.Serializable;
import java.util.Objects;

@Getter
public class QueueConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String sqlitePath;
    private final int maxPollRecords;
    private final int resurrectBatchSize;
    private final long resurrectIntervalMs;
    private final int cleanupBatchSize;
    private final long ackedRetentionMs;

    public QueueConfig(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config");
        ConfigValidator.of(config).validate(EdgeAgentRuntimeOptionRules.queueRule());

        String path = config.get(EdgeAgentRuntimeOptions.QUEUE_SQLITE_PATH);
        if (path == null || path.trim().isEmpty()) {
            this.sqlitePath = EdgeAgentRuntimeOptions.QUEUE_SQLITE_PATH.defaultValue();
        } else {
            this.sqlitePath = path.trim();
        }

        this.maxPollRecords = config.get(EdgeAgentRuntimeOptions.QUEUE_POLL_BATCH_SIZE);
        requireAtLeast(this.maxPollRecords, 1, "queue.poll-batch-size");

        this.resurrectBatchSize = config.get(EdgeAgentRuntimeOptions.QUEUE_RESURRECT_BATCH_SIZE);
        requireAtLeast(this.resurrectBatchSize, 1, "queue.resurrect-batch-size");

        this.resurrectIntervalMs = config.get(EdgeAgentRuntimeOptions.QUEUE_RESURRECT_INTERVAL_MS);
        requireAtLeast(this.resurrectIntervalMs, 1L, "queue.resurrect-interval-ms");

        this.cleanupBatchSize = config.get(EdgeAgentRuntimeOptions.QUEUE_CLEANUP_BATCH_SIZE);
        requireAtLeast(this.cleanupBatchSize, 1, "queue.cleanup-batch-size");

        this.ackedRetentionMs = config.get(EdgeAgentRuntimeOptions.QUEUE_ACKED_RETENTION_MS);
        requireAtLeast(this.ackedRetentionMs, 0L, "queue.acked-retention-ms");
    }

    public static QueueConfig from(ReadonlyConfig config) {
        return new QueueConfig(config);
    }

    private static void requireAtLeast(int value, int min, String optionPath) {
        if (value < min) {
            throw new IllegalArgumentException(optionPath + " must be >= " + min + " when set.");
        }
    }

    private static void requireAtLeast(long value, long min, String optionPath) {
        if (value < min) {
            throw new IllegalArgumentException(optionPath + " must be >= " + min + " when set.");
        }
    }
}
