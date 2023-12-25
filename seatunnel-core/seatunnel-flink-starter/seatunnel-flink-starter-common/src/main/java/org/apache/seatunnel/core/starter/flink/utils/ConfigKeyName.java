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

package org.apache.seatunnel.core.starter.flink.utils;

public class ConfigKeyName {

    private ConfigKeyName() {
        throw new IllegalStateException("Utility class");
    }

    @Deprecated public static final String TIME_CHARACTERISTIC = "execution.time-characteristic";
    @Deprecated public static final String BUFFER_TIMEOUT_MILLIS = "execution.buffer.timeout";
    @Deprecated public static final String PARALLELISM = "execution.parallelism";
    @Deprecated public static final String MAX_PARALLELISM = "execution.max-parallelism";

    @Deprecated public static final String CHECKPOINT_INTERVAL = "execution.checkpoint.interval";
    @Deprecated public static final String CHECKPOINT_MODE = "execution.checkpoint.mode";
    @Deprecated public static final String CHECKPOINT_TIMEOUT = "execution.checkpoint.timeout";
    @Deprecated public static final String CHECKPOINT_DATA_URI = "execution.checkpoint.data-uri";

    @Deprecated
    public static final String MAX_CONCURRENT_CHECKPOINTS = "execution.max-concurrent-checkpoints";

    @Deprecated
    public static final String CHECKPOINT_CLEANUP_MODE = "execution.checkpoint.cleanup-mode";

    @Deprecated
    public static final String MIN_PAUSE_BETWEEN_CHECKPOINTS = "execution.checkpoint.min-pause";

    @Deprecated
    public static final String FAIL_ON_CHECKPOINTING_ERRORS = "execution.checkpoint.fail-on-error";

    @Deprecated public static final String RESTART_STRATEGY = "execution.restart.strategy";
    @Deprecated public static final String RESTART_ATTEMPTS = "execution.restart.attempts";

    @Deprecated
    public static final String RESTART_DELAY_BETWEEN_ATTEMPTS =
            "execution.restart.delayBetweenAttempts";

    @Deprecated
    public static final String RESTART_FAILURE_INTERVAL = "execution.restart.failureInterval";

    @Deprecated public static final String RESTART_FAILURE_RATE = "execution.restart.failureRate";

    @Deprecated
    public static final String RESTART_DELAY_INTERVAL = "execution.restart.delayInterval";

    @Deprecated
    public static final String MAX_STATE_RETENTION_TIME = "execution.query.state.max-retention";

    @Deprecated
    public static final String MIN_STATE_RETENTION_TIME = "execution.query.state.min-retention";

    @Deprecated public static final String STATE_BACKEND = "execution.state.backend";
    public static final String PLANNER = "execution.planner";
}
