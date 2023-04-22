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

package org.apache.seatunnel.core.starter.spark.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.spark.SparkConf;

import java.util.HashMap;
import java.util.Map;

public class CheckpointConfig {

    public static final String CHECKPOINT_LOCATION = "checkpointLocation";
    public static final String ASYNC_PROGRESS_TRACKING_ENABLED = "asyncProgressTrackingEnabled";
    public static final String ASYNC_PROGRESS_CHECK_POINTING_INTERVAL =
            "asyncProgressCheckpointingInterval";

    private static final String DEFAULT_CHECKPOINT_LOCATION = "/tmp";
    private static final long DEFAULT_CHECKPOINT_INTERVAL = 60000L;
    private static final String KEY_CHECK_POINTING_INTERVAL = "checkpoint.interval";
    private static final String KEY_TRIGGER_PROGRESS_INTERVAL = "triggerProcessingInterval";

    /** checkpointLocation */
    private String checkpointLocation;
    /** async checkpoint for microBatch */
    private boolean asyncProgressTrackingEnabled;
    /**
     * async checkpoint interval for microBatch, and asyncProgressTrackingEnabled should be true
     * unit is minute
     */
    private Long asyncProgressCheckpointingInterval;

    /**
     * micro batch processing data interval; <br>
     * if asyncProgressTrackingEnabled = false, checkpointInterval = processingInterval, it's
     * exactly-once <br>
     * if asyncProgressTrackingEnabled = ture, it's at at-most-once <br>
     */
    private Long triggerProcessingInterval;

    public String getCheckpointLocation() {
        return checkpointLocation;
    }

    public boolean asyncProgressTrackingEnabled() {
        return asyncProgressTrackingEnabled;
    }

    public Long asyncProgressCheckpointingInterval() {
        return asyncProgressCheckpointingInterval;
    }

    public Long getTriggerProcessingInterval() {
        return triggerProcessingInterval;
    }

    public CheckpointConfig(Config env, SparkConf sparkConf) {
        // checkpoint location
        if (env.hasPath(CHECKPOINT_LOCATION)) {
            this.checkpointLocation = env.getString(CHECKPOINT_LOCATION);
        } else {
            this.checkpointLocation =
                    sparkConf.get(CHECKPOINT_LOCATION, DEFAULT_CHECKPOINT_LOCATION);
        }
        // checkpoint interval default value
        if (env.hasPath(KEY_CHECK_POINTING_INTERVAL)) {
            this.triggerProcessingInterval = env.getLong(KEY_CHECK_POINTING_INTERVAL);
            this.asyncProgressCheckpointingInterval =
                    env.getLong(KEY_CHECK_POINTING_INTERVAL) / 60000;
        } else {
            this.triggerProcessingInterval =
                    sparkConf.getLong(KEY_CHECK_POINTING_INTERVAL, DEFAULT_CHECKPOINT_INTERVAL);
            this.asyncProgressCheckpointingInterval =
                    sparkConf.getLong(
                            KEY_CHECK_POINTING_INTERVAL, DEFAULT_CHECKPOINT_INTERVAL / 60000);
        }
        this.asyncProgressTrackingEnabled = false;

        // process interval
        if (env.hasPath(KEY_TRIGGER_PROGRESS_INTERVAL)) {
            this.triggerProcessingInterval = env.getLong(KEY_TRIGGER_PROGRESS_INTERVAL);
        }
        // async
        if (env.hasPath(ASYNC_PROGRESS_TRACKING_ENABLED)) {
            this.asyncProgressTrackingEnabled = env.getBoolean(ASYNC_PROGRESS_TRACKING_ENABLED);
        }
        if (env.hasPath(ASYNC_PROGRESS_CHECK_POINTING_INTERVAL)) {
            this.asyncProgressCheckpointingInterval =
                    env.getLong(ASYNC_PROGRESS_CHECK_POINTING_INTERVAL) / 60000;
        }
    }

    public Map<String, String> microBatchConf() {
        // its supported Trigger.processTime(CheckpointInterval)
        HashMap<String, String> checkpointConf = new HashMap<>();
        checkpointConf.put(CHECKPOINT_LOCATION, checkpointLocation);
        if (asyncProgressTrackingEnabled) {
            checkpointConf.put(ASYNC_PROGRESS_TRACKING_ENABLED, Boolean.TRUE.toString());
            checkpointConf.put(
                    ASYNC_PROGRESS_CHECK_POINTING_INTERVAL,
                    asyncProgressCheckpointingInterval.toString());
        }
        return checkpointConf;
    }

    public Map<String, String> continuousConf() {
        // todo: support continuous streaming
        HashMap<String, String> checkpointConf = new HashMap<>();
        checkpointConf.put(CHECKPOINT_LOCATION, checkpointLocation);
        return checkpointConf;
    }

    public Map<String, String> batchConf() {
        HashMap<String, String> checkpointConf = new HashMap<>();
        checkpointConf.put(CHECKPOINT_LOCATION, checkpointLocation);
        return checkpointConf;
    }
}
