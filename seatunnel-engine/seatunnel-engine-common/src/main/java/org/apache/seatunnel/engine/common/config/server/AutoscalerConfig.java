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

@Data
public class AutoscalerConfig implements Serializable {

    private boolean enabled = false;

    /** Minimum number of workers in the cluster. */
    private int minWorkers = 1;

    /** Maximum number of workers in the cluster. */
    private int maxWorkers = 10;

    /** Cooldown period in seconds after a scale-out before another scale-out can happen. */
    private int scaleOutCooldownSeconds = 300;

    /** Cooldown period in seconds after a scale-in before another scale-in can happen. */
    private int scaleInCooldownSeconds = 600;

    /** Stabilization window in seconds: metrics must stay above/below threshold for this duration. */
    private int stabilizationWindowSeconds = 120;

    /**
     * CPU utilization threshold (0.0-1.0) above which scale-out is considered. Average CPU across
     * all workers must exceed this for the stabilization window.
     */
    private double cpuScaleOutThreshold = 0.75;

    /**
     * CPU utilization threshold (0.0-1.0) below which scale-in is considered. Average CPU across
     * all workers must be below this for the stabilization window.
     */
    private double cpuScaleInThreshold = 0.25;

    /**
     * Memory utilization threshold (0.0-1.0) above which scale-out is considered. Average memory
     * across all workers must exceed this for the stabilization window.
     */
    private double memoryScaleOutThreshold = 0.80;

    /**
     * Memory utilization threshold (0.0-1.0) below which scale-in is considered. Average memory
     * across all workers must be below this for the stabilization window.
     */
    private double memoryScaleInThreshold = 0.30;

    /**
     * Slot usage ratio threshold (0.0-1.0) above which scale-out is considered. Ratio of assigned
     * slots to total slots across all workers must exceed this for the stabilization window.
     */
    private double slotUsageScaleOutThreshold = 0.80;

    /**
     * Slot usage ratio threshold (0.0-1.0) below which scale-in is considered. Ratio of assigned
     * slots to total slots across all workers must be below this for the stabilization window.
     */
    private double slotUsageScaleInThreshold = 0.20;

    /** Whether this autoscaler is recommendation-only (does not perform actual scaling). */
    private boolean recommendationOnly = true;

    /** Evaluation interval in seconds for the autoscaler control loop. */
    private int evaluationIntervalSeconds = 30;
}