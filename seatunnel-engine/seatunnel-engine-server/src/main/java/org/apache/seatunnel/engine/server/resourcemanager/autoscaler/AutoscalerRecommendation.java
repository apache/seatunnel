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

package org.apache.seatunnel.engine.server.resourcemanager.autoscaler;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/**
 * A scaling recommendation produced by the autoscaler.
 *
 * <p>In recommendation-only mode, this is an advisory output that external systems can consume. In
 * actuation mode, the system may act on this recommendation directly.
 */
@Data
@Builder
public class AutoscalerRecommendation {

    /** The recommended scaling action. */
    private ScalingAction action;

    /** Human-readable reason for the recommendation. */
    private String reason;

    /** Current number of workers. */
    private int currentWorkerCount;

    /** Recommended number of workers after scaling. */
    private int targetWorkerCount;

    /** Whether this is a recommendation-only (not executing the action). */
    private boolean recommendationOnly;

    /** Current slot usage ratio across all workers (0.0-1.0). */
    private double slotUsageRatio;

    /** Average CPU load across all workers (0.0-1.0). */
    private double averageCpuLoad;

    /** Average memory load across all workers (0.0-1.0). */
    private double averageMemoryLoad;

    /** Timestamp when this recommendation was generated. */
    private Instant timestamp;

    /** Create a recommendation indicating no action is needed. */
    public static AutoscalerRecommendation noAction() {
        return AutoscalerRecommendation.builder()
                .action(ScalingAction.NO_ACTION)
                .reason("No scaling action needed")
                .currentWorkerCount(0)
                .targetWorkerCount(0)
                .recommendationOnly(true)
                .slotUsageRatio(0.0)
                .averageCpuLoad(0.0)
                .averageMemoryLoad(0.0)
                .timestamp(Instant.now())
                .build();
    }
}
