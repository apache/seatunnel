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

import java.io.Serializable;

/**
 * Scheduling and Resource Allocation Logic <br>
 * <br>
 * 1. <b>Time Weight Design</b>: Time weight affects scheduling priority, where recent data has
 * higher weights <br>
 * and older data decays. Weights follow a distribution of {4, 2, 2, 1, 1}, normalized as: <br>
 * `timeWeight = currentWeight / 10.0`. When fewer than 5 data points are available (e.g., during
 * startup), <br>
 * weights are dynamically adjusted. <br>
 * <br>
 * 2. <b>Resource Utilization Calculation</b>: CPU and memory idle rates are combined using weighted
 * evaluation: <br>
 * `resourceIdleRate = ((1 - cpuUtilization) * cpuWeight + (1 - memoryUtilization) * memoryWeight) /
 * (cpuWeight + memoryWeight)`. <br>
 * The weights (e.g., `cpuWeight = 0.6`, `memoryWeight = 0.4`) are customizable based on specific
 * needs. <br>
 * <br>
 * 3. <b>Time Decay and Priority Formula</b>: With time-weight decay applied, the aggregated
 * resource idle rate is: <br>
 * `aggregatedResourceIdleRate = Σ[((1 - cpuUtilization[i]) * cpuWeight + (1 - memoryUtilization[i])
 * * memoryWeight) / (cpuWeight + memoryWeight) * timeWeight[i]]` for the latest 5 data points. <br>
 * <br>
 * 4. <b>Dynamic Adjustment During Slot Allocation</b>: Allocating slots updates idle rates
 * dynamically: <br>
 * - Per-slot resource usage: `perSlotResourceUsage = (1 - aggregatedResourceIdleRate) /
 * allocatedSlots`. <br>
 * - Updated idle rate: `updatedIdleRate = aggregatedResourceIdleRate - perSlotResourceUsage`. <br>
 * A default slot usage of 10% prevents over-allocation and ensures reasonable load distribution
 * until monitoring data refines the estimate. <br>
 * <br>
 * 5. <b>Balance Factor</b>: To avoid resource concentration, a balance factor adjusts scheduling
 * priority: <br>
 * `balanceFactor = 1 - (slotsUsed / totalSlots)`. The overall priority is weighted as: <br>
 * `weightedPriority = alpha * updatedIdleRate + beta * balanceFactor`, where `alpha` (e.g., 0.7)
 * emphasizes resource utilization and `beta` (e.g., 0.3) ensures load balance. <br>
 * <br>
 * 6. <b>Dynamic Adjustment Logic</b>: Periodic collection of CPU and memory utilization data
 * (latest 5 entries) <br>
 * ensures real-time updates. Slot allocations dynamically balance resources, preventing overloads
 * and refining decisions. <br>
 * <br>
 * Example: If Node A has 10 free slots and Node B has 20, but Node A consistently shows higher
 * priority after <br>
 * applying these formulas, it may indicate suboptimal slot configuration on Node B, requiring
 * adjustment. <br>
 */
public enum AllocateStrategy implements Serializable {
    SYSTEM_LOAD,
    SLOT_RATIO,
    RANDOM
}
