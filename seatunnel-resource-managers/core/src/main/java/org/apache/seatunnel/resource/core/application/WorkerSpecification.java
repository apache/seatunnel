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

package org.apache.seatunnel.resource.core.application;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Fixed worker resource envelope for an application. */
@Getter
@EqualsAndHashCode
public final class WorkerSpecification {
    /** Total container memory in MiB, including JVM overhead. */
    private final int memoryMb;
    /** Requested CPU core count. */
    private final int cpuCores;
    /** Fixed number of Engine execution slots exposed by this worker. */
    private final int slots;
    /**
     * Defines the fixed capacity to request for each worker container.
     *
     * @param memoryMb positive total memory in MiB
     * @param cpuCores positive CPU core count
     * @param slots positive fixed execution-slot count
     * @throws IllegalArgumentException if any resource is nonpositive
     */
    public WorkerSpecification(int memoryMb, int cpuCores, int slots) {
        if (memoryMb <= 0 || cpuCores <= 0 || slots <= 0) {
            throw new IllegalArgumentException(
                    "Worker memory, CPU cores and slots must be positive");
        }
        this.memoryMb = memoryMb;
        this.cpuCores = cpuCores;
        this.slots = slots;
    }
}
