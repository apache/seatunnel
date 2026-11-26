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

package org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy;

import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import java.util.List;
import java.util.Optional;

/** Slot allocation strategy interface. */
public interface SlotAllocationStrategy {

    /**
     * Selects a worker for an allocation request that is not associated with a job.
     *
     * @param availableWorkers workers that can satisfy the request
     * @return the selected worker, if any
     */
    Optional<WorkerProfile> selectWorker(List<WorkerProfile> availableWorkers);

    /**
     * Selects a worker for the given job.
     *
     * <p>A strategy instance is shared by every job running on the master, so implementations that
     * keep allocation bookkeeping must scope that bookkeeping to the job. The default
     * implementation keeps the unscoped behaviour for strategies that do not track state.
     *
     * @param availableWorkers workers that can satisfy the request
     * @param jobId job the request belongs to
     * @return the selected worker, if any
     */
    default Optional<WorkerProfile> selectWorker(List<WorkerProfile> availableWorkers, long jobId) {
        return selectWorker(availableWorkers);
    }

    /**
     * Drops the allocation bookkeeping of one job, when the job starts a new allocation round or
     * reaches a terminal state. Other jobs' state must not be affected.
     *
     * @param jobId job whose state should be dropped
     */
    default void resetJobState(long jobId) {}
}
