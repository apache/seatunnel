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

package org.apache.seatunnel.engine.server.common.statestore;

import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.common.statestore.cleanup.PendingPipelineCleanupStore;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;
import org.apache.seatunnel.engine.server.common.statestore.runtime.RuntimeStateStore;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import java.util.Map;

/**
 * Bundle of authoritative control state that a new leader must trust during leader handoff.
 *
 * <p>This layer groups states that are likely to live on top of a consensus layer. The actual
 * storage backend may still be a local store, but the responsibility for deciding what is
 * authoritative belongs more strongly to this group.
 */
public interface AuthoritativeStateStores {

    /**
     * Returns the store for running job info.
     *
     * @return running job info store
     */
    RuntimeStateStore<Long, JobInfo> runningJobInfoStore();

    /**
     * Returns the store for running job, pipeline, or task execution state.
     *
     * <p>The current engine structure mixes multiple key/value shapes, so this contract remains
     * {@code Object}-based for now.
     *
     * @return running job state store
     */
    RuntimeStateStore<Object, Object> runningJobStateStore();

    /**
     * Returns the store for timestamps associated with running state.
     *
     * @return running job state timestamps store
     */
    RuntimeStateStore<Object, Long[]> runningJobStateTimestampsStore();

    /**
     * Returns the store for owned slot profiles by pipeline.
     *
     * @return owned slot profiles store
     */
    RuntimeStateStore<PipelineLocation, Map<TaskGroupLocation, SlotProfile>>
            ownedSlotProfilesStore();

    /**
     * Returns the checkpoint ID counter store.
     *
     * <p>The current key format is a string composed from {@code jobId + pipelineId}.
     *
     * @return checkpoint counter store
     */
    CounterStateStore<String> checkpointCounterStore();

    /**
     * Returns the store for pending pipeline cleanup records.
     *
     * @return pending pipeline cleanup store
     */
    PendingPipelineCleanupStore pendingPipelineCleanupStore();
}
