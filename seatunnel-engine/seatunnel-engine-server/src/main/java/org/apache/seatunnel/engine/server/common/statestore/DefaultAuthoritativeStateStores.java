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
import org.apache.seatunnel.engine.server.common.jar.ConnectorJarReferenceStateStore;
import org.apache.seatunnel.engine.server.common.statestore.cleanup.PendingPipelineCleanupStore;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;
import org.apache.seatunnel.engine.server.common.statestore.runtime.RuntimeStateStore;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import java.util.Map;
import java.util.Objects;

/** Default immutable implementation of {@link AuthoritativeStateStores}. */
public class DefaultAuthoritativeStateStores implements AuthoritativeStateStores {

    private final RuntimeStateStore<Long, JobInfo> runningJobInfoStore;
    private final RuntimeStateStore<Object, Object> runningJobStateStore;
    private final RuntimeStateStore<Object, Long[]> runningJobStateTimestampsStore;
    private final RuntimeStateStore<PipelineLocation, Map<TaskGroupLocation, SlotProfile>>
            ownedSlotProfilesStore;
    private final CounterStateStore<String> checkpointCounterStore;
    private final PendingPipelineCleanupStore pendingPipelineCleanupStore;
    private final ConnectorJarReferenceStateStore connectorJarReferenceStateStore;

    public DefaultAuthoritativeStateStores(
            RuntimeStateStore<Long, JobInfo> runningJobInfoStore,
            RuntimeStateStore<Object, Object> runningJobStateStore,
            RuntimeStateStore<Object, Long[]> runningJobStateTimestampsStore,
            RuntimeStateStore<PipelineLocation, Map<TaskGroupLocation, SlotProfile>>
                    ownedSlotProfilesStore,
            CounterStateStore<String> checkpointCounterStore,
            PendingPipelineCleanupStore pendingPipelineCleanupStore,
            ConnectorJarReferenceStateStore connectorJarReferenceStateStore) {
        this.runningJobInfoStore =
                Objects.requireNonNull(runningJobInfoStore, "runningJobInfoStore");
        this.runningJobStateStore =
                Objects.requireNonNull(runningJobStateStore, "runningJobStateStore");
        this.runningJobStateTimestampsStore =
                Objects.requireNonNull(
                        runningJobStateTimestampsStore, "runningJobStateTimestampsStore");
        this.ownedSlotProfilesStore =
                Objects.requireNonNull(ownedSlotProfilesStore, "ownedSlotProfilesStore");
        this.checkpointCounterStore =
                Objects.requireNonNull(checkpointCounterStore, "checkpointCounterStore");
        this.pendingPipelineCleanupStore =
                Objects.requireNonNull(pendingPipelineCleanupStore, "pendingPipelineCleanupStore");
        this.connectorJarReferenceStateStore =
                Objects.requireNonNull(
                        connectorJarReferenceStateStore, "connectorJarReferenceStateStore");
    }

    @Override
    public RuntimeStateStore<Long, JobInfo> runningJobInfoStore() {
        return runningJobInfoStore;
    }

    @Override
    public RuntimeStateStore<Object, Object> runningJobStateStore() {
        return runningJobStateStore;
    }

    @Override
    public RuntimeStateStore<Object, Long[]> runningJobStateTimestampsStore() {
        return runningJobStateTimestampsStore;
    }

    @Override
    public RuntimeStateStore<PipelineLocation, Map<TaskGroupLocation, SlotProfile>>
            ownedSlotProfilesStore() {
        return ownedSlotProfilesStore;
    }

    @Override
    public CounterStateStore<String> checkpointCounterStore() {
        return checkpointCounterStore;
    }

    @Override
    public PendingPipelineCleanupStore pendingPipelineCleanupStore() {
        return pendingPipelineCleanupStore;
    }

    @Override
    public ConnectorJarReferenceStateStore connectorJarReferenceStateStore() {
        return connectorJarReferenceStateStore;
    }
}
