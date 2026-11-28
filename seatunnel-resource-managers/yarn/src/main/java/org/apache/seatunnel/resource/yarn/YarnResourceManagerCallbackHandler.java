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

package org.apache.seatunnel.resource.yarn;

import org.apache.seatunnel.resource.yarn.exception.YarnClientException;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.function.Consumer;

/**
 * Receives heartbeat callbacks for the current AM attempt. The driver matches containers to pending
 * requests and dispatches blocking launches and engine notifications to its executors.
 */
@Slf4j
final class YarnResourceManagerCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
    private static final float APPLICATION_PROGRESS = 0.0f;

    private final String clusterName;
    private final Consumer<Container> allocated;
    private final Consumer<ContainerStatus> completed;
    private final Consumer<YarnClientException> failed;

    YarnResourceManagerCallbackHandler(
            String clusterName,
            Consumer<Container> allocated,
            Consumer<ContainerStatus> completed,
            Consumer<YarnClientException> failed) {
        this.clusterName = clusterName;
        this.allocated = allocated;
        this.completed = completed;
        this.failed = failed;
    }

    /** Passes each granted container to the driver to claim for a pending request or release. */
    @Override
    public void onContainersAllocated(List<Container> containers) {
        containers.forEach(allocated);
    }

    /** Forwards the exit status and diagnostics used to complete or retire the tracked worker. */
    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        statuses.forEach(completed);
    }

    /** Reports an RM-requested shutdown as a driver failure for this application attempt. */
    @Override
    public void onShutdownRequest() {
        failed.accept(
                new YarnClientException(
                        "YARN ResourceManager requested shutdown of " + clusterName));
    }

    /** Preserves the heartbeat failure as the cause delivered to the driver. */
    @Override
    public void onError(Throwable cause) {
        failed.accept(
                new YarnClientException(
                        "Could not receive YARN container events for " + clusterName, cause));
    }

    @Override
    public float getProgress() {
        // The engine owns job progress. The AM only reports container lifecycle here.
        return APPLICATION_PROGRESS;
    }

    /** Logs resource updates for diagnostics. Worker resource requests are fixed at allocation. */
    @Override
    public void onContainersUpdated(List<UpdatedContainer> containers) {
        if (log.isDebugEnabled()) {
            for (UpdatedContainer update : containers) {
                Container container = update.getContainer();
                log.debug(
                        "YARN container {} update {} with resources {}",
                        container.getId(),
                        update.getUpdateType(),
                        container.getResource());
            }
        }
    }

    /**
     * Logs node health changes. Worker termination is reported by the container completion
     * callback.
     */
    @Override
    public void onNodesUpdated(List<NodeReport> nodes) {
        if (log.isDebugEnabled()) {
            for (NodeReport node : nodes) {
                log.debug("YARN node {} reported state {}", node.getNodeId(), node.getNodeState());
            }
        }
    }
}
