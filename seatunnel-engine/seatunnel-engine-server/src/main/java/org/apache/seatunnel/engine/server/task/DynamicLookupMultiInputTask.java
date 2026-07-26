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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupAction;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.dag.physical.MultiInputTaskDeploymentDescriptor;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import lombok.NonNull;

import java.net.URL;
import java.util.List;
import java.util.Set;

/**
 * Deployment-safe shell for a future dynamic lookup multi-input task.
 *
 * <p>The standard JobMaster deployment path rejects a physical plan containing this task before
 * any source starts. Direct invocation also fails fast until exchange, barrier, state, and lookup
 * semantics are implemented by their separately gated proposals.
 */
public final class DynamicLookupMultiInputTask extends AbstractTask {

    private static final long serialVersionUID = 1L;

    /**
     * Immutable Phase-0 lookup declaration.
     */
    private final DynamicLookupAction action;

    /**
     * Explicit input-port and physical-channel declaration for this subtask.
     */
    private final MultiInputTaskDeploymentDescriptor deploymentDescriptor;

    /**
     * Creates the disabled runtime shell carried by the Phase-0 physical plan.
     *
     * @param jobId job identifier
     * @param taskLocation lookup subtask location
     * @param action Phase-0 lookup action
     * @param deploymentDescriptor explicit multi-input deployment descriptor
     */
    public DynamicLookupMultiInputTask(
            long jobId,
            TaskLocation taskLocation,
            DynamicLookupAction action,
            MultiInputTaskDeploymentDescriptor deploymentDescriptor) {
        super(jobId, taskLocation);
        this.action = action;
        this.deploymentDescriptor = deploymentDescriptor;
    }

    public DynamicLookupAction getAction() {
        return action;
    }

    public MultiInputTaskDeploymentDescriptor getDeploymentDescriptor() {
        return deploymentDescriptor;
    }

    @NonNull @Override
    public ProgressState call() {
        throw new IllegalStateException(
                "Dynamic Lookup runtime is disabled: Phase-0 PR-1 only provides "
                        + "multi-input planning and deployment descriptors");
    }

    @Override
    public Set<URL> getJarsUrl() {
        return action.getJarUrls();
    }

    @Override
    public Set<ConnectorJarIdentifier> getConnectorPluginJars() {
        return action.getConnectorJarIdentifiers();
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) {
        restoreComplete.complete(null);
    }
}
