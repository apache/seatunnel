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

import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupCoordinatorAction;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointPlan;
import org.apache.seatunnel.engine.server.checkpoint.CoordinatorStateKey;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.record.Barrier;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;

import lombok.NonNull;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.CANCELED;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.CLOSED;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.INIT;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.PREPARE_CLOSE;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.READY_START;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.RUNNING;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.STARTING;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.WAITING_RESTORE;

/**
 * Operator-scoped coordinator task for dynamic lookup control-plane state.
 *
 * <p>The current runtime participates in lifecycle and checkpoint topology with an empty state
 * payload. Bootstrap leases, ready aggregation, and source gate commands are still fenced out until
 * their protocol state is implemented.
 */
public final class DynamicLookupCoordinatorTask extends CoordinatorTask {

    private static final long serialVersionUID = 1L;

    /** Immutable control-plane action paired with the lookup declaration. */
    private final DynamicLookupCoordinatorAction action;

    /** Stable operator-scoped checkpoint identity. */
    private final CoordinatorStateKey coordinatorStateKey;

    /** Local lifecycle state for the coordinator shell. */
    private volatile SeaTunnelTaskState currentState;

    /**
     * Creates an operator-scoped coordinator task.
     *
     * @param jobId job identifier
     * @param taskLocation coordinator task location
     * @param action lookup coordinator action
     * @param coordinatorStateKey stable checkpoint identity
     */
    public DynamicLookupCoordinatorTask(
            long jobId,
            TaskLocation taskLocation,
            DynamicLookupCoordinatorAction action,
            CoordinatorStateKey coordinatorStateKey) {
        super(jobId, taskLocation);
        this.action = action;
        this.coordinatorStateKey = coordinatorStateKey;
        this.currentState = SeaTunnelTaskState.CREATED;
    }

    @Override
    public void init() throws Exception {
        super.init();
        currentState = INIT;
    }

    public CoordinatorStateKey getCoordinatorStateKey() {
        return coordinatorStateKey;
    }

    public DynamicLookupCoordinatorAction getAction() {
        return action;
    }

    @NonNull @Override
    public ProgressState call() throws Exception {
        processState();
        return progress.toState();
    }

    @Override
    public void triggerBarrier(Barrier barrier) {
        if (barrier.prepareClose(taskLocation)) {
            prepareCloseStatus = true;
            prepareCloseBarrierId.set(barrier.getId());
        }
        if (!barrier.snapshot()) {
            return;
        }
        getExecutionContext()
                .sendToMaster(
                        new TaskAcknowledgeOperation(
                                taskLocation,
                                (CheckpointBarrier) barrier,
                                Collections.singletonList(
                                        new ActionSubtaskState(
                                                coordinatorStateKey,
                                                CheckpointPlan.COORDINATOR_INDEX,
                                                Collections.emptyList()))))
                .join();
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) {
        for (ActionSubtaskState actionState : actionStateList) {
            if (!coordinatorStateKey.equals(actionState.getStateKey())
                    || actionState.getIndex() != CheckpointPlan.COORDINATOR_INDEX) {
                throw new IllegalArgumentException(
                        "Unexpected coordinator checkpoint state for " + coordinatorStateKey);
            }
        }
        restoreComplete.complete(null);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        tryClose(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        tryClose(checkpointId);
    }

    @Override
    public void notifyCheckpointEnd(long checkpointId) {
        tryClose(checkpointId);
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
    public void close() throws IOException {
        super.close();
        progress.done();
    }

    private void processState() throws Exception {
        switch (currentState) {
            case INIT:
                currentState = WAITING_RESTORE;
                reportTaskStatus(WAITING_RESTORE);
                break;
            case WAITING_RESTORE:
                if (restoreComplete.isDone()) {
                    currentState = READY_START;
                    reportTaskStatus(READY_START);
                } else {
                    Thread.sleep(100);
                }
                break;
            case READY_START:
                if (startCalled) {
                    currentState = STARTING;
                } else {
                    Thread.sleep(100);
                }
                break;
            case STARTING:
                currentState = RUNNING;
                break;
            case RUNNING:
                if (prepareCloseStatus) {
                    currentState = PREPARE_CLOSE;
                } else {
                    Thread.sleep(100);
                }
                break;
            case PREPARE_CLOSE:
                if (closeCalled) {
                    currentState = CLOSED;
                } else {
                    Thread.sleep(100);
                }
                break;
            case CLOSED:
            case CANCELED:
                close();
                break;
            case CANCELLING:
                close();
                currentState = CANCELED;
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown dynamic lookup coordinator state: " + currentState);
        }
    }
}
