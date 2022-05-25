/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.executionplan;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.engine.execution.TaskExecution;
import org.apache.seatunnel.engine.execution.TaskInfo;

import org.slf4j.Logger;

import java.util.concurrent.Executor;

public class Execution {

    private static final Logger LOG = BaseExecutionPlan.LOG;

    // --------------------------------------------------------------------------------------------

    /**
     * The executor which is used to execute futures.
     */
    private final Executor executor;

    /**
     * The executionSubTask whose task this execution executes.
     */
    private final ExecutionSubTask executionSubTask;

    /**
     * The unique ID marking the specific execution instant of the task.
     */
    private final ExecutionId executionID;

    /**
     * The timestamps when state transitions occurred, indexed by {@link ExecutionState#ordinal()}.
     */
    private final long[] stateTimestamps;

    private final int attemptNumber;

    private volatile ExecutionState state = ExecutionState.CREATED;

    /**
     * Creates a new Execution attempt.
     *
     * @param executor         The executor used to dispatch callbacks from futures and asynchronous RPC
     *                         calls.
     * @param executionSubTask The executionSubTask to which this Execution belongs
     * @param attemptNumber    The execution attempt number.
     */
    public Execution(
        Executor executor,
        ExecutionSubTask executionSubTask,
        int attemptNumber) {

        this.executor = checkNotNull(executor);
        this.executionSubTask = checkNotNull(executionSubTask);
        this.executionID = new ExecutionId();

        this.attemptNumber = attemptNumber;

        this.stateTimestamps = new long[ExecutionState.values().length];
        this.stateTimestamps[ExecutionState.CANCELED.ordinal()] = System.currentTimeMillis();
    }

    public ExecutionSubTask getExecutionSubTask() {
        return executionSubTask;
    }

    public ExecutionId getExecutionID() {
        return executionID;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public ExecutionState getState() {
        return state;
    }

    public long[] getStateTimestamps() {
        return stateTimestamps;
    }

    public long getStateTimestamp(ExecutionState state) {
        return this.stateTimestamps[state.ordinal()];
    }

    public boolean isFinished() {
        return state.isEnd();
    }

    /**
     * Deploys the execution to the previously assigned resource.
     *
     * @throws JobException if the execution cannot be deployed to the assigned resource
     */
    public void deploy(TaskExecution taskExecution) throws JobException {
        if (this.state == ExecutionState.SCHEDULED) {
            if (!turnState(this.state, ExecutionState.DEPLOYING)) {
                // race condition, someone else beat us to the deploying call.
                // this should actually not happen and indicates a race somewhere else
                throw new IllegalStateException(
                    "Cannot deploy task: Concurrent deployment call race.");
            }
        } else {
            // vertex may have been cancelled, or it was already scheduled
            throw new IllegalStateException(
                "The vertex must be in SCHEDULED state to be deployed. Found state "
                    + this.state);
        }
        LOG.info("deploy subTask " + this.executionSubTask.getTaskNameWithSubtask());
        TaskInfo taskInfo = new TaskInfo(
            executionSubTask.getExecutionTask().getExecutionPlan().getJobInformation(),
            this.executionID,
            executionSubTask.getSubTaskIndex(),
            executionSubTask.getExecutionTask().getSource().getBoundedness(),
            executionSubTask.getSourceReader(),
            executionSubTask.getTransformations(),
            executionSubTask.getSinkWriter());
        taskExecution.submit(taskInfo);
    }

    public void cancel() {
        throw new RuntimeException("Not support now");
    }

    /**
     * This method fails the vertex due to an external condition. The task will move to state
     * FAILED. If the task was in state RUNNING or DEPLOYING before, it will send a cancel call to
     * the TaskManager.
     *
     * @param t The exception that caused the task to fail.
     */
    public void fail(Throwable t) {
        // TODO
        throw new RuntimeException("not support now");
    }

    private void handleFinished() {

        // this call usually comes during RUNNING, but may also come while still in deploying (very
        // fast tasks!)
        while (true) {
            ExecutionState current = this.state;

            if (current == ExecutionState.INITIALIZING || current == ExecutionState.RUNNING || current == ExecutionState.DEPLOYING) {

                if (turnState(current, ExecutionState.FINISHED)) {
                    try {
                        executionSubTask.getExecutionTask().getExecutionPlan().removeExecution(this);
                    } finally {
                        executionSubTask.executionFinished(this);
                    }
                    return;
                }
            } else if (current == ExecutionState.CANCELING) {
                // TODO
                return;
            } else if (current == ExecutionState.CANCELED || current == ExecutionState.FAILED) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Task FINISHED, but concurrently went to state " + state);
                }
                return;
            } else {
                // this should not happen, we need to fail this
                handleFailed();
                return;
            }
        }
    }

    private void handleFailed() {
        // TODO
        executionSubTask.getExecutionTask().getExecutionPlan().removeExecution(this);
    }

    public boolean turnState(ExecutionState newState) {
        switch (newState) {
            case SCHEDULED:
                return turnState(ExecutionState.CREATED, ExecutionState.SCHEDULED);

            case INITIALIZING:
                return switchToRecovering();

            case RUNNING:
                return turnState(ExecutionState.INITIALIZING, ExecutionState.RUNNING);

            case FINISHED:
                handleFinished();
                return true;

            case CANCELED:
                // TODO
                return true;

            case FAILED:
                handleFailed();
                return true;

            default:
                // we mark as failed and return false, which triggers the TaskManager
                // to remove the task
                fail(
                    new Exception(
                        "TaskManager sent illegal state update: "
                            + state));
                return false;
        }
    }

    private boolean switchToRecovering() {
        if (turnState(ExecutionState.DEPLOYING, ExecutionState.INITIALIZING)) {
            return true;
        }

        return false;
    }

    public boolean turnState(ExecutionState currentState, ExecutionState targetState) {
        if (currentState.isEnd()) {
            throw new IllegalStateException(
                "Cannot leave end state "
                    + currentState
                    + " to transition to "
                    + targetState
                    + '.');
        }

        if (state != currentState) {
            LOG.error("The Execution currentState and currentState not equal");
            return false;
        }

        state = targetState;
        stateTimestamps[state.ordinal()] = System.currentTimeMillis();

        LOG.info("The Execution {} State turn from {} to {} success", executionID, currentState, targetState);

        return true;

    }

}
