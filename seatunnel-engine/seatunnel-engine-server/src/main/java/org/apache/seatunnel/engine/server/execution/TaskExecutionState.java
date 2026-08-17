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

package org.apache.seatunnel.engine.server.execution;

import org.apache.seatunnel.common.exception.NonRetryableException;
import org.apache.seatunnel.common.utils.ExceptionUtils;

import java.io.Serializable;

public class TaskExecutionState implements Serializable {

    private static final long serialVersionUID = -108652017022658969L;

    private final TaskGroupLocation taskGroupLocation;

    private final ExecutionState executionState;

    private final String throwableMsg;

    private final boolean nonRetryable;

    public TaskExecutionState(
            TaskGroupLocation taskGroupLocation,
            ExecutionState executionState,
            Throwable throwable) {
        this(taskGroupLocation, executionState, throwable, false);
    }

    public TaskExecutionState(
            TaskGroupLocation taskGroupLocation,
            ExecutionState executionState,
            Throwable throwable,
            boolean nonRetryable) {
        this.taskGroupLocation = taskGroupLocation;
        this.executionState = executionState;
        this.throwableMsg = throwable == null ? "" : ExceptionUtils.getMessage(throwable);
        this.nonRetryable = nonRetryable || containsNonRetryableException(throwable);
    }

    public TaskExecutionState(TaskGroupLocation taskGroupLocation, ExecutionState executionState) {
        this.taskGroupLocation = taskGroupLocation;
        this.executionState = executionState;
        this.throwableMsg = null;
        this.nonRetryable = false;
    }

    public TaskExecutionState(
            TaskGroupLocation taskGroupLocation,
            ExecutionState executionState,
            String throwableMsg) {
        this.taskGroupLocation = taskGroupLocation;
        this.executionState = executionState;
        this.throwableMsg = throwableMsg;
        this.nonRetryable = false;
    }

    public ExecutionState getExecutionState() {
        return executionState;
    }

    public String getThrowableMsg() {
        return throwableMsg;
    }

    public TaskGroupLocation getTaskGroupLocation() {
        return taskGroupLocation;
    }

    public boolean isNonRetryable() {
        return nonRetryable;
    }

    private static boolean containsNonRetryableException(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof NonRetryableException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
