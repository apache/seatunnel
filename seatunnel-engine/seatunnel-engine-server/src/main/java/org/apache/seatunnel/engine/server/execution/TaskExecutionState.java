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

import org.apache.seatunnel.common.utils.ExceptionUtils;

import java.io.Serializable;

public class TaskExecutionState implements Serializable {

    private final TaskGroupLocation taskGroupLocation;

    private final ExecutionState executionState;

    private final String throwableMsg;

    public TaskExecutionState(
            TaskGroupLocation taskGroupLocation,
            ExecutionState executionState,
            Throwable throwable) {
        this.taskGroupLocation = taskGroupLocation;
        this.executionState = executionState;
        this.throwableMsg = throwable == null ? "" : ExceptionUtils.getMessage(throwable);
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
}
