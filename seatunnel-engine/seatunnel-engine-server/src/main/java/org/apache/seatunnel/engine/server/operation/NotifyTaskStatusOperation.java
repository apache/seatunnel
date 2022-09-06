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

package org.apache.seatunnel.engine.server.operation;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;

import com.hazelcast.spi.impl.operationservice.Operation;

public class NotifyTaskStatusOperation extends Operation {

    private final TaskGroupLocation taskGroupLocation;
    private final TaskExecutionState taskExecutionState;

    public NotifyTaskStatusOperation(TaskGroupLocation taskGroupLocation, TaskExecutionState taskExecutionState) {
        super();
        this.taskGroupLocation = taskGroupLocation;
        this.taskExecutionState = taskExecutionState;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer service = getService();
        service.updateTaskExecutionState(taskExecutionState);
    }

    @Override
    public Object getResponse() {
        return super.getResponse();
    }
}
