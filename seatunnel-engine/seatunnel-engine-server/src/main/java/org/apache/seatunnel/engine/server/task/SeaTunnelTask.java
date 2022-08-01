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

import org.apache.seatunnel.engine.server.dag.physical.ActionWrapper;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.Task;

import com.hazelcast.spi.impl.operationservice.OperationService;
import lombok.NonNull;

import java.io.IOException;

public class SeaTunnelTask implements Task {

    private static final long serialVersionUID = 2604309561613784425L;
    private OperationService operationService;
    private final ActionWrapper actionWrapper;
    private Progress progress;

    public SeaTunnelTask(ActionWrapper actionWrapper) {
        this.actionWrapper = actionWrapper;
    }

    @Override
    public void init() {
        progress = new Progress();
    }

    @NonNull
    @Override
    public ProgressState call() {
        return progress.toState();
    }

    @NonNull
    @Override
    public Long getTaskID() {
        return (long) actionWrapper.getAction().getId();
    }

    @Override
    public void close() throws IOException {
        Task.super.close();
    }

    @Override
    public void setOperationService(OperationService operationService) {
        this.operationService = operationService;
    }
}
