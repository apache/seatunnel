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

import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.serializable.ClientToServerOperationDataSerializerHook;
import org.apache.seatunnel.engine.server.telemetry.log.TaskLogManagerService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CleanLogOperation extends AbstractJobAsyncOperation {
    public CleanLogOperation() {
        super();
    }

    public CleanLogOperation(long jobId) {
        super(jobId);
    }

    @Override
    protected PassiveCompletableFuture<?> doRun() throws Exception {
        SeaTunnelServer service = getService();
        TaskLogManagerService taskLogManagerService = service.getTaskLogManagerService();
        if (taskLogManagerService == null) {
            // not enable
            return new PassiveCompletableFuture<>(null);
        }
        return taskLogManagerService.clean(jobId);
    }

    @Override
    public int getClassId() {
        return ClientToServerOperationDataSerializerHook.CLEAN_LOG_OPERATION;
    }
}
