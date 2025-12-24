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

package org.apache.seatunnel.engine.server.telemetry.log.operation;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.operation.TracingOperation;
import org.apache.seatunnel.engine.server.telemetry.log.TaskLogManagerService;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class CleanLogOperation extends TracingOperation implements IdentifiedDataSerializable {

    private long jobId;

    public CleanLogOperation(long jobId) {
        super();
        this.jobId = jobId;
    }

    public CleanLogOperation() {}

    @Override
    public void runInternal() throws Exception {
        SeaTunnelServer service = getService();
        TaskLogManagerService taskLogManagerService = service.getTaskLogManagerService();
        if (taskLogManagerService != null) {
            taskLogManagerService.clean(jobId);
        }
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.CLEAN_LOG_OPERATION;
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }
}
