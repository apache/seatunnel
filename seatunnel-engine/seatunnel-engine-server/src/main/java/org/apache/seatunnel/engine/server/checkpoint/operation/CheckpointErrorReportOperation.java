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

package org.apache.seatunnel.engine.server.checkpoint.operation;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.CheckpointDataSerializerHook;
import org.apache.seatunnel.engine.server.task.operation.TaskOperation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import lombok.NoArgsConstructor;

import java.io.IOException;

@NoArgsConstructor
public class CheckpointErrorReportOperation extends TaskOperation {

    private String errorMsg;

    public CheckpointErrorReportOperation(TaskLocation taskLocation, Throwable e) {
        super(taskLocation);
        this.errorMsg = ExceptionUtils.getMessage(e);
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        server.getCoordinatorService()
                .getJobMaster(taskLocation.getJobId())
                .getCheckpointManager()
                .reportCheckpointErrorFromTask(taskLocation, errorMsg);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(errorMsg);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        errorMsg = in.readString();
    }

    @Override
    public int getFactoryId() {
        return CheckpointDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CheckpointDataSerializerHook.CHECKPOINT_ERROR_REPORT_OPERATOR;
    }
}
