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

package org.apache.seatunnel.engine.server.task.operation.checkpoint;

import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

public class ReportReadyRestoreOperation extends Operation implements IdentifiedDataSerializable {

    private long jobID;

    private TaskLocation taskLocation;

    public ReportReadyRestoreOperation() {
    }

    public ReportReadyRestoreOperation(long jobID, TaskLocation taskLocation) {
        this.jobID = jobID;
        this.taskLocation = taskLocation;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        RetryUtils.retryWithException(() -> {
            CheckpointManager checkpointCoordinator = server.getJobMaster(jobID).getCheckpointManager();
            // TODO tell coordinator task ready restore.
            return null;
        }, new RetryUtils.RetryMaterial(Constant.OPERATION_RETRY_TIME, true,
            exception -> exception instanceof NullPointerException, Constant.OPERATION_RETRY_SLEEP));
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(jobID);
        taskLocation.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        jobID = in.readLong();
        taskLocation.readData(in);
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.REPORT_READY_RESTORE_TYPE;
    }
}
