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

import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.serializable.ClientToServerOperationDataSerializerHook;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class GetJobInfoOperation extends Operation
        implements IdentifiedDataSerializable, AllowedDuringPassiveState {

    private long jobId;

    private Data response;

    public GetJobInfoOperation() {}

    public GetJobInfoOperation(long jobId) {
        this.jobId = jobId;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer service = getService();
        CompletableFuture<Data> future =
                CompletableFuture.supplyAsync(
                        () ->
                                this.getNodeEngine()
                                        .toData(service.getCoordinatorService().getJobInfo(jobId)),
                        getNodeEngine()
                                .getExecutionService()
                                .getExecutor("get_job_info_operation"));

        try {
            response = future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new SeaTunnelEngineException(e);
        }
    }

    @Override
    public int getFactoryId() {
        return ClientToServerOperationDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ClientToServerOperationDataSerializerHook.GET_JOB_INFO_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(jobId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        jobId = in.readLong();
    }

    @Override
    public Object getResponse() {
        return response;
    }
}
