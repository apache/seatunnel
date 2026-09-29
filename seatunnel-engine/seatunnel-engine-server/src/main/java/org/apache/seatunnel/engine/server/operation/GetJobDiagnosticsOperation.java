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
import org.apache.seatunnel.engine.server.diagnostic.JobRuntimeDiagnostics;
import org.apache.seatunnel.engine.server.serializable.ClientToServerOperationDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * Master operation that returns the runtime diagnostics (state timestamps, pipeline restore counts)
 * of one job as a JSON string, so REST callers served by a non-master member get the same payload.
 */
public class GetJobDiagnosticsOperation extends Operation
        implements IdentifiedDataSerializable, AllowedDuringPassiveState {

    private long jobId;
    private String response;

    public GetJobDiagnosticsOperation() {}

    public GetJobDiagnosticsOperation(long jobId) {
        this.jobId = jobId;
    }

    @Override
    public int getFactoryId() {
        return ClientToServerOperationDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ClientToServerOperationDataSerializerHook.GET_JOB_DIAGNOSTICS_OPERATION;
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
    public void run() {
        SeaTunnelServer service = getService();
        response = JobRuntimeDiagnostics.build(service, jobId).toString();
    }

    @Override
    public Object getResponse() {
        return response;
    }
}
