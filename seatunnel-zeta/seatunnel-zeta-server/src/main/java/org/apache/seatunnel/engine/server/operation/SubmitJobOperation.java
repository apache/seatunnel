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

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import lombok.NonNull;

import java.io.IOException;

public class SubmitJobOperation extends AbstractJobAsyncOperation {
    private Data jobImmutableInformation;

    public SubmitJobOperation() {
    }

    public SubmitJobOperation(long jobId, @NonNull Data jobImmutableInformation) {
        super(jobId);
        this.jobImmutableInformation = jobImmutableInformation;
    }

    @Override
    public int getClassId() {
        return ClientToServerOperationDataSerializerHook.SUBMIT_OPERATOR;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, jobImmutableInformation);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        jobImmutableInformation = IOUtil.readData(in);
    }

    @Override
    protected PassiveCompletableFuture<?> doRun() throws Exception {
        SeaTunnelServer seaTunnelServer = getService();
        return seaTunnelServer.getCoordinatorService().submitJob(jobId, jobImmutableInformation);
    }
}
