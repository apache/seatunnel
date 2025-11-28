/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.resourcemanager.opeartion;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.diagnostic.PendingJobsResponse;
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Map;

public class GetPendingJobsOperation extends Operation implements IdentifiedDataSerializable {

    private Map<String, String> tags;
    private Long jobId;
    private int limit;
    private PendingJobsResponse response;

    public GetPendingJobsOperation() {}

    public GetPendingJobsOperation(Map<String, String> tags, Long jobId, int limit) {
        this.tags = tags;
        this.jobId = jobId;
        this.limit = limit;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        response = server.getCoordinatorService().getPendingJobs(tags, jobId, limit);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getFactoryId() {
        return ResourceDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ResourceDataSerializerHook.GET_PENDING_JOBS_TYPE;
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(tags);
        out.writeObject(jobId);
        out.writeInt(limit);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        tags = in.readObject();
        jobId = in.readObject();
        limit = in.readInt();
    }
}
