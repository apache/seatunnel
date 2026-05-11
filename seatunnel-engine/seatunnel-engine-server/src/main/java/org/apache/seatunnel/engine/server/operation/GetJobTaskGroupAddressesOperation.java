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

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.serializable.ClientToServerOperationDataSerializerHook;

import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.seatunnel.engine.server.metrics.JobMetricsUtil.toJsonString;

public class GetJobTaskGroupAddressesOperation extends Operation
        implements IdentifiedDataSerializable, AllowedDuringPassiveState {
    private long jobId;

    private String response;

    public GetJobTaskGroupAddressesOperation() {}

    public GetJobTaskGroupAddressesOperation(long jobId) {
        this.jobId = jobId;
    }

    @Override
    public final int getFactoryId() {
        return ClientToServerOperationDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ClientToServerOperationDataSerializerHook.GET_JOB_TASK_GROUP_ADDRESSES_OPERATOR;
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
        CompletableFuture<String> future =
                CompletableFuture.supplyAsync(
                        () -> {
                            Map<TaskGroupLocation, Address> addresses =
                                    service.getCoordinatorService()
                                            .queryJobTaskGroupAddresses(jobId);
                            return toJsonString(toTaskGroupAddressList(addresses));
                        },
                        getNodeEngine()
                                .getExecutionService()
                                .getExecutor("get_job_taskgroup_addresses_operation"));

        try {
            response = future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Map<String, Object>> toTaskGroupAddressList(
            Map<TaskGroupLocation, Address> taskGroupAddresses) {
        List<Map<String, Object>> result = new ArrayList<>(taskGroupAddresses.size());
        taskGroupAddresses.forEach(
                (location, address) -> {
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("jobId", location.getJobId());
                    item.put("pipelineId", location.getPipelineId());
                    item.put("taskGroupId", location.getTaskGroupId());
                    item.put("host", address.getHost());
                    item.put("port", address.getPort());
                    result.add(item);
                });
        return result;
    }

    @Override
    public Object getResponse() {
        return response;
    }
}
