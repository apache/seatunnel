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

import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public class GetEventOperation extends Operation implements IdentifiedDataSerializable {

    private Long jobId;

    private Boolean isAll;

    private List<Event> events = new ArrayList<>();

    public GetEventOperation() {}

    private Data response;

    /**
     * @param jobId job id
     * @param isAll When isAll is true, retrieve all events; otherwise, retrieve the latest event
     */
    public GetEventOperation(Long jobId, boolean isAll) {
        this.jobId = jobId;
        this.isAll = isAll;
    }

    @Override
    public void run() throws Exception {
        if (jobId == null) {
            throw new SeaTunnelEngineException("JobId cannot be null");
        }

        SeaTunnelServer server = getService();
        CoordinatorService coordinatorService = server.getCoordinatorService();
        try {
            JobMaster jobMaster = coordinatorService.getJobMaster(jobId);
            if (jobMaster != null) {
                log.debug("Retrieving events for active job {}, isAll: {}", jobId, isAll);
                if (isAll) {
                    Optional.ofNullable(jobMaster.getHistoryEvents()).ifPresent(events::addAll);
                } else {
                    Optional.ofNullable(jobMaster.getEvents())
                            .ifPresent(event -> event.drainTo(events));
                }
            } else {
                log.debug("Job {} not active, retrieving from history", jobId);
                Optional.ofNullable(
                                coordinatorService
                                        .getJobHistoryService()
                                        .getFinishedJobEventImap()
                                        .get(jobId))
                        .ifPresent(events::addAll);
            }

            CompletableFuture<Data> future =
                    CompletableFuture.supplyAsync(
                            () -> this.getNodeEngine().toData(events),
                            getNodeEngine()
                                    .getExecutionService()
                                    .getExecutor("get_event_operation"));

            response = future.get();
        } catch (Exception e) {
            log.error("Failed to retrieve events for job " + jobId, e);
            throw new SeaTunnelEngineException("Failed to retrieve events: " + e.getMessage(), e);
        }
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
        return ResourceDataSerializerHook.GET_EVENT_TYPE;
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(jobId);
        out.writeBoolean(isAll);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        jobId = in.readLong();
        isAll = in.readBoolean();
    }
}
