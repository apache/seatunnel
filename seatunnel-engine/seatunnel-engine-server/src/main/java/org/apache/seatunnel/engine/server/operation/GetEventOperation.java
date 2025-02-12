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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;

@Slf4j
public class GetEventOperation extends Operation implements IdentifiedDataSerializable {

    private Long jobId;

    private Boolean isAll;

    private List<Event> events = new ArrayList<>();

    public GetEventOperation() {}

    private Data response;

    public GetEventOperation(Long jobId, boolean isAll) {
        this.jobId = jobId;
        this.isAll = isAll;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        CoordinatorService coordinatorService = server.getCoordinatorService();
        JobMaster jobMaster = coordinatorService.getJobMaster(jobId);
        if (jobMaster != null) {
            if (isAll) {
                ArrayBlockingQueue<Event> event = jobMaster.getHistoryEvents();
                if (event != null) {
                    events.addAll(event);
                }
            } else {
                ArrayBlockingQueue<Event> event = jobMaster.getEvents();
                if (event != null) {
                    event.drainTo(events);
                }
            }
        } else {
            ArrayBlockingQueue<Event> historyEvents =
                    coordinatorService.getJobHistoryService().getFinishedJobEventImap().get(jobId);
            if (historyEvents != null) {
                events.addAll(historyEvents);
            }
        }

        CompletableFuture<Data> future =
                CompletableFuture.supplyAsync(
                        () -> this.getNodeEngine().toData(events),
                        getNodeEngine().getExecutionService().getExecutor("get_event_operation"));

        try {
            response = future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new SeaTunnelEngineException(e);
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
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        jobId = in.readLong();
    }
}
