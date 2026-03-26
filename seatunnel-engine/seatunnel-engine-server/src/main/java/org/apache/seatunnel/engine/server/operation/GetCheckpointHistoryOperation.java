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

import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.serializable.ClientToServerOperationDataSerializerHook;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class GetCheckpointHistoryOperation extends Operation
        implements IdentifiedDataSerializable, AllowedDuringPassiveState {

    private long jobId;
    private Integer pipelineId;
    private int limit;
    private int statusOrdinal;

    private Data response;

    public GetCheckpointHistoryOperation() {}

    public GetCheckpointHistoryOperation(
            long jobId, Integer pipelineId, int limit, int statusOrdinal) {
        this.jobId = jobId;
        this.pipelineId = pipelineId;
        this.limit = limit;
        this.statusOrdinal = statusOrdinal;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer service = getService();
        CheckpointMonitorService monitorService = service.getCheckpointMonitorService();
        List<CheckpointHistoryEntry> entries =
                monitorService == null
                        ? Collections.emptyList()
                        : monitorService.getHistory(
                                jobId,
                                pipelineId,
                                limit,
                                statusOrdinal < 0
                                        ? null
                                        : CheckpointStatus.values()[statusOrdinal]);
        response = getNodeEngine().toData(entries);
    }

    @Override
    public int getFactoryId() {
        return ClientToServerOperationDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ClientToServerOperationDataSerializerHook.GET_CHECKPOINT_HISTORY_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(jobId);
        out.writeBoolean(pipelineId != null);
        if (pipelineId != null) {
            out.writeInt(pipelineId);
        }
        out.writeInt(limit);
        out.writeInt(statusOrdinal);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        jobId = in.readLong();
        if (in.readBoolean()) {
            pipelineId = in.readInt();
        }
        limit = in.readInt();
        statusOrdinal = in.readInt();
    }

    @Override
    public Object getResponse() {
        return response;
    }
}
