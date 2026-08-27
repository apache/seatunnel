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

package org.apache.seatunnel.engine.server.task.operation;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReportMetricsOperation extends TracingOperation implements IdentifiedDataSerializable {
    private Map<TaskLocation, SeaTunnelMetricsContext> localMap;

    public ReportMetricsOperation() {}

    public ReportMetricsOperation(Map<TaskLocation, SeaTunnelMetricsContext> localMap) {
        this.localMap = localMap;
    }

    @Override
    public void runInternal() throws Exception {
        SeaTunnelServer seaTunnelServer = getService();
        if (localMap != null) {
            seaTunnelServer.updateMetrics(localMap);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(localMap == null ? 0 : localMap.size());
        if (localMap != null) {
            for (Map.Entry<TaskLocation, SeaTunnelMetricsContext> e : localMap.entrySet()) {
                out.writeObject(e.getKey());
                out.writeObject(e.getValue());
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        this.localMap = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            TaskLocation key = in.readObject();
            SeaTunnelMetricsContext value = in.readObject();
            this.localMap.put(key, value);
        }
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.REPORT_METRICS_OPERATION;
    }
}
