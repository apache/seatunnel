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

package org.apache.seatunnel.engine.server.resourcemanager.opeartion;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.autoscale.WorkerMetricsSample;
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;

import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * Worker-to-master operation carrying one autoscaler CPU/JVM-memory sample.
 *
 * <p>The payload uses worker-observed event time and keeps autoscaler metrics out of WorkerProfile
 * serialization.
 */
public class ReportAutoscalerMetricsOperation extends Operation
        implements IdentifiedDataSerializable {

    private Address workerAddress;
    private long eventTimeMillis;
    private double cpuUtilization;
    private double jvmMemoryUtilization;

    public ReportAutoscalerMetricsOperation() {}

    public ReportAutoscalerMetricsOperation(
            Address workerAddress,
            long eventTimeMillis,
            double cpuUtilization,
            double jvmMemoryUtilization) {
        this.workerAddress = workerAddress;
        this.eventTimeMillis = eventTimeMillis;
        this.cpuUtilization = cpuUtilization;
        this.jvmMemoryUtilization = jvmMemoryUtilization;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        server.getCoordinatorService()
                .getResourceManager()
                .reportAutoscalerMetrics(
                        new WorkerMetricsSample(
                                workerAddress,
                                eventTimeMillis,
                                cpuUtilization,
                                jvmMemoryUtilization),
                        System.currentTimeMillis());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(workerAddress);
        out.writeLong(eventTimeMillis);
        out.writeDouble(cpuUtilization);
        out.writeDouble(jvmMemoryUtilization);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        workerAddress = in.readObject();
        eventTimeMillis = in.readLong();
        cpuUtilization = in.readDouble();
        jvmMemoryUtilization = in.readDouble();
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ResourceDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ResourceDataSerializerHook.REPORT_AUTOSCALER_METRICS_TYPE;
    }
}
