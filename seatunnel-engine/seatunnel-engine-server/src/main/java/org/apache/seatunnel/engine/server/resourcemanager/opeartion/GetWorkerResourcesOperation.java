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
import org.apache.seatunnel.engine.server.diagnostic.PendingDiagnosticsCollector;
import org.apache.seatunnel.engine.server.diagnostic.WorkerResourceSnapshot;
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

/** Collects the current worker resource snapshot on the master node. */
public class GetWorkerResourcesOperation extends Operation implements IdentifiedDataSerializable {

    private WorkerResourceSnapshot snapshot;

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        snapshot = getWorkerResourceSnapshot(server);
    }

    @Override
    public Object getResponse() {
        return snapshot;
    }

    @Override
    public int getFactoryId() {
        return ResourceDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ResourceDataSerializerHook.GET_WORKER_RESOURCES_TYPE;
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    public static WorkerResourceSnapshot getWorkerResourceSnapshot(SeaTunnelServer server) {
        return PendingDiagnosticsCollector.collectWorkerResourceSnapshot(
                server.getCoordinatorService().getResourceManager());
    }
}
