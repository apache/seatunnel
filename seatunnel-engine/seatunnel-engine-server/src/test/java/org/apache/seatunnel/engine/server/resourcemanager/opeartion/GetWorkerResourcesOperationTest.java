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

import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.diagnostic.WorkerResourceSnapshot;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GetWorkerResourcesOperationTest {

    @Test
    void shouldCollectWorkerResourcesFromTheMasterResourceManager() {
        SeaTunnelServer server = mock(SeaTunnelServer.class);
        CoordinatorService coordinatorService = mock(CoordinatorService.class);
        ResourceManager resourceManager = mock(ResourceManager.class);
        when(server.getCoordinatorService()).thenReturn(coordinatorService);
        when(coordinatorService.getResourceManager()).thenReturn(resourceManager);
        when(resourceManager.getRegisterWorker()).thenReturn(new ConcurrentHashMap<>());

        WorkerResourceSnapshot snapshot =
                GetWorkerResourcesOperation.getWorkerResourceSnapshot(server);

        assertTrue(snapshot.isAvailable());
        assertTrue(snapshot.getWorkers().isEmpty());
    }

    @Test
    void shouldUseTheResourceSerializerContract() {
        GetWorkerResourcesOperation operation = new GetWorkerResourcesOperation();

        assertEquals(ResourceDataSerializerHook.FACTORY_ID, operation.getFactoryId());
        assertEquals(ResourceDataSerializerHook.GET_WORKER_RESOURCES_TYPE, operation.getClassId());
        assertEquals(SeaTunnelServer.SERVICE_NAME, operation.getServiceName());
    }
}
