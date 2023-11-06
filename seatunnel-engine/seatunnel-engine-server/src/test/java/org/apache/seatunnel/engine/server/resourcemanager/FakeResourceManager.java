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

package org.apache.seatunnel.engine.server.resourcemanager;

import org.apache.seatunnel.engine.server.resourcemanager.opeartion.RequestSlotOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.service.slot.SlotAndWorkerProfile;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

/** Used to test ResourceManager, override init method to register more workers. */
public class FakeResourceManager extends AbstractResourceManager {
    public FakeResourceManager(NodeEngine nodeEngine) {
        super(nodeEngine);
        init();
    }

    @Override
    public void init() {
        try {
            Address address1 = new Address("localhost", 5801);
            WorkerProfile workerProfile1 =
                    new WorkerProfile(
                            address1,
                            new ResourceProfile(),
                            new ResourceProfile(),
                            new SlotProfile[] {},
                            new SlotProfile[] {});
            this.registerWorker.put(address1, workerProfile1);

            Address address2 = new Address("localhost", 5802);
            WorkerProfile workerProfile2 =
                    new WorkerProfile(
                            address2,
                            new ResourceProfile(),
                            new ResourceProfile(),
                            new SlotProfile[] {},
                            new SlotProfile[] {});
            this.registerWorker.put(address2, workerProfile2);
            Address address3 = new Address("localhost", 5803);
            WorkerProfile workerProfile3 =
                    new WorkerProfile(
                            address3,
                            new ResourceProfile(),
                            new ResourceProfile(),
                            new SlotProfile[] {},
                            new SlotProfile[] {});
            this.registerWorker.put(address3, workerProfile3);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected <E> CompletableFuture<E> sendToMember(Operation operation, Address address) {
        if (operation instanceof RequestSlotOperation) {
            return (CompletableFuture<E>)
                    CompletableFuture.completedFuture(
                            new SlotAndWorkerProfile(
                                    new WorkerProfile(
                                            address,
                                            new ResourceProfile(),
                                            new ResourceProfile(),
                                            new SlotProfile[] {},
                                            new SlotProfile[] {}),
                                    new SlotProfile(address, 1, new ResourceProfile(), "")));
        } else {
            return super.sendToMember(operation, address);
        }
    }
}
