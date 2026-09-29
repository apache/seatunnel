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

package org.apache.seatunnel.engine.server.resourcemanager.thirdparty.yarn;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.ApplicationResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceIDRetrievable;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngine;

public class YarnResourceManager<WorkerType extends ResourceIDRetrievable>
        extends ApplicationResourceManager<WorkerType> {

    public YarnResourceManager(
            NodeEngine nodeEngine,
            EngineConfig engineConfig,
            String applicationId,
            ApplicationSpecification specification,
            Address masterAddress,
            ResourceManagerDriver<WorkerType> driver) {
        super(nodeEngine, engineConfig, applicationId, specification, masterAddress, driver);
    }

    @Override
    public CompletableFuture<WorkerType> requestWorker(WorkerSpecification specification) {
        return getDriver().requestWorker(specification);
    }

    @Override
    public CompletableFuture<Void> releaseWorker(WorkerType worker) {
        return getDriver().releaseWorker(worker);
    }
}
