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

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import com.hazelcast.spi.impl.NodeEngine;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Native slot bookkeeping for an externally maintained fixed application pool. */
final class FixedPoolApplicationResourceManager extends StandaloneResourceManager {
    private final int expectedWorkers;

    FixedPoolApplicationResourceManager(
            NodeEngine nodeEngine, EngineConfig config, int expectedWorkers) {
        super(nodeEngine, config);
        this.expectedWorkers = expectedWorkers;
    }

    /** Native registration gates allocation; Pod readiness never substitutes for usable slots. */
    @Override
    public CompletableFuture<List<SlotProfile>> applyResources(
            long jobId, List<ResourceProfile> resources, Map<String, String> tags)
            throws NoEnoughResourceException {
        if (workerCount(Collections.emptyMap()) < expectedWorkers) {
            throw new NoEnoughResourceException();
        }
        return super.applyResources(jobId, resources, tags);
    }
}
