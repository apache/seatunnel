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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.GetWorkerOverviewOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.WorkerOverviewInfo;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collections;
import java.util.List;

/**
 * Serves the per-worker resource projection ({@link GetWorkerOverviewOperation}) for the Web UI
 * Workers/Master pages. Mirrors {@link OverviewService}'s local-vs-forward-to-master routing: when
 * this node is not the active master, the request is forwarded there since resource manager state
 * only lives on the master.
 */
public class WorkerOverviewService extends BaseService {

    private final NodeEngineImpl nodeEngine;

    public WorkerOverviewService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        this.nodeEngine = nodeEngine;
    }

    @SuppressWarnings("unchecked")
    public List<WorkerOverviewInfo> getWorkerOverviewInfos() {
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);

        if (seaTunnelServer == null) {
            // Master election may not be finished yet (e.g. right after local engine startup).
            // Avoid sending operation to a null master address which will trigger NPE.
            if (nodeEngine.getMasterAddress() == null) {
                return Collections.emptyList();
            }
            return (List<WorkerOverviewInfo>)
                    NodeEngineUtil.sendOperationToMasterNode(
                                    nodeEngine, new GetWorkerOverviewOperation())
                            .join();
        }
        return GetWorkerOverviewOperation.getWorkerOverviewInfos(seaTunnelServer);
    }
}
