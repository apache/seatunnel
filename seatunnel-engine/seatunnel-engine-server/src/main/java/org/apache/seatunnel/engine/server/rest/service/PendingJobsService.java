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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.diagnostic.PendingJobsResponse;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.GetPendingJobsOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Map;

public class PendingJobsService extends BaseService {

    public PendingJobsService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public PendingJobsResponse getPendingJobs(Map<String, String> tags, Long jobId, int limit) {
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);
        if (seaTunnelServer == null) {
            return (PendingJobsResponse)
                    NodeEngineUtil.sendOperationToMasterNode(
                                    nodeEngine, new GetPendingJobsOperation(tags, jobId, limit))
                            .join();
        }
        return seaTunnelServer.getCoordinatorService().getPendingJobs(tags, jobId, limit);
    }
}
