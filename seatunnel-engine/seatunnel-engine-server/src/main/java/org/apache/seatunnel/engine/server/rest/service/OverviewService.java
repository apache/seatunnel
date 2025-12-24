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

import org.apache.seatunnel.engine.common.env.EnvironmentUtil;
import org.apache.seatunnel.engine.common.env.Version;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.GetOverviewOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.OverviewInfo;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Map;

public class OverviewService extends BaseService {

    private final NodeEngineImpl nodeEngine;

    public OverviewService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        this.nodeEngine = nodeEngine;
    }

    public OverviewInfo getOverviewInfo(Map<String, String> tags) {
        Version version = EnvironmentUtil.getVersion();
        OverviewInfo overviewInfo;

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);

        if (seaTunnelServer == null) {
            overviewInfo =
                    (OverviewInfo)
                            NodeEngineUtil.sendOperationToMasterNode(
                                            nodeEngine, new GetOverviewOperation(tags))
                                    .join();
        } else {
            overviewInfo = GetOverviewOperation.getOverviewInfo(seaTunnelServer, nodeEngine, tags);
        }

        overviewInfo.setProjectVersion(version.getProjectVersion());
        overviewInfo.setGitCommitAbbrev(version.getGitCommitAbbrev());

        return overviewInfo;
    }
}
