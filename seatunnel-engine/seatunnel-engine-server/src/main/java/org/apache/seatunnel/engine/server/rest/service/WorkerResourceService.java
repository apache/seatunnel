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

import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.diagnostic.WorkerResourceSnapshot;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.GetWorkerResourcesOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collections;

/** Resolves the worker resource snapshot locally on the master or forwards the read to it. */
public class WorkerResourceService extends BaseService {

    public WorkerResourceService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    /** Returns the local master's snapshot or forwards the request to the current master. */
    public WorkerResourceSnapshot getWorkerResources() {
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);
        if (seaTunnelServer != null) {
            return GetWorkerResourcesOperation.getWorkerResourceSnapshot(seaTunnelServer);
        }
        Address masterAddress = nodeEngine.getMasterAddress();
        if (masterAddress == null) {
            return unavailableSnapshot();
        }
        try {
            return invokeOnMaster(masterAddress);
        } catch (RuntimeException e) {
            if (isTransientMasterFailure(e)) {
                return unavailableSnapshot();
            }
            throw e;
        }
    }

    protected WorkerResourceSnapshot invokeOnMaster(Address masterAddress) {
        return (WorkerResourceSnapshot)
                NodeEngineUtil.sendOperationToMemberNode(
                                nodeEngine, new GetWorkerResourcesOperation(), masterAddress)
                        .join();
    }

    private WorkerResourceSnapshot unavailableSnapshot() {
        return new WorkerResourceSnapshot(
                false, System.currentTimeMillis(), Collections.emptyList());
    }

    /** Identifies failures caused by a master transition while the request is in flight. */
    private boolean isTransientMasterFailure(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof TargetNotMemberException
                    || current instanceof TargetDisconnectedException
                    || current instanceof MemberLeftException
                    || current instanceof HazelcastInstanceNotActiveException
                    || current instanceof SeaTunnelEngineException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
