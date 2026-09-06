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

import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.autoscale.AutoscalerMetricsSnapshot;
import org.apache.seatunnel.engine.server.autoscale.AutoscalerView;
import org.apache.seatunnel.engine.server.autoscale.ScalingAction;
import org.apache.seatunnel.engine.server.operation.GetAutoscalerViewOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collections;
import java.util.EnumMap;

/**
 * REST service facade for autoscaler read models.
 *
 * <p>Active Master reads are local; follower reads are forwarded to the current master and tolerate
 * master transitions.
 */
public class AutoscalerService extends BaseService {

    public AutoscalerService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public AutoscalerView getView() {
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(true);
        if (seaTunnelServer != null) {
            return seaTunnelServer.getCoordinatorService().getAutoscalerView();
        }
        Address masterAddress = nodeEngine.getMasterAddress();
        if (masterAddress == null) {
            return unavailableView();
        }
        try {
            return invokeOnMaster(masterAddress);
        } catch (RuntimeException e) {
            if (isTransientMasterFailure(e)) {
                return unavailableView();
            }
            throw e;
        }
    }

    public AutoscalerMetricsSnapshot getMetrics() {
        AutoscalerMetricsSnapshot snapshot = getView().getCurrentSnapshot();
        if (snapshot == null) {
            return AutoscalerMetricsSnapshot.builder().build();
        }
        return snapshot;
    }

    protected AutoscalerView invokeOnMaster(Address masterAddress) {
        return (AutoscalerView)
                NodeEngineUtil.sendOperationToMemberNode(
                                nodeEngine, new GetAutoscalerViewOperation(), masterAddress)
                        .join();
    }

    private AutoscalerView unavailableView() {
        return new AutoscalerView(
                false,
                false,
                0L,
                0L,
                0,
                0,
                null,
                null,
                Collections.emptyList(),
                new EnumMap<>(ScalingAction.class));
    }

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
