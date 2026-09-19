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

package org.apache.seatunnel.engine.server.utils;

import org.apache.seatunnel.engine.server.SeaTunnelServer;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

public class NodeEngineUtil {

    private NodeEngineUtil() {}

    public static <E> InvocationFuture<E> sendOperationToMasterNode(
            NodeEngine nodeEngine, Operation operation) {
        Address masterAddress = getActiveMasterAddressOrThrow(nodeEngine);
        InvocationBuilder invocationBuilder =
                nodeEngine
                        .getOperationService()
                        .createInvocationBuilder(
                                SeaTunnelServer.SERVICE_NAME, operation, masterAddress)
                        .setAsync();
        return invocationBuilder.invoke();
    }

    /**
     * Returns the active SeaTunnel coordinator address when the local membership view can confirm
     * one.
     *
     * <p>In separated clusters, worker-only nodes are Hazelcast lite members. Hazelcast mastership
     * can temporarily point to a lite worker after failover, but SeaTunnel control-plane operations
     * must still be sent to a coordinator-capable member. Mixed clusters keep the old behavior
     * because the Hazelcast master is not a lite member. When the current membership view cannot
     * confirm any coordinator-capable member yet, this method returns {@code null} so callers can
     * retry instead of misrouting the request to a lite worker.
     */
    public static Address getActiveMasterAddress(NodeEngine nodeEngine) {
        Address hazelcastMasterAddress = nodeEngine.getMasterAddress();
        if (hazelcastMasterAddress == null) {
            return null;
        }
        Member hazelcastMaster = nodeEngine.getClusterService().getMember(hazelcastMasterAddress);
        if (hazelcastMaster == null) {
            return null;
        }
        if (!hazelcastMaster.isLiteMember()) {
            return hazelcastMasterAddress;
        }
        return nodeEngine.getClusterService().getMembers().stream()
                .filter(member -> !member.isLiteMember())
                .map(Member::getAddress)
                .findFirst()
                .orElse(null);
    }

    public static <E> InvocationFuture<E> sendOperationToMemberNode(
            NodeEngine nodeEngine, Operation operation, Address memberAddress) {
        InvocationBuilder invocationBuilder =
                nodeEngine
                        .getOperationService()
                        .createInvocationBuilder(
                                SeaTunnelServer.SERVICE_NAME, operation, memberAddress)
                        .setAsync();
        return invocationBuilder.invoke();
    }

    private static Address getActiveMasterAddressOrThrow(NodeEngine nodeEngine) {
        Address masterAddress = getActiveMasterAddress(nodeEngine);
        if (masterAddress == null) {
            throw new RetryableHazelcastException("active master not yet known");
        }
        return masterAddress;
    }
}
