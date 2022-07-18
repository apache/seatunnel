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

package org.apache.seatunnel.engine.server;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.spi.impl.NodeEngineImpl.JetPacketConsumer;

import java.util.Map;

public class NodeExtension extends DefaultNodeExtension implements JetPacketConsumer {
    private final NodeExtensionCommon extCommon;

    public NodeExtension(Node node) {
        super(node);
        extCommon = new NodeExtensionCommon(node, new Server(node));
    }

    @Override
    public void beforeStart() {
        // TODO Get Config from Node here
        super.beforeStart();
    }

    @Override
    public void afterStart() {
        super.afterStart();
        extCommon.afterStart();
    }

    @Override
    public void beforeClusterStateChange(ClusterState currState, ClusterState requestedState, boolean isTransient) {
        super.beforeClusterStateChange(currState, requestedState, isTransient);
        extCommon.beforeClusterStateChange(requestedState);
    }

    @Override
    public void onClusterStateChange(ClusterState newState, boolean isTransient) {
        super.onClusterStateChange(newState, isTransient);
        extCommon.onClusterStateChange(newState);
    }

    @Override
    public Map<String, Object> createExtensionServices() {
        return extCommon.createExtensionServices();
    }

    @Override
    public void printNodeInfo() {
        extCommon.printNodeInfo(systemLogger, "");
    }

    @Override
    public void accept(Packet packet) {
        extCommon.handlePacket(packet);
    }
}
