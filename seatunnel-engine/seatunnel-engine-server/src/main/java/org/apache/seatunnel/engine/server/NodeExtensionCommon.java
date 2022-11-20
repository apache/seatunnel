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

import static com.hazelcast.cluster.ClusterState.PASSIVE;

import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.engine.common.Constant;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.HashMap;
import java.util.Map;

class NodeExtensionCommon {
    private final Node node;
    private final ILogger logger;
    private final SeaTunnelServer server;

    NodeExtensionCommon(Node node, SeaTunnelServer server) {
        this.node = node;
        this.logger = node.getLogger(getClass().getName());
        this.server = server;
    }

    void afterStart() {
        //TODO seaTunnelServer after start in here
    }

    void beforeClusterStateChange(ClusterState requestedState) {
        if (requestedState != PASSIVE) {
            return;
        }
        logger.info("st is preparing to enter the PASSIVE cluster state");
        NodeEngineImpl ne = node.nodeEngine;
        //TODO This is where cluster state changes are handled
    }

    void onClusterStateChange(ClusterState ignored) {
        //TODO This is where cluster state changes are handled
    }

    void printNodeInfo(ILogger log, String addToProductName) {
        log.info(imdgVersionMessage());
        log.info(clusterNameMessage());
        log.fine(serializationVersionMessage());
        log.info('\n' + Constants.ST_LOGO);
        log.info(Constants.COPYRIGHT_LINE);
    }

    private String imdgVersionMessage() {
        String build = node.getBuildInfo().getBuild();
        String revision = node.getBuildInfo().getRevision();
        if (!revision.isEmpty()) {
            build += " - " + revision;
        }
        return "Based on Hazelcast IMDG version: " + node.getVersion() + " (" + build + ")";
    }

    private String serializationVersionMessage() {
        return "Configured Hazelcast Serialization version: " + node.getBuildInfo().getSerializationVersion();
    }

    private String clusterNameMessage() {
        return "Cluster name: " + node.getConfig().getClusterName();
    }

    Map<String, Object> createExtensionServices() {
        Map<String, Object> extensionServices = new HashMap<>();

        extensionServices.put(Constant.SEATUNNEL_SERVICE_NAME, server);

        return extensionServices;
    }
}
