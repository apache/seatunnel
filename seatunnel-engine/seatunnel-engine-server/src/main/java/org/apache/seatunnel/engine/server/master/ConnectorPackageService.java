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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageMode;
import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.task.operation.SendConnectorJarToMemberNodeOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;

@Slf4j
public class ConnectorPackageService {

    private static final ILogger LOGGER = Logger.getLogger(ConnectorPackageService.class);

    private final SeaTunnelServer seaTunnelServer;

    private final SeaTunnelConfig seaTunnelConfig;

    private final ConnectorJarStorageConfig connectorJarStorageConfig;

    private final NodeEngineImpl nodeEngine;

    private ConnectorJarStorageStrategy connectorJarStorageStrategy;

    public ConnectorPackageService(SeaTunnelServer seaTunnelServer) {
        this.seaTunnelServer = seaTunnelServer;
        this.seaTunnelConfig = seaTunnelServer.getSeaTunnelConfig();
        this.connectorJarStorageConfig =
                seaTunnelConfig.getEngineConfig().getConnectorJarStorageConfig();
        this.nodeEngine = seaTunnelServer.getNodeEngine();
        this.connectorJarStorageStrategy =
                StorageStrategyFactory.of(
                        connectorJarStorageConfig.getStorageMode(),
                        connectorJarStorageConfig,
                        seaTunnelServer);
    }

    public ConnectorJarIdentifier storageConnectorJarFile(long jobId, Data connectorJarData) {
        // deserialize connector jar package data
        ConnectorJar connectorJar = nodeEngine.getSerializationService().toObject(connectorJarData);
        /**
         * If the server holds the same Jar package file, there is no need for additional storaged.
         * When the Connector Jar storage strategy is SharedConnectorJarStorageStrategy, the
         * reference count in the connectorJarRefCounters needs to be increased. When the Connector
         * Jar storage strategy is IsolatedConnectorJarStorageStrategy, we don't need to do any
         * processing, just return the identifier of connector jar.
         */
        boolean connectorJarExisted =
                connectorJarStorageStrategy.checkConnectorJarExisted(jobId, connectorJar);
        if (connectorJarExisted) {
            ConnectorJarIdentifier connectorJarIdentifier =
                    connectorJarStorageStrategy.getConnectorJarIdentifier(jobId, connectorJar);
            ConnectorJarStorageMode storageMode = connectorJarStorageConfig.getStorageMode();
            if (storageMode.equals(ConnectorJarStorageMode.SHARED)) {
                SharedConnectorJarStorageStrategy sharedConnectorJarStorageStrategy =
                        (SharedConnectorJarStorageStrategy) connectorJarStorageStrategy;
                sharedConnectorJarStorageStrategy.increaseRefCountForConnectorJar(
                        connectorJarIdentifier);
            }
            return connectorJarStorageStrategy.getConnectorJarIdentifier(jobId, connectorJar);
        }
        ConnectorJarIdentifier connectorJarIdentifier =
                connectorJarStorageStrategy.storageConnectorJarFile(jobId, connectorJar);
        Address masterNodeAddress = nodeEngine.getClusterService().getMasterAddress();
        Collection<Member> memberList = nodeEngine.getClusterService().getMembers();
        memberList.forEach(
                member -> {
                    Address address = member.getAddress();
                    if (!address.equals(masterNodeAddress)) {
                        sendConnectorJarToMemberNode(connectorJarIdentifier, connectorJar, address);
                    }
                });
        return connectorJarIdentifier;
    }

    private void sendConnectorJarToMemberNode(
            ConnectorJarIdentifier connectorJarIdentifier,
            ConnectorJar connectorJar,
            Address address) {
        InvocationFuture<Object> invocationFuture =
                NodeEngineUtil.sendOperationToMemberNode(
                        nodeEngine,
                        new SendConnectorJarToMemberNodeOperation(
                                connectorJar, connectorJarIdentifier),
                        address);
        invocationFuture.join();
    }

    public void cleanUpWhenJobFinished(
            long jobId, List<ConnectorJarIdentifier> connectorJarIdentifierList) {
        connectorJarStorageStrategy.cleanUpWhenJobFinished(jobId, connectorJarIdentifierList);
    }
}
