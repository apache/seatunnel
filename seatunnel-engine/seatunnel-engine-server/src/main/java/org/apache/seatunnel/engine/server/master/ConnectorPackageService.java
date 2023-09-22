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

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.job.SeaTunnelHazelcastClient;
import org.apache.seatunnel.engine.server.task.operation.SendConnectorJarToMemberNodeOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.spi.ClientClusterService;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ConnectorPackageService {

    private static final ILogger LOGGER = Logger.getLogger(ConnectorPackageService.class);

    private final SeaTunnelServer seaTunnelServer;

    private final SeaTunnelConfig seaTunnelConfig;

    private final ConnectorJarStorageConfig connectorJarStorageConfig;

    private final NodeEngineImpl nodeEngine;

    private ConnectorJarStorageStrategy connectorJarStorageStrategy;

    private SeaTunnelHazelcastClient seaTunnelHazelcastClient;

    private final ScheduledExecutorService masterActiveListener;

    /** If this node is a master node */
    private volatile boolean isActive = false;

    public ConnectorPackageService(SeaTunnelServer seaTunnelServer) {
        this.seaTunnelServer = seaTunnelServer;
        this.seaTunnelConfig = seaTunnelServer.getSeaTunnelConfig();
        this.connectorJarStorageConfig =
                seaTunnelConfig.getEngineConfig().getConnectorJarStorageConfig();
        this.nodeEngine = seaTunnelServer.getNodeEngine();
        masterActiveListener = Executors.newSingleThreadScheduledExecutor();
        masterActiveListener.scheduleAtFixedRate(
                this::checkNewActiveMaster, 0, 100, TimeUnit.MILLISECONDS);
    }

    public ConnectorJarIdentifier storageConnectorJarFile(long jobId, Data connectorJarData) {
        // deserialize connector jar package data
        ConnectorJar connectorJar = nodeEngine.getSerializationService().toObject(connectorJarData);
        ConnectorJarIdentifier connectorJarIdentifier =
                connectorJarStorageStrategy.storageConnectorJarFile(jobId, connectorJar);
        ClientClusterService clientClusterService =
                seaTunnelHazelcastClient.getHazelcastClient().getClientClusterService();
        Address masterNodeAddress = clientClusterService.getMasterMember().getAddress();
        Collection<Member> memberList = clientClusterService.getMemberList();
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
                                seaTunnelHazelcastClient
                                        .getSerializationService()
                                        .toData(connectorJar),
                                seaTunnelHazelcastClient
                                        .getSerializationService()
                                        .toData(connectorJarIdentifier)),
                        address);
        invocationFuture.join();
    }

    public void cleanUpWhenJobFinished(
            long jobId, List<ConnectorJarIdentifier> connectorJarIdentifierList) {
        connectorJarStorageStrategy.cleanUpWhenJobFinished(jobId, connectorJarIdentifierList);
    }

    private void initConnectorPackageService() {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        // The local cluster will generate a random cluster name,
        // which needs to be reset to ensure the correct connection to the cluster.
        clientConfig.setClusterName(seaTunnelConfig.getHazelcastConfig().getClusterName());
        this.seaTunnelHazelcastClient = new SeaTunnelHazelcastClient(clientConfig);
        this.connectorJarStorageStrategy =
                StorageStrategyFactory.of(
                        connectorJarStorageConfig.getStorageMode(),
                        connectorJarStorageConfig,
                        seaTunnelServer,
                        seaTunnelHazelcastClient);
    }

    private void clearConnectorPackageService() {
        seaTunnelHazelcastClient = null;
        this.connectorJarStorageStrategy = null;
    }

    private void checkNewActiveMaster() {
        // Only when the current node is the master node will the connector jar service be provided,
        // which is used to maintain the jar package files from all currently executing jobs
        // and provide download services for the task execution nodes.
        try {
            if (!isActive && this.seaTunnelServer.isMasterNode()) {
                LOGGER.info(
                        "This node become a new active master node, begin init connector package service");
                initConnectorPackageService();
                isActive = true;
            } else if (isActive && !this.seaTunnelServer.isMasterNode()) {
                isActive = false;
                LOGGER.info(
                        "This node become leave active master node, begin clear connector package service");
                clearConnectorPackageService();
            }
        } catch (Exception e) {
            isActive = false;
            LOGGER.severe(ExceptionUtils.getMessage(e));
            throw new SeaTunnelEngineException("check new active master error, stop loop", e);
        }
    }

    /**
     * return true if this node is a master node and the connector jar package service init
     * finished.
     */
    public boolean isConnectorPackageServiceActive() {
        return isActive;
    }
}
