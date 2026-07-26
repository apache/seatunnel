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

package org.apache.seatunnel.engine.server.service.jar;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageMode;
import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.RefCount;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.task.operation.SendConnectorJarToMemberNodeOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.cluster.Address;
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
        this(seaTunnelServer, null);
    }

    /**
     * Creates the connector package service with an optional storage strategy override.
     *
     * <p>The override keeps replication retry semantics independently testable without changing the
     * production strategy selection.
     *
     * @param seaTunnelServer local SeaTunnel server
     * @param connectorJarStorageStrategy storage strategy override, or {@code null} for the
     *     configured strategy
     */
    ConnectorPackageService(
            SeaTunnelServer seaTunnelServer,
            ConnectorJarStorageStrategy connectorJarStorageStrategy) {
        this.seaTunnelServer = seaTunnelServer;
        this.seaTunnelConfig = seaTunnelServer.getSeaTunnelConfig();
        this.connectorJarStorageConfig =
                seaTunnelConfig.getEngineConfig().getConnectorJarStorageConfig();
        this.nodeEngine = seaTunnelServer.getNodeEngine();
        this.connectorJarStorageStrategy =
                connectorJarStorageStrategy == null
                        ? StorageStrategyFactory.of(
                                connectorJarStorageConfig.getStorageMode(),
                                connectorJarStorageConfig,
                                seaTunnelServer)
                        : connectorJarStorageStrategy;
    }

    public synchronized ConnectorJarIdentifier storageConnectorJarFile(
            long jobId, Data connectorJarData) {
        ConnectorJar connectorJar = nodeEngine.getSerializationService().toObject(connectorJarData);
        /*
         * A local file can remain after a previous fan-out was only partially acknowledged.
         * Repeating the idempotent remote writes repairs missing copies before committing the next
         * shared reference. Isolated storage has no reference count to update.
         */
        boolean connectorJarExisted =
                connectorJarStorageStrategy.checkConnectorJarExisted(jobId, connectorJar);
        ConnectorJarStorageMode storageMode = connectorJarStorageConfig.getStorageMode();
        ConnectorJarIdentifier connectorJarIdentifier;
        if (connectorJarExisted) {
            connectorJarIdentifier =
                    connectorJarStorageStrategy.getConnectorJarIdentifier(jobId, connectorJar);
            if (storageMode.equals(ConnectorJarStorageMode.SHARED)) {
                SharedConnectorJarStorageStrategy sharedConnectorJarStorageStrategy =
                        (SharedConnectorJarStorageStrategy) connectorJarStorageStrategy;
                boolean referenceReserved =
                        sharedConnectorJarStorageStrategy.increaseRefCountForConnectorJar(
                                connectorJarIdentifier);
                if (!referenceReserved) {
                    // Cleanup removed the record after the existence check. Rebuild local storage
                    // and its first reference before remote replication.
                    connectorJarIdentifier =
                            connectorJarStorageStrategy.storageConnectorJarFile(
                                    jobId, connectorJar);
                }
            }
        } else {
            connectorJarIdentifier =
                    connectorJarStorageStrategy.storageConnectorJarFile(jobId, connectorJar);
        }
        try {
            replicateConnectorJarToMembers(connectorJarIdentifier, connectorJar);
        } catch (RuntimeException e) {
            // Roll back the local shared reference until every server member acknowledges its
            // copy. A retry can then fan out again and commit exactly one reference.
            if (storageMode.equals(ConnectorJarStorageMode.SHARED)) {
                SharedConnectorJarStorageStrategy sharedConnectorJarStorageStrategy =
                        (SharedConnectorJarStorageStrategy) connectorJarStorageStrategy;
                sharedConnectorJarStorageStrategy.rollbackConnectorJarRefCount(
                        connectorJarIdentifier);
            }
            throw e;
        }
        return connectorJarIdentifier;
    }

    /**
     * Replicates a connector jar to every other server member.
     *
     * <p>Remote writes are idempotent, so retrying this fan-out repairs a partially completed
     * replication without replacing existing copies.
     *
     * @param connectorJarIdentifier connector jar storage identifier
     * @param connectorJar connector jar payload
     */
    private void replicateConnectorJarToMembers(
            ConnectorJarIdentifier connectorJarIdentifier, ConnectorJar connectorJar) {
        nodeEngine
                .getClusterService()
                .getMembers()
                .forEach(
                        member -> {
                            Address address = member.getAddress();
                            if (!address.equals(nodeEngine.getThisAddress())) {
                                sendConnectorJarToMemberNode(
                                        connectorJarIdentifier, connectorJar, address);
                            }
                        });
    }

    /**
     * Sends one connector jar replica and waits for the remote write acknowledgement.
     *
     * @param connectorJarIdentifier connector jar storage identifier
     * @param connectorJar connector jar payload
     * @param address destination server member
     */
    void sendConnectorJarToMemberNode(
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

    public synchronized void cleanUpWhenJobFinished(
            long jobId, List<ConnectorJarIdentifier> connectorJarIdentifierList) {
        connectorJarStorageStrategy.cleanUpWhenJobFinished(jobId, connectorJarIdentifierList);
    }

    public int getTrackedConnectorJarCount() {
        return nodeEngine
                .getHazelcastInstance()
                .<ConnectorJarIdentifier, RefCount>getMap(Constant.IMAP_CONNECTOR_JAR_REF_COUNTERS)
                .size();
    }

    public long getTotalConnectorJarReferences() {
        Collection<RefCount> refCounts =
                nodeEngine
                        .getHazelcastInstance()
                        .<ConnectorJarIdentifier, RefCount>getMap(
                                Constant.IMAP_CONNECTOR_JAR_REF_COUNTERS)
                        .values();
        long total = 0L;
        for (RefCount refCount : refCounts) {
            if (refCount != null && refCount.getReferences() != null) {
                total += refCount.getReferences();
            }
        }
        return total;
    }
}
