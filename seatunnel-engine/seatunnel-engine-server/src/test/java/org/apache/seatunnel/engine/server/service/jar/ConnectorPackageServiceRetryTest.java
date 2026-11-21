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

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageMode;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.ConnectorJarType;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Arrays;
import java.util.LinkedHashSet;

/**
 * Covers connector jar replication retries after only part of the cluster accepted the payload.
 *
 * <p>The test protects failover readiness and shared reference-count idempotency.
 */
public class ConnectorPackageServiceRetryTest {

    /**
     * Verifies that retry repairs every remote member and commits exactly one shared reference.
     *
     * <p>The assertions protect idempotent fan-out after a partially acknowledged first attempt.
     */
    @Test
    void testRetryReplicatesAfterPartialFailureWithoutDoubleCounting() {
        long jobId = 1L;
        byte[] jarData = new byte[] {1, 2, 3};
        ConnectorJar connectorJar =
                ConnectorJar.createConnectorJar(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR, jarData, "test.jar");
        ConnectorJarIdentifier identifier =
                ConnectorJarIdentifier.of(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR, "test.jar", "/tmp/test.jar");
        Data connectorJarData = Mockito.mock(Data.class);
        SeaTunnelConfig config = new SeaTunnelConfig();
        config.getEngineConfig()
                .getConnectorJarStorageConfig()
                .setStorageMode(ConnectorJarStorageMode.SHARED);
        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        NodeEngineImpl nodeEngine = Mockito.mock(NodeEngineImpl.class);
        SerializationService serializationService = Mockito.mock(SerializationService.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        SharedConnectorJarStorageStrategy storageStrategy =
                Mockito.mock(SharedConnectorJarStorageStrategy.class);
        Address localAddress = Address.createUnresolvedAddress("localhost", 5801);
        Member firstRemote = Mockito.mock(Member.class);
        Member secondRemote = Mockito.mock(Member.class);
        Mockito.when(firstRemote.getAddress())
                .thenReturn(Address.createUnresolvedAddress("localhost", 5802));
        Mockito.when(secondRemote.getAddress())
                .thenReturn(Address.createUnresolvedAddress("localhost", 5803));
        Mockito.when(server.getSeaTunnelConfig()).thenReturn(config);
        Mockito.when(server.getNodeEngine()).thenReturn(nodeEngine);
        Mockito.when(nodeEngine.getSerializationService()).thenReturn(serializationService);
        Mockito.when(serializationService.toObject(connectorJarData)).thenReturn(connectorJar);
        Mockito.when(nodeEngine.getClusterService()).thenReturn(clusterService);
        Mockito.when(nodeEngine.getThisAddress()).thenReturn(localAddress);
        Mockito.when(clusterService.getMembers())
                .thenReturn(new LinkedHashSet<Member>(Arrays.asList(firstRemote, secondRemote)));
        Mockito.when(storageStrategy.checkConnectorJarExisted(jobId, connectorJar))
                .thenReturn(false, true);
        Mockito.when(storageStrategy.storageConnectorJarFile(jobId, connectorJar))
                .thenReturn(identifier);
        Mockito.when(storageStrategy.getConnectorJarIdentifier(jobId, connectorJar))
                .thenReturn(identifier);
        Mockito.when(storageStrategy.increaseRefCountForConnectorJar(identifier)).thenReturn(true);
        ConnectorPackageService connectorPackageService =
                Mockito.spy(new ConnectorPackageService(server, storageStrategy));
        SeaTunnelEngineException replicationFailure =
                new SeaTunnelEngineException("second member storage failed");
        Mockito.doNothing()
                .doThrow(replicationFailure)
                .doNothing()
                .doNothing()
                .when(connectorPackageService)
                .sendConnectorJarToMemberNode(
                        Mockito.eq(identifier),
                        Mockito.eq(connectorJar),
                        Mockito.any(Address.class));

        SeaTunnelEngineException firstAttempt =
                Assertions.assertThrows(
                        SeaTunnelEngineException.class,
                        () ->
                                connectorPackageService.storageConnectorJarFile(
                                        jobId, connectorJarData));
        Assertions.assertSame(replicationFailure, firstAttempt);
        Mockito.verify(storageStrategy).rollbackConnectorJarRefCount(identifier);
        Mockito.verify(storageStrategy, Mockito.never())
                .increaseRefCountForConnectorJar(identifier);
        Mockito.clearInvocations(storageStrategy, connectorPackageService);

        ConnectorJarIdentifier retriedIdentifier =
                connectorPackageService.storageConnectorJarFile(jobId, connectorJarData);

        Assertions.assertSame(identifier, retriedIdentifier);
        InOrder retryOrder = Mockito.inOrder(storageStrategy, connectorPackageService);
        retryOrder.verify(storageStrategy).increaseRefCountForConnectorJar(identifier);
        retryOrder
                .verify(connectorPackageService, Mockito.times(2))
                .sendConnectorJarToMemberNode(
                        Mockito.eq(identifier),
                        Mockito.eq(connectorJar),
                        Mockito.any(Address.class));
    }
}
