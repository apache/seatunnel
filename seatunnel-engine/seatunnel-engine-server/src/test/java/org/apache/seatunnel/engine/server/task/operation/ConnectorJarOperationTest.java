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

package org.apache.seatunnel.engine.server.task.operation;

import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.ConnectorJarType;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.service.jar.ServerConnectorPackageClient;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Covers connector jar replication and cleanup operations on coordinator-only server members.
 *
 * <p>The tests verify role-independent dispatch and remote failure propagation.
 */
public class ConnectorJarOperationTest {

    /**
     * Verifies that connector jar operations use the role-independent server client when the worker
     * task service is absent.
     */
    @Test
    void testStoresAndDeletesJarOnCoordinatorOnlyMember() throws Exception {
        byte[] jarData = new byte[] {1, 2, 3};
        ConnectorJar connectorJar =
                ConnectorJar.createConnectorJar(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR, jarData, "test.jar");
        ConnectorJarIdentifier identifier =
                ConnectorJarIdentifier.of(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR, "test.jar", "/tmp/test.jar");
        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        ServerConnectorPackageClient client = Mockito.mock(ServerConnectorPackageClient.class);
        Mockito.when(server.getTaskExecutionService()).thenReturn(null);
        Mockito.when(server.getServerConnectorPackageClient()).thenReturn(client);

        SendConnectorJarToMemberNodeOperation sendOperation =
                new SendConnectorJarToMemberNodeOperation(connectorJar, identifier);
        sendOperation.setService(server);
        sendOperation.run();

        DeleteConnectorJarInExecutionNode deleteOperation =
                new DeleteConnectorJarInExecutionNode(identifier);
        deleteOperation.setService(server);
        deleteOperation.run();

        Mockito.verify(client).storageConnectorJarFile(jarData, identifier);
        Mockito.verify(client).deleteConnectorJar(identifier);
        Mockito.verify(server, Mockito.never()).getTaskExecutionService();
    }

    /**
     * Verifies that a standby storage failure reaches the sending operation caller.
     *
     * <p>The sender must not treat a missing replica as a successful acknowledgement.
     */
    @Test
    void testStandbyStorageFailureIsPropagated() {
        byte[] jarData = new byte[] {1, 2, 3};
        ConnectorJar connectorJar =
                ConnectorJar.createConnectorJar(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR, jarData, "test.jar");
        ConnectorJarIdentifier identifier =
                ConnectorJarIdentifier.of(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR, "test.jar", "/tmp/test.jar");
        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        ServerConnectorPackageClient client = Mockito.mock(ServerConnectorPackageClient.class);
        SeaTunnelEngineException storageFailure =
                new SeaTunnelEngineException("standby storage failed");
        Mockito.when(server.getServerConnectorPackageClient()).thenReturn(client);
        Mockito.doThrow(storageFailure).when(client).storageConnectorJarFile(jarData, identifier);
        SendConnectorJarToMemberNodeOperation operation =
                new SendConnectorJarToMemberNodeOperation(connectorJar, identifier);
        operation.setService(server);

        SeaTunnelEngineException actual =
                Assertions.assertThrows(SeaTunnelEngineException.class, operation::run);

        Assertions.assertSame(storageFailure, actual);
    }
}
