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
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.ConnectorJarType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import com.hazelcast.spi.impl.NodeEngineImpl;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Covers node-local connector jar persistence used by every server role.
 *
 * <p>The scenarios protect standby coordinator replication and cleanup behavior.
 */
public class ServerConnectorPackageClientTest {

    /**
     * Temporary directory used to verify node-local connector jar persistence.
     *
     * <p>Each test receives an isolated filesystem location.
     */
    @TempDir private Path tempDir;

    /**
     * Verifies that node-local jar storage creates missing parent directories and supports cleanup.
     */
    @Test
    void testStoresAndDeletesJarWhenParentDirectoryDoesNotExist() throws Exception {
        Path jarPath = tempDir.resolve("jobs/1/connectors/test.jar");
        ConnectorJarIdentifier identifier =
                ConnectorJarIdentifier.of(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR, "test.jar", jarPath.toString());
        ServerConnectorPackageClient client =
                new ServerConnectorPackageClient(
                        Mockito.mock(NodeEngineImpl.class), Mockito.mock(SeaTunnelConfig.class));
        byte[] jarData = new byte[] {1, 2, 3};

        client.storageConnectorJarFile(jarData, identifier);

        Assertions.assertArrayEquals(jarData, Files.readAllBytes(jarPath));
        client.deleteConnectorJar(identifier);
        Assertions.assertFalse(Files.exists(jarPath));
    }

    /**
     * Verifies that a failed standby copy is reported and does not leave the local write lock held.
     */
    @Test
    void testStorageFailureIsPropagatedAndWriteLockIsReleased() throws Exception {
        Path invalidParent = tempDir.resolve("not-a-directory");
        Files.write(invalidParent, new byte[] {1});
        ConnectorJarIdentifier invalidIdentifier =
                ConnectorJarIdentifier.of(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR,
                        "test.jar",
                        invalidParent.resolve("test.jar").toString());
        ServerConnectorPackageClient client =
                new ServerConnectorPackageClient(
                        Mockito.mock(NodeEngineImpl.class), Mockito.mock(SeaTunnelConfig.class));

        Assertions.assertThrows(
                SeaTunnelEngineException.class,
                () -> client.storageConnectorJarFile(new byte[] {1, 2, 3}, invalidIdentifier));

        Path validPath = tempDir.resolve("valid/test.jar");
        ConnectorJarIdentifier validIdentifier =
                ConnectorJarIdentifier.of(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR, "test.jar", validPath.toString());
        client.storageConnectorJarFile(new byte[] {4, 5, 6}, validIdentifier);
        Assertions.assertArrayEquals(new byte[] {4, 5, 6}, Files.readAllBytes(validPath));
    }

    /**
     * Verifies that remote cleanup cannot acknowledge a physical deletion that failed.
     *
     * <p>The coordinator must retain lifecycle state until the standby confirms removal.
     */
    @Test
    void testDeleteFailureIsPropagated() throws Exception {
        Path nonEmptyDirectory = tempDir.resolve("non-empty");
        Files.createDirectories(nonEmptyDirectory);
        Files.write(nonEmptyDirectory.resolve("child"), new byte[] {1});
        ConnectorJarIdentifier identifier =
                ConnectorJarIdentifier.of(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR,
                        "test.jar",
                        nonEmptyDirectory.toString());
        ServerConnectorPackageClient client =
                new ServerConnectorPackageClient(
                        Mockito.mock(NodeEngineImpl.class), Mockito.mock(SeaTunnelConfig.class));

        Assertions.assertThrows(
                SeaTunnelEngineException.class, () -> client.deleteConnectorJar(identifier));
    }
}
