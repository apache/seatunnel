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

import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.ConnectorJarType;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import com.hazelcast.spi.impl.NodeEngineImpl;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Covers job-end cleanup of isolated connector jars.
 *
 * <p>Jar deletion failures are raised as exceptions so the shared cleanup timer can retry them. The
 * isolated strategy has no retry path, so its job-end cleanup must stay best effort.
 */
public class IsolatedConnectorJarStorageStrategyTest {

    /**
     * Temporary directory used as the connector jar storage root.
     *
     * <p>Each test receives an isolated filesystem location.
     */
    @TempDir private Path tempDir;

    /**
     * Verifies that one jar that cannot be deleted neither aborts cleanup of the remaining jars nor
     * escapes into the job master of the already finished job.
     */
    @Test
    void testJobEndCleanupContinuesAfterOneJarFails() {
        ConnectorJarStorageConfig storageConfig = new ConnectorJarStorageConfig();
        storageConfig.setStoragePath(tempDir.toString());
        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        NodeEngineImpl nodeEngine = Mockito.mock(NodeEngineImpl.class);
        Mockito.when(server.getNodeEngine()).thenReturn(nodeEngine);
        ConnectorJarIdentifier failingJar =
                ConnectorJarIdentifier.of(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR,
                        "failing.jar",
                        tempDir.resolve("failing.jar").toString());
        ConnectorJarIdentifier healthyJar =
                ConnectorJarIdentifier.of(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR,
                        "healthy.jar",
                        tempDir.resolve("healthy.jar").toString());
        List<ConnectorJarIdentifier> deletedJars = new ArrayList<>();
        IsolatedConnectorJarStorageStrategy strategy =
                new IsolatedConnectorJarStorageStrategy(storageConfig, server) {
                    @Override
                    public void deleteConnectorJar(ConnectorJarIdentifier connectorJarIdentifier) {
                        if (connectorJarIdentifier == failingJar) {
                            throw new SeaTunnelEngineException("delete failed");
                        }
                        deletedJars.add(connectorJarIdentifier);
                    }
                };

        Assertions.assertDoesNotThrow(
                () -> strategy.cleanUpWhenJobFinished(1L, Arrays.asList(failingJar, healthyJar)));

        Assertions.assertEquals(Collections.singletonList(healthyJar), deletedJars);
    }
}
