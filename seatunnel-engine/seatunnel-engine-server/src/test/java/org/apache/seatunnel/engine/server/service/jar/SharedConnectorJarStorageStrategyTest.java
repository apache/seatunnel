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
import org.apache.seatunnel.engine.core.job.RefCount;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Covers atomic shared connector jar cleanup claims while uploads reserve new references.
 *
 * <p>The scenarios protect local and remote copies from a stale cleanup timer observation.
 */
public class SharedConnectorJarStorageStrategyTest {

    /**
     * Temporary directory used for shared connector jar files.
     *
     * <p>Each test receives an isolated filesystem location.
     */
    @TempDir private Path tempDir;

    /**
     * Verifies that a stale cleanup callback cannot delete a jar after an upload reserves a
     * reference.
     */
    @Test
    void testCleanupDoesNotDeleteJarWithProvisionalReference() throws Exception {
        ConnectorJarStorageConfig storageConfig = new ConnectorJarStorageConfig();
        storageConfig.setStoragePath(tempDir.toString());
        storageConfig.setCleanupTaskInterval(3600);
        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        NodeEngineImpl nodeEngine = Mockito.mock(NodeEngineImpl.class);
        HazelcastInstanceImpl hazelcastInstance = Mockito.mock(HazelcastInstanceImpl.class);
        @SuppressWarnings("unchecked")
        IMap<ConnectorJarIdentifier, RefCount> refCounts = Mockito.mock(IMap.class);
        Map<ConnectorJarIdentifier, RefCount> backingMap = new HashMap<>();
        Mockito.when(server.getNodeEngine()).thenReturn(nodeEngine);
        Mockito.when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        Mockito.when(
                        hazelcastInstance.<ConnectorJarIdentifier, RefCount>getMap(
                                Mockito.anyString()))
                .thenReturn(refCounts);
        Mockito.when(refCounts.compute(Mockito.any(), Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            ConnectorJarIdentifier identifier = invocation.getArgument(0);
                            BiFunction<ConnectorJarIdentifier, RefCount, RefCount>
                                    remappingFunction = invocation.getArgument(1);
                            RefCount result =
                                    remappingFunction.apply(identifier, backingMap.get(identifier));
                            if (result == null) {
                                backingMap.remove(identifier);
                            } else {
                                backingMap.put(identifier, result);
                            }
                            return result;
                        });
        Path jarPath = tempDir.resolve("connectors/test.jar");
        Files.createDirectories(jarPath.getParent());
        Files.write(jarPath, new byte[] {1, 2, 3});
        ConnectorJarIdentifier identifier =
                ConnectorJarIdentifier.of(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR, "test.jar", jarPath.toString());
        RefCount provisionalReference = new RefCount();
        provisionalReference.setReferences(1L);
        backingMap.put(identifier, provisionalReference);
        SharedConnectorJarStorageStrategy strategy =
                new SharedConnectorJarStorageStrategy(storageConfig, server);

        strategy.deleteConnectorJar(identifier);

        Assertions.assertTrue(Files.exists(jarPath));
        Assertions.assertSame(provisionalReference, backingMap.get(identifier));
        Mockito.verify(refCounts).lock(identifier);
        Mockito.verify(refCounts).unlock(identifier);
    }

    /**
     * Verifies that one failed physical deletion does not terminate later cleanup timer cycles.
     *
     * <p>The failed jar remains eligible for a later cleanup attempt.
     */
    @Test
    void testCleanupTaskContainsPerJarDeletionFailure() {
        ConnectorJarIdentifier identifier =
                ConnectorJarIdentifier.of(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR,
                        "test.jar",
                        tempDir.resolve("test.jar").toString());
        RefCount zeroReference = new RefCount();
        @SuppressWarnings("unchecked")
        IMap<ConnectorJarIdentifier, RefCount> refCounts = Mockito.mock(IMap.class);
        Mockito.when(refCounts.entrySet())
                .thenReturn(
                        java.util.Collections.singletonMap(identifier, zeroReference).entrySet());
        @SuppressWarnings("unchecked")
        Consumer<ConnectorJarIdentifier> cleanupCallback = Mockito.mock(Consumer.class);
        Mockito.doThrow(new SeaTunnelEngineException("remote deletion failed"))
                .when(cleanupCallback)
                .accept(identifier);
        SharedConnectorJarCleanupTask cleanupTask =
                new SharedConnectorJarCleanupTask(cleanupCallback, refCounts);

        Assertions.assertDoesNotThrow(cleanupTask::run);
        Mockito.verify(cleanupCallback).accept(identifier);
    }

    /**
     * Verifies that rollback retains a zero-reference tombstone for cluster-wide timer cleanup.
     *
     * <p>The tombstone keeps partial remote replicas discoverable until every copy is removed.
     */
    @Test
    void testRollbackRetainsCleanupTombstoneForClusterCleanup() throws Exception {
        ConnectorJarStorageConfig storageConfig = new ConnectorJarStorageConfig();
        storageConfig.setStoragePath(tempDir.toString());
        storageConfig.setCleanupTaskInterval(3600);
        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        NodeEngineImpl nodeEngine = Mockito.mock(NodeEngineImpl.class);
        HazelcastInstanceImpl hazelcastInstance = Mockito.mock(HazelcastInstanceImpl.class);
        @SuppressWarnings("unchecked")
        IMap<ConnectorJarIdentifier, RefCount> refCounts = Mockito.mock(IMap.class);
        Map<ConnectorJarIdentifier, RefCount> backingMap = new HashMap<>();
        Mockito.when(server.getNodeEngine()).thenReturn(nodeEngine);
        Mockito.when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        Mockito.when(
                        hazelcastInstance.<ConnectorJarIdentifier, RefCount>getMap(
                                Mockito.anyString()))
                .thenReturn(refCounts);
        Mockito.when(refCounts.compute(Mockito.any(), Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            ConnectorJarIdentifier identifier = invocation.getArgument(0);
                            BiFunction<ConnectorJarIdentifier, RefCount, RefCount>
                                    remappingFunction = invocation.getArgument(1);
                            RefCount result =
                                    remappingFunction.apply(identifier, backingMap.get(identifier));
                            if (result == null) {
                                backingMap.remove(identifier);
                            } else {
                                backingMap.put(identifier, result);
                            }
                            return result;
                        });
        Path jarPath = tempDir.resolve("connectors/test.jar");
        Files.createDirectories(jarPath.getParent());
        Files.write(jarPath, new byte[] {1});
        ConnectorJarIdentifier identifier =
                ConnectorJarIdentifier.of(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR, "test.jar", jarPath.toString());
        RefCount provisionalReference = new RefCount();
        provisionalReference.setReferences(1L);
        backingMap.put(identifier, provisionalReference);
        SharedConnectorJarStorageStrategy strategy =
                Mockito.spy(new SharedConnectorJarStorageStrategy(storageConfig, server));
        Mockito.doNothing().when(strategy).deleteConnectorJarInExecutionNode(identifier);

        Assertions.assertDoesNotThrow(() -> strategy.rollbackConnectorJarRefCount(identifier));

        Assertions.assertNotNull(backingMap.get(identifier));
        Assertions.assertEquals(0L, backingMap.get(identifier).getReferences());
        Assertions.assertTrue(Files.exists(jarPath));

        strategy.deleteConnectorJar(identifier);

        Assertions.assertNull(backingMap.get(identifier));
        Assertions.assertFalse(Files.exists(jarPath));
        Mockito.verify(strategy).deleteConnectorJarInExecutionNode(identifier);
        Mockito.verify(refCounts, Mockito.times(2)).lock(identifier);
        Mockito.verify(refCounts, Mockito.times(2)).unlock(identifier);
    }

    /**
     * Verifies that a failed remote cleanup restores the tombstone for the next timer cycle.
     *
     * <p>The retry must attempt remote deletion again even after the local copy was removed.
     */
    @Test
    void testRemoteCleanupFailureRestoresTombstoneForRetry() throws Exception {
        ConnectorJarStorageConfig storageConfig = new ConnectorJarStorageConfig();
        storageConfig.setStoragePath(tempDir.toString());
        storageConfig.setCleanupTaskInterval(3600);
        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        NodeEngineImpl nodeEngine = Mockito.mock(NodeEngineImpl.class);
        HazelcastInstanceImpl hazelcastInstance = Mockito.mock(HazelcastInstanceImpl.class);
        @SuppressWarnings("unchecked")
        IMap<ConnectorJarIdentifier, RefCount> refCounts = Mockito.mock(IMap.class);
        Map<ConnectorJarIdentifier, RefCount> backingMap = new HashMap<>();
        Mockito.when(server.getNodeEngine()).thenReturn(nodeEngine);
        Mockito.when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        Mockito.when(
                        hazelcastInstance.<ConnectorJarIdentifier, RefCount>getMap(
                                Mockito.anyString()))
                .thenReturn(refCounts);
        Mockito.when(refCounts.compute(Mockito.any(), Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            ConnectorJarIdentifier identifier = invocation.getArgument(0);
                            BiFunction<ConnectorJarIdentifier, RefCount, RefCount>
                                    remappingFunction = invocation.getArgument(1);
                            RefCount result =
                                    remappingFunction.apply(identifier, backingMap.get(identifier));
                            if (result == null) {
                                backingMap.remove(identifier);
                            } else {
                                backingMap.put(identifier, result);
                            }
                            return result;
                        });
        Mockito.when(refCounts.put(Mockito.any(), Mockito.any()))
                .thenAnswer(
                        invocation ->
                                backingMap.put(
                                        invocation.getArgument(0), invocation.getArgument(1)));
        Path jarPath = tempDir.resolve("connectors/retry-test.jar");
        Files.createDirectories(jarPath.getParent());
        Files.write(jarPath, new byte[] {1});
        ConnectorJarIdentifier identifier =
                ConnectorJarIdentifier.of(
                        ConnectorJarType.CONNECTOR_PLUGIN_JAR,
                        "retry-test.jar",
                        jarPath.toString());
        backingMap.put(identifier, new RefCount());
        SharedConnectorJarStorageStrategy strategy =
                Mockito.spy(new SharedConnectorJarStorageStrategy(storageConfig, server));
        SeaTunnelEngineException remoteFailure =
                new SeaTunnelEngineException("remote deletion failed");
        Mockito.doThrow(remoteFailure)
                .doNothing()
                .when(strategy)
                .deleteConnectorJarInExecutionNode(identifier);

        SeaTunnelEngineException firstAttempt =
                Assertions.assertThrows(
                        SeaTunnelEngineException.class,
                        () -> strategy.deleteConnectorJar(identifier));

        Assertions.assertSame(remoteFailure, firstAttempt);
        Assertions.assertNotNull(backingMap.get(identifier));
        Assertions.assertEquals(0L, backingMap.get(identifier).getReferences());
        Assertions.assertFalse(Files.exists(jarPath));

        strategy.deleteConnectorJar(identifier);

        Assertions.assertNull(backingMap.get(identifier));
        Mockito.verify(strategy, Mockito.times(2)).deleteConnectorJarInExecutionNode(identifier);
        Mockito.verify(refCounts, Mockito.times(2)).lock(identifier);
        Mockito.verify(refCounts, Mockito.times(2)).unlock(identifier);
    }
}
