/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.persistence;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.imap.storage.api.IMapStorage;
import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.common.FileConstants;
import org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.map.EntryLoader.MetadataAwareValue;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.spi.impl.NodeEngine;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for FileMapStore.
 *
 * <p>Covers two scenarios introduced/improved by the fix for
 * https://github.com/apache/seatunnel/issues/10883:
 *
 * <ol>
 *   <li>init() with a valid local-fs config succeeds end-to-end (SPI discovery + storage init).
 *   <li>init() with an unknown storage type throws immediately instead of silently failing.
 * </ol>
 *
 * <p>Also covers durability-failure propagation for write-through MapStore calls:
 * store/storeAll/delete/deleteAll must throw when the underlying {@link IMapStorage} reports a
 * failed write, especially after WAL APPEND has been permanently fail-closed.
 *
 * <p>Note: HDFS/S3/OSS backends are NOT tested here because they require Hadoop uber jars and
 * remote infrastructure. Those are covered by IMapFileStorageTest. What we verify here is that the
 * SPI discovery path (FactoryUtil.discoverFactory) is wired correctly through FileMapStore, and
 * that failures surface as exceptions rather than silent no-ops.
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
public class FileMapStoreTest {

    @TempDir Path tempDir;

    /**
     * Verifies that FileMapStore.init() can successfully resolve the "hdfs" factory via SPI and
     * initialize local-filesystem storage without errors.
     *
     * <p>This test fails if ServicesResourceTransformer is absent from the shade config, because
     * ServiceLoader would return no IMapStorageFactory implementations.
     */
    @Test
    public void testInitSucceedsWithLocalFileSystem() {
        FileMapStore store = new FileMapStore();
        Properties props = buildLocalFsProperties(tempDir.toString());

        Assertions.assertDoesNotThrow(
                () -> store.init(mock(HazelcastInstance.class), props, "test-map"),
                "FileMapStore.init() should succeed with local-fs config. "
                        + "If this fails with 'Could not find any factories', "
                        + "ServicesResourceTransformer is likely missing from seatunnel-starter shade config.");

        store.destroy();
    }

    /**
     * Verifies that FileMapStore.init() throws (rather than silently swallowing) when the storage
     * type is unknown.
     *
     * <p>Before the fix, Hazelcast's MapLoader lifecycle could absorb the exception and leave
     * mapStorage null, causing NullPointerException on the first store/load call with no indication
     * of the real root cause.
     */
    @Test
    public void testInitThrowsOnUnknownStorageType() {
        FileMapStore store = new FileMapStore();
        Properties props = new Properties();
        props.setProperty("type", "non-existent-storage-type");

        RuntimeException ex =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> store.init(mock(HazelcastInstance.class), props, "test-map"),
                        "FileMapStore.init() must throw on unknown storage type instead of silently failing.");

        String msg = ex.getMessage() != null ? ex.getMessage() : "";
        Assertions.assertTrue(
                msg.contains("non-existent-storage-type")
                        || msg.contains("Could not find any factories"),
                "Exception message should identify the unknown type. Got: " + msg);
    }

    @Test
    public void testMetricsSnapshotMapSkipsPersistentStorageInitialization() {
        FileMapStore store = new FileMapStore();
        Properties props = new Properties();
        props.setProperty("type", "non-existent-storage-type");

        Assertions.assertDoesNotThrow(
                () ->
                        store.init(
                                mock(HazelcastInstance.class),
                                props,
                                EngineStateStoreNames.RUNNING_JOB_METRICS));
        Assertions.assertEquals(Collections.emptySet(), store.loadAllKeys());
        Assertions.assertDoesNotThrow(() -> store.store("k", new MetadataAwareValue<>("v")));
        Assertions.assertDoesNotThrow(() -> store.delete("k"));

        store.destroy();
    }

    @Test
    public void testStoreThrowsWhenUnderlyingPersistenceFails() throws Exception {
        FileMapStore store = new FileMapStore();
        IMapStorage failingStorage = mock(IMapStorage.class);
        when(failingStorage.store(any(), any(), anyLong())).thenReturn(false);
        when(failingStorage.isAppendPermanentlyBlocked()).thenReturn(false);
        setMapStorage(store, failingStorage);

        IMapStorageException ex =
                Assertions.assertThrows(
                        IMapStorageException.class,
                        () -> store.store("checkpoint-id", new MetadataAwareValue<>(1L)));
        Assertions.assertTrue(
                ex.getMessage().contains("failed to persist durably"),
                "Expected durability failure message, got: " + ex.getMessage());
    }

    @Test
    public void testStoreThrowsExplicitlyWhenWalAppendIsPermanentlyFailClosed() throws Exception {
        FileMapStore store = new FileMapStore();
        IMapStorage blockedStorage = mock(IMapStorage.class);
        when(blockedStorage.store(any(), any(), anyLong())).thenReturn(false);
        when(blockedStorage.isAppendPermanentlyBlocked()).thenReturn(true);
        setMapStorage(store, blockedStorage);

        IMapStorageException ex =
                Assertions.assertThrows(
                        IMapStorageException.class,
                        () -> store.store("checkpoint-id", new MetadataAwareValue<>(2L)));
        Assertions.assertTrue(
                ex.getMessage().contains("permanently fail-closed"),
                "Expected fail-closed message, got: " + ex.getMessage());
        Assertions.assertTrue(
                ex.getMessage().contains("restart"),
                "Expected restart guidance, got: " + ex.getMessage());
    }

    @Test
    public void testStoreAllThrowsWhenAnyKeyFailsToPersist() throws Exception {
        FileMapStore store = new FileMapStore();
        IMapStorage failingStorage = mock(IMapStorage.class);
        Set<Object> failures = new HashSet<>();
        failures.add("k1");
        when(failingStorage.storeAllWithExpirationTime(anyMap())).thenReturn(failures);
        when(failingStorage.isAppendPermanentlyBlocked()).thenReturn(true);
        setMapStorage(store, failingStorage);

        Map<Object, MetadataAwareValue<Object>> batch = new HashMap<>();
        batch.put("k1", new MetadataAwareValue<>("v1"));
        batch.put("k2", new MetadataAwareValue<>("v2"));
        IMapStorageException ex =
                Assertions.assertThrows(IMapStorageException.class, () -> store.storeAll(batch));
        Assertions.assertTrue(
                ex.getMessage().contains("storeAll"),
                "Expected storeAll in message, got: " + ex.getMessage());
        Assertions.assertTrue(
                ex.getMessage().contains("permanently fail-closed"),
                "Expected fail-closed message, got: " + ex.getMessage());
    }

    @Test
    public void testDeleteThrowsWhenUnderlyingPersistenceFails() throws Exception {
        FileMapStore store = new FileMapStore();
        IMapStorage failingStorage = mock(IMapStorage.class);
        when(failingStorage.delete(any())).thenReturn(false);
        when(failingStorage.isAppendPermanentlyBlocked()).thenReturn(false);
        setMapStorage(store, failingStorage);

        IMapStorageException ex =
                Assertions.assertThrows(
                        IMapStorageException.class, () -> store.delete("old-checkpoint"));
        Assertions.assertTrue(
                ex.getMessage().contains("failed to persist durably"),
                "Expected durability failure message, got: " + ex.getMessage());
        Assertions.assertTrue(
                ex.getMessage().contains("delete"),
                "Expected delete in message, got: " + ex.getMessage());
    }

    @Test
    public void testDeleteThrowsExplicitlyWhenWalAppendIsPermanentlyFailClosed() throws Exception {
        FileMapStore store = new FileMapStore();
        IMapStorage blockedStorage = mock(IMapStorage.class);
        when(blockedStorage.delete(any())).thenReturn(false);
        when(blockedStorage.isAppendPermanentlyBlocked()).thenReturn(true);
        setMapStorage(store, blockedStorage);

        IMapStorageException ex =
                Assertions.assertThrows(
                        IMapStorageException.class, () -> store.delete("old-checkpoint"));
        Assertions.assertTrue(
                ex.getMessage().contains("permanently fail-closed"),
                "Expected fail-closed message, got: " + ex.getMessage());
        Assertions.assertTrue(
                ex.getMessage().contains("restart"),
                "Expected restart guidance, got: " + ex.getMessage());
        Assertions.assertTrue(
                ex.getMessage().contains("delete"),
                "Expected delete in message, got: " + ex.getMessage());
    }

    @Test
    public void testDeleteAllThrowsWhenAnyKeyFailsToPersist() throws Exception {
        FileMapStore store = new FileMapStore();
        IMapStorage failingStorage = mock(IMapStorage.class);
        Set<Object> failures = new HashSet<>();
        failures.add("k1");
        when(failingStorage.deleteAll(any())).thenReturn(failures);
        when(failingStorage.isAppendPermanentlyBlocked()).thenReturn(true);
        setMapStorage(store, failingStorage);

        IMapStorageException ex =
                Assertions.assertThrows(
                        IMapStorageException.class,
                        () -> store.deleteAll(Arrays.asList("k1", "k2")));
        Assertions.assertTrue(
                ex.getMessage().contains("deleteAll"),
                "Expected deleteAll in message, got: " + ex.getMessage());
        Assertions.assertTrue(
                ex.getMessage().contains("permanently fail-closed"),
                "Expected fail-closed message, got: " + ex.getMessage());
    }

    private static void setMapStorage(FileMapStore store, IMapStorage mapStorage) throws Exception {
        Field field = FileMapStore.class.getDeclaredField("mapStorage");
        field.setAccessible(true);
        field.set(store, mapStorage);
    }

    /**
     * Hazelcast does not delete an entry from the map store when its TTL expires. Verifies that
     * after a restart an expired entry is not loaded again, and an entry put with a TTL keeps its
     * expiration time instead of never expiring.
     */
    @Test
    public void testEntriesKeepExpirationTimeAcrossRestart() {
        String clusterName = "file-map-store-expiration-" + System.nanoTime();
        String mapName = Constant.IMAP_FINISHED_JOB_STATE;
        long expirationTimeBeforeRestart;

        HazelcastInstance writer = Hazelcast.newHazelcastInstance(hazelcastConfig(clusterName));
        try {
            IMap<Long, String> map = writer.getMap(mapName);
            map.put(1L, "expired", 1, TimeUnit.SECONDS);
            map.put(2L, "expiring", 10, TimeUnit.MINUTES);
            map.put(3L, "never-expiring");
            expirationTimeBeforeRestart = map.getEntryView(2L).getExpirationTime();
            await().atMost(10, TimeUnit.SECONDS).until(() -> !map.containsKey(1L));
        } finally {
            writer.shutdown();
        }

        HazelcastInstance restarted = Hazelcast.newHazelcastInstance(hazelcastConfig(clusterName));
        try {
            IMap<Long, String> map = restarted.getMap(mapName);

            Assertions.assertNull(map.getEntryView(1L));
            Assertions.assertEquals(2, map.size());

            EntryView<Long, String> expiring = map.getEntryView(2L);
            Assertions.assertEquals("expiring", expiring.getValue());
            // Hazelcast keeps record times at second granularity.
            long drift = Math.abs(expiring.getExpirationTime() - expirationTimeBeforeRestart);
            Assertions.assertTrue(
                    drift <= TimeUnit.SECONDS.toMillis(2),
                    "expiration time moved by " + drift + " ms across the restart");

            EntryView<Long, String> neverExpiring = map.getEntryView(3L);
            Assertions.assertEquals("never-expiring", neverExpiring.getValue());
            Assertions.assertEquals(Long.MAX_VALUE, neverExpiring.getExpirationTime());
        } finally {
            restarted.shutdown();
        }
    }

    /**
     * A real Hazelcast write-through put must surface a WAL durability failure to the caller. The
     * store/delete tests above inject a fake {@link IMapStorage}; this one uses the file MapStore
     * Hazelcast actually calls with {@code write-delay-seconds=0}.
     */
    @Test
    public void testWriteThroughPutThrowsWhenWalAppendFails() throws Exception {
        String clusterName = "file-map-store-write-through-failure-" + System.nanoTime();
        String mapName = Constant.IMAP_FINISHED_JOB_STATE;
        HazelcastInstance node = Hazelcast.newHazelcastInstance(hazelcastConfig(clusterName));
        try {
            IMap<Long, String> map = node.getMap(mapName);
            map.put(1L, "durable");
            FileMapStore store = mapStore(node, mapName);
            spyStorage(store);

            IMapStorageException ex =
                    Assertions.assertThrows(
                            IMapStorageException.class, () -> map.put(2L, "not-durable"));
            Assertions.assertTrue(
                    ex.getMessage().contains("permanently fail-closed"),
                    "Expected fail-closed message, got: " + ex.getMessage());
            Assertions.assertFalse(
                    map.containsKey(2L),
                    "a failed write-through put must not leave the in-memory entry behind");
        } finally {
            node.shutdown();
        }
    }

    private static FileMapStore mapStore(HazelcastInstance node, String mapName) {
        HazelcastInstanceImpl impl =
                node instanceof HazelcastInstanceProxy
                        ? ((HazelcastInstanceProxy) node).getOriginal()
                        : (HazelcastInstanceImpl) node;
        NodeEngine nodeEngine = impl.node.getNodeEngine();
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = mapService.getMapServiceContext();
        MapContainer container = context.getMapContainer(mapName);
        MapStoreWrapper wrapper = container.getMapStoreContext().getMapStoreWrapper();
        Assertions.assertInstanceOf(FileMapStore.class, wrapper.getMapStore());
        return (FileMapStore) wrapper.getMapStore();
    }

    /**
     * Wraps the live storage so every {@code store} returns {@code false} and the storage reports
     * fail-close. The {@link FileMapStore} itself stays real; a Mockito spy answers the storage
     * calls. This is not a one-shot failure of the next append, and it does not corrupt the WAL
     * writer.
     */
    private static IMapStorage spyStorage(FileMapStore store) throws Exception {
        Field field = FileMapStore.class.getDeclaredField("mapStorage");
        field.setAccessible(true);
        IMapStorage storage = Mockito.spy((IMapStorage) field.get(store));
        field.set(store, storage);
        Mockito.doReturn(true).when(storage).isAppendPermanentlyBlocked();
        Mockito.doReturn(false).when(storage).store(any(), any(), anyLong());
        return storage;
    }

    private Config hazelcastConfig(String clusterName) {
        MapStoreConfig mapStoreConfig =
                new MapStoreConfig()
                        .setEnabled(true)
                        .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                        .setFactoryClassName(FileMapStoreFactory.class.getName())
                        .setProperties(buildLocalFsProperties(tempDir.toString()));

        Config config = new Config();
        config.setClusterName(clusterName);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.addMapConfig(new MapConfig("engine*").setMapStoreConfig(mapStoreConfig));
        return config;
    }

    private Properties buildLocalFsProperties(String namespace) {
        Properties props = new Properties();
        // "hdfs" is the factoryIdentifier of IMapFileStorageFactory — covers local fs via
        // fs.defaultFS=file:///
        props.setProperty("type", "hdfs");
        props.setProperty("fs.defaultFS", "file:///");
        props.setProperty("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        props.setProperty(FileConstants.FileInitProperties.BUSINESS_KEY, "test");
        props.setProperty(FileConstants.FileInitProperties.NAMESPACE_KEY, namespace);
        props.setProperty(FileConstants.FileInitProperties.CLUSTER_NAME, "test-cluster");
        return props;
    }
}
