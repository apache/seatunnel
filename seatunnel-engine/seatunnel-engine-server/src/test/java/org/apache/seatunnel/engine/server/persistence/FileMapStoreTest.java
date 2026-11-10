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

package org.apache.seatunnel.engine.server.persistence;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.utils.FactoryUtil;
import org.apache.seatunnel.engine.imap.storage.api.ExpirableValue;
import org.apache.seatunnel.engine.imap.storage.api.IMapStorage;
import org.apache.seatunnel.engine.imap.storage.api.IMapStorageFactory;
import org.apache.seatunnel.engine.imap.storage.file.common.FileConstants;
import org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import com.google.auto.service.AutoService;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryLoader.MetadataAwareValue;
import com.hazelcast.map.IMap;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

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
 * <p>Note: HDFS/S3/OSS backends are NOT tested here because they require Hadoop uber jars and
 * remote infrastructure. Those are covered by IMapFileStorageTest. What we verify here is that the
 * SPI discovery path (FactoryUtil.discoverFactory) is wired correctly through FileMapStore, and
 * that failures surface as exceptions rather than silent no-ops.
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
public class FileMapStoreTest {

    private static final String LOCAL_FS_STORAGE = "hdfs";

    /** Storage type of {@link CountingStorageFactory}. */
    private static final String COUNTING_STORAGE = "counting-hdfs";

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
     * Hazelcast loads a map by calling {@code loadAllKeys()} once and then {@code loadAll(keys)}
     * once for every partition that holds keys, and the 40 keys here fall into about 37 partitions.
     * Verifies that the IMap storage is read once for the whole initial load, not once per call.
     */
    @Test
    public void testInitialLoadReadsStorageOnce() {
        String clusterName = "file-map-store-initial-load-" + System.nanoTime();
        String mapName = Constant.IMAP_RUNNING_JOB_STATE;
        int keyCount = 40;

        HazelcastInstance writer =
                Hazelcast.newHazelcastInstance(hazelcastConfig(clusterName, COUNTING_STORAGE));
        try {
            IMap<Long, String> map = writer.getMap(mapName);
            for (long key = 0; key < keyCount; key++) {
                map.put(key, "value-" + key);
            }
        } finally {
            writer.shutdown();
        }

        CountingStorageFactory.resetScans();
        HazelcastInstance restarted =
                Hazelcast.newHazelcastInstance(hazelcastConfig(clusterName, COUNTING_STORAGE));
        try {
            IMap<Long, String> map = restarted.getMap(mapName);
            Assertions.assertEquals(keyCount, map.size());
            for (long key = 0; key < keyCount; key++) {
                Assertions.assertEquals("value-" + key, map.get(key));
            }
            Assertions.assertEquals(1, CountingStorageFactory.scans(mapName));
        } finally {
            restarted.shutdown();
        }
    }

    /**
     * The entries read for the initial load are shared only until the map store is first written
     * to. Verifies that loads running at the same time read the storage once, and that a load after
     * a write reads the storage again and sees the written entry.
     */
    @Test
    public void testFirstWriteEndsSharingOfInitialLoadEntries() throws Exception {
        Properties props = buildLocalFsProperties(tempDir.toString(), COUNTING_STORAGE);
        String businessName = props.getProperty(FileConstants.FileInitProperties.BUSINESS_KEY);

        FileMapStore previousRun = new FileMapStore();
        previousRun.init(mock(HazelcastInstance.class), props, "test-map");
        previousRun.store("k1", new MetadataAwareValue<>("v1"));
        previousRun.destroy();

        CountingStorageFactory.resetScans();
        FileMapStore store = new FileMapStore();
        store.init(mock(HazelcastInstance.class), props, "test-map");
        int loaders = 8;
        ExecutorService executor = Executors.newFixedThreadPool(loaders);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Map<Object, MetadataAwareValue<Object>>>> loads = new ArrayList<>();
            for (int i = 0; i < loaders; i++) {
                Callable<Map<Object, MetadataAwareValue<Object>>> load =
                        () -> {
                            start.await();
                            return store.loadAll(Collections.singletonList("k1"));
                        };
                loads.add(executor.submit(load));
            }
            start.countDown();
            for (Future<Map<Object, MetadataAwareValue<Object>>> load : loads) {
                Assertions.assertEquals("v1", load.get(30, TimeUnit.SECONDS).get("k1").getValue());
            }
            Set<Object> keys = new HashSet<>();
            store.loadAllKeys().forEach(keys::add);
            Assertions.assertEquals(Collections.singleton("k1"), keys);
            Assertions.assertEquals(1, CountingStorageFactory.scans(businessName));

            store.store("k2", new MetadataAwareValue<>("v2"));
            Map<Object, MetadataAwareValue<Object>> loaded =
                    store.loadAll(Arrays.asList("k1", "k2"));
            Assertions.assertEquals("v1", loaded.get("k1").getValue());
            Assertions.assertNotNull(
                    loaded.get("k2"), "a load after a write must read the storage again");
            Assertions.assertEquals("v2", loaded.get("k2").getValue());
            Assertions.assertEquals(2, CountingStorageFactory.scans(businessName));
        } finally {
            executor.shutdownNow();
            store.destroy();
        }
    }

    private Config hazelcastConfig(String clusterName) {
        return hazelcastConfig(clusterName, LOCAL_FS_STORAGE);
    }

    private Config hazelcastConfig(String clusterName, String storageType) {
        MapStoreConfig mapStoreConfig =
                new MapStoreConfig()
                        .setEnabled(true)
                        .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                        .setFactoryClassName(FileMapStoreFactory.class.getName())
                        .setProperties(buildLocalFsProperties(tempDir.toString(), storageType));

        Config config = new Config();
        config.setClusterName(clusterName);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.addMapConfig(new MapConfig("engine*").setMapStoreConfig(mapStoreConfig));
        return config;
    }

    private Properties buildLocalFsProperties(String namespace) {
        return buildLocalFsProperties(namespace, LOCAL_FS_STORAGE);
    }

    private Properties buildLocalFsProperties(String namespace, String storageType) {
        Properties props = new Properties();
        // "hdfs" is the factoryIdentifier of IMapFileStorageFactory — covers local fs via
        // fs.defaultFS=file:///
        props.setProperty("type", storageType);
        props.setProperty("fs.defaultFS", "file:///");
        props.setProperty("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        props.setProperty(FileConstants.FileInitProperties.BUSINESS_KEY, "test");
        props.setProperty(FileConstants.FileInitProperties.NAMESPACE_KEY, namespace);
        props.setProperty(FileConstants.FileInitProperties.CLUSTER_NAME, "test-cluster");
        return props;
    }

    /**
     * Creates the local file IMap storage and counts, by the map it is created for, how often the
     * whole storage is read.
     */
    @AutoService(IMapStorageFactory.class)
    public static class CountingStorageFactory implements IMapStorageFactory {

        private static final Map<String, AtomicInteger> SCANS = new ConcurrentHashMap<>();

        static void resetScans() {
            SCANS.clear();
        }

        static int scans(String businessName) {
            AtomicInteger scans = SCANS.get(businessName);
            return scans == null ? 0 : scans.get();
        }

        @Override
        public String factoryIdentifier() {
            return COUNTING_STORAGE;
        }

        @Override
        public IMapStorage create(Map<String, Object> configuration) {
            IMapStorage storage =
                    FactoryUtil.discoverFactory(
                                    Thread.currentThread().getContextClassLoader(),
                                    IMapStorageFactory.class,
                                    LOCAL_FS_STORAGE)
                            .create(configuration);
            String businessName =
                    String.valueOf(
                            configuration.get(FileConstants.FileInitProperties.BUSINESS_KEY));
            return new CountingStorage(storage, businessName);
        }
    }

    private static final class CountingStorage implements IMapStorage {

        private final IMapStorage delegate;
        private final String businessName;

        private CountingStorage(IMapStorage delegate, String businessName) {
            this.delegate = delegate;
            this.businessName = businessName;
        }

        private void countScan() {
            CountingStorageFactory.SCANS
                    .computeIfAbsent(businessName, name -> new AtomicInteger())
                    .incrementAndGet();
        }

        @Override
        public void initialize(Map<String, Object> properties) {
            delegate.initialize(properties);
        }

        @Override
        public boolean store(Object key, Object value) {
            return delegate.store(key, value);
        }

        @Override
        public boolean store(Object key, Object value, long expirationTime) {
            return delegate.store(key, value, expirationTime);
        }

        @Override
        public Set<Object> storeAll(Map<Object, Object> map) {
            return delegate.storeAll(map);
        }

        @Override
        public Set<Object> storeAllWithExpirationTime(Map<Object, ExpirableValue> map) {
            return delegate.storeAllWithExpirationTime(map);
        }

        @Override
        public boolean delete(Object key) {
            return delegate.delete(key);
        }

        @Override
        public Set<Object> deleteAll(Collection<Object> keys) {
            return delegate.deleteAll(keys);
        }

        @Override
        public Map<Object, Object> loadAll() throws IOException {
            countScan();
            return delegate.loadAll();
        }

        @Override
        public Map<Object, ExpirableValue> loadAllWithExpirationTime() throws IOException {
            countScan();
            return delegate.loadAllWithExpirationTime();
        }

        @Override
        public Set<Object> loadAllKeys() {
            countScan();
            return delegate.loadAllKeys();
        }

        @Override
        public void destroy(boolean deleteAllFileFlag) {
            delegate.destroy(deleteAllFileFlag);
        }
    }
}
