/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file;

import org.apache.seatunnel.engine.imap.storage.api.ExpirableValue;
import org.apache.seatunnel.engine.imap.storage.file.common.FileConstants;
import org.apache.seatunnel.engine.imap.storage.file.compaction.HdfsCompactionStorage;
import org.apache.seatunnel.engine.imap.storage.file.config.CompactionConfig;
import org.apache.seatunnel.engine.imap.storage.file.wal.reader.DefaultReader;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.seatunnel.engine.imap.storage.file.common.FileConstants.FileInitProperties.WRITE_DATA_TIMEOUT_MILLISECONDS_KEY;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

@EnabledOnOs({LINUX, MAC})
public class IMapFileStorageTest {

    @TempDir java.nio.file.Path temporaryDirectory;

    private static final Configuration CONF;

    private static final IMapFileStorage STORAGE;

    static {
        CONF = new Configuration();
        CONF.set("fs.defaultFS", "file:///");
        CONF.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        STORAGE = new IMapFileStorage();

        Map<String, Object> properties = new HashMap<>();
        properties.put("fs.defaultFS", "file:///");
        properties.put("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        properties.put(FileConstants.FileInitProperties.BUSINESS_KEY, "random");
        properties.put(FileConstants.FileInitProperties.NAMESPACE_KEY, "/tmp/imap-kris-test/2");
        properties.put(FileConstants.FileInitProperties.CLUSTER_NAME, "test-one");
        properties.put(WRITE_DATA_TIMEOUT_MILLISECONDS_KEY, 60L);

        STORAGE.initialize(properties);
    }

    @Test
    void testAll() {

        List<Object> keys = new ArrayList<>();
        String key1Index = "key1";
        String key2Index = "key2";
        String key50Index = "key50";

        AtomicInteger dataSize = new AtomicInteger();
        Long keyValue = 123456789L;
        for (int i = 0; i < 100; i++) {
            String key = "key" + i;
            Long value = System.currentTimeMillis();

            if (i == 50) {
                // delete
                STORAGE.delete(key1Index);
                keys.remove(key1Index);
                // update
                STORAGE.store(key2Index, keyValue);
                keys.add(key2Index);
                value = keyValue;
                new Thread(() -> dataSize.set(STORAGE.loadAll().size())).start();
            }
            STORAGE.store(key, value);
            keys.add(key);
            STORAGE.delete(key1Index);
            keys.remove(key1Index);
        }

        await().atMost(1, TimeUnit.SECONDS).until(dataSize::get, size -> size > 0);
        Map<Object, Object> loadAllDatas = STORAGE.loadAll();
        Assertions.assertTrue(dataSize.get() >= 50);
        Assertions.assertEquals(keyValue, loadAllDatas.get(key50Index));
        Assertions.assertEquals(keyValue, loadAllDatas.get(key2Index));
        Assertions.assertNull(loadAllDatas.get(key1Index));

        STORAGE.deleteAll(keys);
    }

    @Test
    void testStoreWithExpirationTime() {
        long expirationTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
        STORAGE.store("expiring", 1L, expirationTime);
        STORAGE.store("expired", 2L, System.currentTimeMillis() - 1);
        Map<Object, ExpirableValue> batch = new HashMap<>();
        batch.put("batch-expiring", new ExpirableValue(3L, expirationTime));
        batch.put("batch-never-expiring", new ExpirableValue(4L));
        Assertions.assertTrue(STORAGE.storeAllWithExpirationTime(batch).isEmpty());

        Map<Object, ExpirableValue> loaded = STORAGE.loadAllWithExpirationTime();
        Assertions.assertEquals(new ExpirableValue(1L, expirationTime), loaded.get("expiring"));
        Assertions.assertEquals(
                new ExpirableValue(3L, expirationTime), loaded.get("batch-expiring"));
        Assertions.assertEquals(new ExpirableValue(4L), loaded.get("batch-never-expiring"));
        Assertions.assertFalse(loaded.containsKey("expired"));
        Assertions.assertFalse(STORAGE.loadAll().containsKey("expired"));
        Assertions.assertFalse(STORAGE.loadAllKeys().contains("expired"));

        STORAGE.deleteAll(
                Arrays.asList("expiring", "expired", "batch-expiring", "batch-never-expiring"));
    }

    @Test
    void testStoreArray() {
        Long[] data = new Long[10];
        data[6] = 111111111L;
        STORAGE.store("array", data);
        Long[] array = (Long[]) STORAGE.loadAll().get("array");
        Assertions.assertEquals(array[6], 111111111L);
    }

    @Test
    void testPeriodicCompactionPreservesRawMutationsAndStopsWithLastWriter() throws Exception {
        IMapFileStorage first = new IMapFileStorage();
        IMapFileStorage second = new IMapFileStorage();
        first.initialize(compactionProperties("map"));
        second.initialize(compactionProperties("map"));
        Path firstRegion = regionPath(first);
        Path secondRegion = regionPath(second);
        try {
            Assertions.assertNotEquals(first.region, second.region);
            for (int i = 0; i < 40; i++) {
                Assertions.assertTrue(first.store("hot", "value"));
            }
            Assertions.assertTrue(first.store("deleted", "old"));
            Assertions.assertTrue(first.delete("deleted"));
            Assertions.assertTrue(first.store("expired", "value", System.currentTimeMillis() - 1));
            long expirationTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
            Assertions.assertTrue(first.store("expiring", "value", expirationTime));

            await().atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Path snapshot = snapshot(first, firstRegion);
                                Assertions.assertNotNull(snapshot);
                                DefaultReader reader = new DefaultReader();
                                reader.initialize(first.fs, new ProtoStuffSerializer());
                                Assertions.assertEquals(4, reader.readAllData(snapshot).size());
                            });
            Assertions.assertEquals(
                    new ExpirableValue("value", expirationTime),
                    first.loadAllWithExpirationTime().get("expiring"));
            Assertions.assertFalse(first.loadAllKeys().contains("deleted"));
            Assertions.assertFalse(first.loadAllKeys().contains("expired"));
            Assertions.assertEquals(1, compactionThreads());

            first.destroy(false);
            Set<Path> previousRegionFiles = files(second, firstRegion);
            for (int i = 0; i < 20; i++) {
                Assertions.assertTrue(second.store("second", "value"));
            }
            await().atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertNotNull(snapshot(second, secondRegion)));
            Assertions.assertEquals(previousRegionFiles, files(second, firstRegion));
            Assertions.assertEquals("value", second.loadAll().get("hot"));
            Assertions.assertEquals("value", second.loadAll().get("second"));
            Assertions.assertEquals(1, compactionThreads());
        } finally {
            first.destroy(false);
            second.destroy(false);
        }
        await().atMost(5, TimeUnit.SECONDS).until(() -> compactionThreads() == 0);
    }

    @Test
    void testCompactionIsOptInAndRejectsUnsupportedSettings() throws Exception {
        Map<String, Object> properties = compactionProperties("legacy");
        properties.remove(CompactionConfig.ENABLED.key());
        IMapFileStorage storage = new IMapFileStorage();
        storage.initialize(properties);
        try {
            Assertions.assertTrue(storage.store("key", "value"));
            Assertions.assertTrue(storage.fs.exists(new Path(regionPath(storage), "wal.txt")));
            Assertions.assertFalse(
                    storage.fs.exists(
                            new Path(regionPath(storage), HdfsCompactionStorage.CURRENT_FILE)));
            Assertions.assertEquals(Collections.singletonMap("key", "value"), storage.loadAll());
        } finally {
            storage.destroy(false);
        }

        Map<String, Object> unsupported = compactionProperties("unsupported");
        unsupported.put("storage.type", "s3");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new IMapFileStorage().initialize(unsupported));
        for (String key :
                Arrays.asList(
                        CompactionConfig.SEGMENT_SIZE.key(),
                        CompactionConfig.INTERVAL_SECONDS.key())) {
            Map<String, Object> invalid = compactionProperties("invalid");
            invalid.put(key, "0");
            Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> new IMapFileStorage().initialize(invalid));
        }
    }

    /**
     * Exercises HDFS sync, atomic token overwrite, reclamation and restart against a real cluster.
     */
    @Test
    @EnabledIfSystemProperty(named = "imap.test.hdfs-uri", matches = "hdfs://.+")
    void testCompactionAndRestartOnHdfs() throws Exception {
        Map<String, Object> properties = compactionProperties("hdfs-map");
        properties.put("fs.defaultFS", System.getProperty("imap.test.hdfs-uri"));
        properties.put(
                FileConstants.FileInitProperties.NAMESPACE_KEY,
                "/seatunnel-compaction-test-" + UUID.randomUUID());
        properties.put("seatunnel.hadoop.dfs.client.use.datanode.hostname", "true");
        properties.put("seatunnel.hadoop.dfs.replication", "1");
        IMapFileStorage storage = new IMapFileStorage();
        IMapFileStorage restarted = new IMapFileStorage();
        storage.initialize(properties);
        boolean stopped = false;
        boolean restartedInitialized = false;
        long expirationTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
        try {
            Assertions.assertEquals("hdfs", storage.fs.getUri().getScheme());
            for (int i = 0; i < 30; i++) {
                Assertions.assertTrue(storage.store("hot", "value"));
            }
            Assertions.assertTrue(storage.store("deleted", "old"));
            Assertions.assertTrue(storage.delete("deleted"));
            Assertions.assertTrue(
                    storage.store("expired", "value", System.currentTimeMillis() - 1));
            Assertions.assertTrue(storage.store("expiring", "value", expirationTime));
            Path region = regionPath(storage);
            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Path snapshot = snapshot(storage, region);
                                Assertions.assertNotNull(snapshot);
                                DefaultReader reader = new DefaultReader();
                                reader.initialize(storage.fs, new ProtoStuffSerializer());
                                Assertions.assertEquals(4, reader.readAllData(snapshot).size());
                                Assertions.assertEquals(
                                        1,
                                        files(storage, region).stream()
                                                .filter(path -> path.getName().endsWith("wal.txt"))
                                                .count());
                            });
            storage.destroy(false);
            stopped = true;
            restarted.initialize(properties);
            restartedInitialized = true;
            Map<Object, ExpirableValue> expected = new HashMap<>();
            expected.put("hot", new ExpirableValue("value"));
            expected.put("expiring", new ExpirableValue("value", expirationTime));
            Assertions.assertEquals(expected, restarted.loadAllWithExpirationTime());
            Assertions.assertFalse(restarted.loadAllKeys().contains("deleted"));
            Assertions.assertFalse(restarted.loadAllKeys().contains("expired"));
        } finally {
            if (!stopped) {
                storage.destroy(false);
            }
            if (restartedInitialized) {
                restarted.destroy(true);
            }
        }
    }

    private Map<String, Object> compactionProperties(String map) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("fs.defaultFS", "file:///");
        properties.put(
                FileConstants.FileInitProperties.NAMESPACE_KEY, temporaryDirectory.toString());
        properties.put(FileConstants.FileInitProperties.CLUSTER_NAME, "compaction");
        properties.put(FileConstants.FileInitProperties.BUSINESS_KEY, map);
        properties.put(CompactionConfig.ENABLED.key(), "true");
        properties.put(CompactionConfig.INTERVAL_SECONDS.key(), "1");
        properties.put(CompactionConfig.SEGMENT_SIZE.key(), "1");
        return properties;
    }

    private Path regionPath(IMapFileStorage storage) {
        return new Path(
                new Path(new Path(storage.namespace, storage.clusterName), storage.businessName),
                storage.region);
    }

    private Path snapshot(IMapFileStorage storage, Path region) throws IOException {
        String token =
                HdfsCompactionStorage.readToken(
                        storage.fs, new Path(region, HdfsCompactionStorage.CURRENT_FILE));
        for (Path path : files(storage, region)) {
            if (path.getName().equals(HdfsCompactionStorage.SNAPSHOT_PREFIX + token + "-wal.txt")) {
                return path;
            }
        }
        return null;
    }

    private Set<Path> files(IMapFileStorage storage, Path region) throws IOException {
        Set<Path> files = new HashSet<>();
        for (org.apache.hadoop.fs.FileStatus status : storage.fs.listStatus(region)) {
            files.add(status.getPath());
        }
        return files;
    }

    private long compactionThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(
                        thread ->
                                thread.isAlive()
                                        && "seatunnel-imap-compaction".equals(thread.getName()))
                .count();
    }

    @AfterAll
    static void afterAll() throws IOException {
        FileSystem.get(CONF).delete(new Path("/tmp/imap-kris-test/2"), true);
    }
}
