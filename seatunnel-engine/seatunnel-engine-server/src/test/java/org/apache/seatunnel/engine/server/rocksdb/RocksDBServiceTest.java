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

package org.apache.seatunnel.engine.server.rocksdb;

import org.apache.seatunnel.engine.common.config.server.MapStoreConfig;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
class RocksDBServiceTest {
    private static final String DB_PATH = "rocksdb_test";
    private static final String STATE_NAME = "default";
    private static String prevProperty;
    private static Path jniTmpDir;
    private RocksDBService rocksDBService;
    private String path;

    @TempDir Path tempDir;

    @BeforeAll
    static void initNative() throws Exception {
        jniTmpDir = Paths.get("target", "rocksdb-jni-tmp").toAbsolutePath();
        Files.createDirectories(jniTmpDir);
        prevProperty = System.getProperty("java.io.tmpdir");
        System.setProperty("java.io.tmpdir", jniTmpDir.toString());
    }

    @AfterAll
    static void cleanupNative() {
        if (prevProperty != null) {
            System.setProperty("java.io.tmpdir", prevProperty);
        } else {
            System.clearProperty("java.io.tmpdir");
        }
        safeDeleteDirectory(jniTmpDir);
    }

    private static void safeDeleteDirectory(Path dir) {
        if (dir == null) return;
        final int maxRetries = 5;
        for (int i = 0; i < maxRetries; i++) {
            try {
                if (Files.exists(dir)) {
                    try (Stream<Path> walk = Files.walk(dir)) {
                        walk.sorted(Comparator.reverseOrder())
                                .forEach(
                                        p -> {
                                            try {
                                                Files.deleteIfExists(p);
                                            } catch (IOException ignore) {
                                            }
                                        });
                    }
                }
                break;
            } catch (IOException e) {
                if (i == maxRetries - 1) {
                    log.warn("Failed to delete JNI tmp dir: {}", dir, e);
                } else {
                    try {
                        Thread.sleep(200L);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        Path dbDir = tempDir.resolve(DB_PATH);
        Files.createDirectories(dbDir);
        path = dbDir.toString();
        rocksDBService = new RocksDBService(path, new MapStoreConfig());
    }

    @AfterEach
    void tearDown() {
        rocksDBService.close(true);
    }

    @Test
    void testPutAndGetData() {
        String key = "testKey";
        String value = "testValue";

        rocksDBService.putData(STATE_NAME, Collections.singletonMap(key, value));
        String retrievedValue = rocksDBService.getData(STATE_NAME, key);

        Assertions.assertEquals(value, retrievedValue);
    }

    @Test
    void testGetAllData() {
        Map<String, String> initialData = new HashMap<>();
        initialData.put("testKey1", "testValue1");
        initialData.put("testKey2", "testValue2");
        initialData.put("testKey3", "testValue3");
        rocksDBService.putData(STATE_NAME, initialData);

        Map<String, String> allData = rocksDBService.getAllData(STATE_NAME);
        for (Map.Entry<String, String> entry : initialData.entrySet()) {
            Assertions.assertEquals(entry.getValue(), allData.get(entry.getKey()));
        }
    }

    @Test
    void testRemoveData() {
        Map<String, String> initialData = new HashMap<>();
        initialData.put("testKey1", "testValue1");
        initialData.put("testKey2", "testValue2");
        initialData.put("testKey3", "testValue3");
        rocksDBService.putData(STATE_NAME, initialData);

        rocksDBService.removeData(STATE_NAME, "testKey2");

        String testKey2 = rocksDBService.getData(STATE_NAME, "testKey2");
        Assertions.assertNull(testKey2);

        Map<String, String> allData = rocksDBService.getAllData(STATE_NAME);
        Assertions.assertEquals(2, allData.size());
    }

    @Test
    void testCloseAndReopen() {
        String key = "testKey";
        String value = "testValue";

        rocksDBService.putData(STATE_NAME, Collections.singletonMap(key, value));
        rocksDBService.close(false);

        rocksDBService = new RocksDBService(path, new MapStoreConfig());
        String retrievedValue = rocksDBService.getData(STATE_NAME, key);

        Assertions.assertEquals(value, retrievedValue);
    }

    @Test
    void testPutMultipleData() {
        Map<String, String> dataToPut = new HashMap<>();
        dataToPut.put("key1", "value1");
        dataToPut.put("key2", "value2");
        dataToPut.put("key3", "value3");

        rocksDBService.putData(STATE_NAME, dataToPut);

        for (Map.Entry<String, String> entry : dataToPut.entrySet()) {
            String retrievedValue = rocksDBService.getData(STATE_NAME, entry.getKey());
            Assertions.assertEquals(entry.getValue(), retrievedValue);
        }
    }
}
