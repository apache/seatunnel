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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class RocksDBServiceTest {
    private static final String DB_PATH = "/tmp/rocksdb_test";
    private static final String STATE_NAME = "default";
    private RocksDBService rocksDBService;

    @BeforeEach
    void setUp() {
        rocksDBService = new RocksDBService(DB_PATH, new MapStoreConfig());
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

        rocksDBService = new RocksDBService(DB_PATH, new MapStoreConfig());
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
