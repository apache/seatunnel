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

import org.apache.seatunnel.engine.imap.storage.file.common.FileConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.seatunnel.engine.imap.storage.file.common.FileConstants.FileInitProperties.WRITE_DATA_TIMEOUT_MILLISECONDS_KEY;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

@EnabledOnOs({LINUX, MAC})
public class IMapFileStorageTest {

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
    void testStoreArray() {
        Long[] data = new Long[10];
        data[6] = 111111111L;
        STORAGE.store("array", data);
        Long[] array = (Long[]) STORAGE.loadAll().get("array");
        Assertions.assertEquals(array[6], 111111111L);
    }

    @AfterAll
    static void afterAll() throws IOException {
        FileSystem.get(CONF).delete(new Path("/tmp/imap-kris-test/2"), true);
    }
}
