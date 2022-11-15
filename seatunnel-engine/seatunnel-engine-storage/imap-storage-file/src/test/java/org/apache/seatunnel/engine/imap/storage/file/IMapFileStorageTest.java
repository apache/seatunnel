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

import static org.apache.seatunnel.engine.imap.storage.file.common.FileConstants.FileInitProperties.ARCHIVE_SCHEDULER_TIME_IN_SECONDS_KEY;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

import org.apache.seatunnel.engine.imap.storage.file.common.FileConstants;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@EnabledOnOs({LINUX, MAC})
public class IMapFileStorageTest {

    @Test
    void testAll() throws InterruptedException {
        IMapFileStorage storage = new IMapFileStorage();
        Map<String, Object> properties = new HashMap<>();
        properties.put(FileConstants.FileInitProperties.BUSINESS_KEY, "kris");
        properties.put(FileConstants.FileInitProperties.NAMESPACE_KEY, "file//imap-seatunnel/test/");
        properties.put(FileConstants.FileInitProperties.CLUSTER_NAME, "cluster1");
        Configuration configuration = new Configuration();
        properties.put(FileConstants.FileInitProperties.HDFS_CONFIG_KEY, configuration);
        properties.put(ARCHIVE_SCHEDULER_TIME_IN_SECONDS_KEY, 1000 * 60 * 60L);
        storage.initialize(properties);

        Map<Object, Object> datas = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            String key = "key" + i;
            Long value = System.currentTimeMillis();
            datas.put(key, value);
        }

        storage.storeAll(datas);
        String key900 = "key900";
        String key0 = "key0";
        String key2 = "key2";
        Long keyValue = 123456789L;
        await().atMost(1, TimeUnit.SECONDS).await();
        storage.store(key900, keyValue);
        storage.delete(key2);
        storage.store(key0, keyValue);
        storage.archive();
        Map<Object, Object> loadAllDatas = storage.loadAll();
        Assertions.assertEquals(100, loadAllDatas.size());
        Assertions.assertNotNull(loadAllDatas.get(key900));
        Assertions.assertNull(loadAllDatas.get(key2));
        storage.destroy();
    }
}
