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

package org.apache.seatunnel.connectors.seatunnel.hive.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class StorageFactoryTest {

    private static final Map<String, Class<? extends Storage>> STORAGE_MAP =
            new HashMap() {
                {
                    put("hdfs://path/to/", HDFSStorage.class);
                    put("s3n://path/to/", S3Storage.class);
                    put("s3://ws-package/hive/test_hive.db/test_hive_sink_on_s3", S3Storage.class);
                    put("s3a://path/to/", S3Storage.class);
                    put("oss://path/to/", OSSStorage.class);
                    put("cosn://path/to/", COSStorage.class);
                }
            };

    @Test
    void testStorageType() {
        STORAGE_MAP
                .entrySet()
                .forEach(
                        storageMapEntry -> {
                            Class<? extends Storage> expectedStorageClass =
                                    storageMapEntry.getValue();
                            Storage storage =
                                    StorageFactory.getStorageType(storageMapEntry.getKey());
                            Assertions.assertNotNull(storage);
                            Assertions.assertTrue(expectedStorageClass.isInstance(storage));
                        });
    }
}
