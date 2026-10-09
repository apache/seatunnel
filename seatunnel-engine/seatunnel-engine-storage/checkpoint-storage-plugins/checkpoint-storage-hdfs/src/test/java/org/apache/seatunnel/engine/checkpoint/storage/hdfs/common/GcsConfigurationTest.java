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

package org.apache.seatunnel.engine.checkpoint.storage.hdfs.common;

import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

public class GcsConfigurationTest {

    @Test
    public void testBuildConfiguration() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("gcs.bucket", "gs://seatunnel-checkpoint");
        config.put("fs.gs.auth.service.account.json.keyfile", "/path/to/key.json");
        config.put("fs.s3a.access.key", "must-not-leak");

        Configuration hadoopConf =
                FileConfiguration.valueOf("GCS").getConfiguration().buildConfiguration(config);

        Assertions.assertEquals("gs://seatunnel-checkpoint", hadoopConf.get(FS_DEFAULT_NAME_KEY));
        Assertions.assertEquals(
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                hadoopConf.get("fs.gs.impl"));
        Assertions.assertTrue(hadoopConf.getBoolean("fs.gs.impl.disable.cache", false));
        Assertions.assertEquals(
                "/path/to/key.json", hadoopConf.get("fs.gs.auth.service.account.json.keyfile"));
        Assertions.assertNull(hadoopConf.get("fs.s3a.access.key"));
    }

    @Test
    public void testDisableCacheCanBeTurnedOff() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("gcs.bucket", "gs://seatunnel-checkpoint");
        config.put("disable.cache", "false");

        Configuration hadoopConf = new GcsConfiguration().buildConfiguration(config);

        Assertions.assertFalse(hadoopConf.getBoolean("fs.gs.impl.disable.cache", true));
    }

    @Test
    public void testBucketIsRequired() {
        IllegalArgumentException e =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new GcsConfiguration().buildConfiguration(new HashMap<>()));
        Assertions.assertEquals("gcs.bucket is required", e.getMessage());
    }

    @Test
    public void testBucketMustUseGsScheme() {
        Map<String, String> config = new HashMap<>();
        config.put("gcs.bucket", "seatunnel-checkpoint");

        IllegalArgumentException e =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new GcsConfiguration().buildConfiguration(config));
        Assertions.assertTrue(e.getMessage().contains("gs://my-bucket"));
    }
}
