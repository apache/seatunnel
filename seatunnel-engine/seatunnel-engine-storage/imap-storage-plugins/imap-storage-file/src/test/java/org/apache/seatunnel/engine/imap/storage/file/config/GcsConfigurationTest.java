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

package org.apache.seatunnel.engine.imap.storage.file.config;

import org.apache.seatunnel.engine.imap.storage.file.wal.DiscoveryWalFileFactory;
import org.apache.seatunnel.engine.imap.storage.file.wal.reader.DefaultReader;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.GcsWriter;

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
        config.put("gcs.bucket", "gs://seatunnel-imap");
        config.put("fs.gs.auth.service.account.json.keyfile", "/path/to/key.json");
        config.put("block.size", "2097152");
        config.put("fs.oss.accessKeyId", "must-not-leak");

        GcsConfiguration gcsConfiguration =
                (GcsConfiguration) FileConfiguration.valueOf("GCS").getConfiguration();
        Configuration hadoopConf = gcsConfiguration.buildConfiguration(config);

        Assertions.assertEquals("gs://seatunnel-imap", hadoopConf.get(FS_DEFAULT_NAME_KEY));
        Assertions.assertEquals(
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                hadoopConf.get("fs.gs.impl"));
        Assertions.assertEquals(
                "/path/to/key.json", hadoopConf.get("fs.gs.auth.service.account.json.keyfile"));
        Assertions.assertNull(hadoopConf.get("fs.oss.accessKeyId"));
        Assertions.assertEquals(2097152L, gcsConfiguration.getBlockSize());
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
        config.put("gcs.bucket", "s3a://seatunnel-imap");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new GcsConfiguration().buildConfiguration(config));
    }

    @Test
    public void testWalUsesCloudWriterForGcs() {
        Assertions.assertTrue(DiscoveryWalFileFactory.getWriter("gcs") instanceof GcsWriter);
        Assertions.assertEquals("gcs", DiscoveryWalFileFactory.getWriter("gcs").identifier());
        Assertions.assertTrue(DiscoveryWalFileFactory.getReader("gcs") instanceof DefaultReader);
    }
}
