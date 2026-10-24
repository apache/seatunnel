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
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.AzureWriter;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.CloudWriter;

import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

public class AzureConfigurationTest {

    private static final String CONTAINER = "wasbs://seatunnel@account.blob.core.windows.net";

    @AfterEach
    void resetSharedBlockSize() {
        // The AZURE configuration is a shared instance; do not leak block.size to other tests.
        FileConfiguration.AZURE.getConfiguration().setBlockSize(1024 * 1024L);
    }

    @Test
    public void testBuildConfiguration() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("azure.container", CONTAINER);
        config.put("fs.azure.account.key.account.blob.core.windows.net", "account-key");
        config.put("block.size", "2097152");
        config.put("fs.oss.accessKeyId", "must-not-leak");

        AzureConfiguration azureConfiguration =
                (AzureConfiguration) FileConfiguration.valueOf("AZURE").getConfiguration();
        Configuration hadoopConf = azureConfiguration.buildConfiguration(config);

        Assertions.assertEquals(CONTAINER, hadoopConf.get(FS_DEFAULT_NAME_KEY));
        Assertions.assertEquals(
                "org.apache.hadoop.fs.azure.NativeAzureFileSystem", hadoopConf.get("fs.wasb.impl"));
        Assertions.assertEquals(
                "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure",
                hadoopConf.get("fs.wasbs.impl"));
        Assertions.assertNull(hadoopConf.get("fs.wasb.impl.disable.cache"));
        Assertions.assertNull(hadoopConf.get("fs.wasbs.impl.disable.cache"));
        Assertions.assertEquals(
                "account-key",
                hadoopConf.get("fs.azure.account.key.account.blob.core.windows.net"));
        Assertions.assertNull(hadoopConf.get("fs.oss.accessKeyId"));
        Assertions.assertEquals(2097152L, azureConfiguration.getBlockSize());
    }

    @Test
    public void testContainerIsRequired() {
        IllegalArgumentException e =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new AzureConfiguration().buildConfiguration(new HashMap<>()));
        Assertions.assertEquals("azure.container is required", e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "wasbs://seatunnel@account.blob.core.windows.net",
                "wasb://seatunnel@storageemulator/"
            })
    public void testWasbContainerUrisAreAccepted(String container) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("azure.container", container);

        Configuration hadoopConf = new AzureConfiguration().buildConfiguration(config);

        Assertions.assertEquals(container, hadoopConf.get(FS_DEFAULT_NAME_KEY));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "abfs://seatunnel@account.dfs.core.windows.net",
                "abfss://seatunnel@account.dfs.core.windows.net"
            })
    public void testAdlsGen2UrisAreRejected(String container) {
        Map<String, String> config = new HashMap<>();
        config.put("azure.container", container);

        IllegalArgumentException e =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new AzureConfiguration().buildConfiguration(config));
        Assertions.assertTrue(e.getMessage().contains("ADLS Gen2"), e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "s3a://seatunnel-imap",
                "wasbs://account.blob.core.windows.net",
                "wasbs://seatunnel@"
            })
    public void testInvalidContainerUrisAreRejected(String container) {
        Map<String, String> config = new HashMap<>();
        config.put("azure.container", container);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new AzureConfiguration().buildConfiguration(config));
    }

    @Test
    public void testWalUsesCloudWriterForAzure() {
        Assertions.assertTrue(DiscoveryWalFileFactory.getWriter("azure") instanceof AzureWriter);
        Assertions.assertTrue(DiscoveryWalFileFactory.getWriter("azure") instanceof CloudWriter);
        Assertions.assertEquals("azure", DiscoveryWalFileFactory.getWriter("azure").identifier());
        Assertions.assertTrue(DiscoveryWalFileFactory.getReader("azure") instanceof DefaultReader);
    }
}
