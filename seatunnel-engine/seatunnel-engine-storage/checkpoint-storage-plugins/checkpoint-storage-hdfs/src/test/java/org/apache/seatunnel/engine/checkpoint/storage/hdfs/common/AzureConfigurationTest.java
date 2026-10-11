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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

public class AzureConfigurationTest {

    private static final String CONTAINER = "wasbs://seatunnel@account.blob.core.windows.net";

    @Test
    public void testBuildConfiguration() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("azure.container", CONTAINER);
        config.put("fs.azure.account.key.account.blob.core.windows.net", "account-key");
        config.put("fs.azure.sas.seatunnel.account.blob.core.windows.net", "sas-token");
        config.put("fs.gs.auth.service.account.json.keyfile", "must-not-leak");

        Configuration hadoopConf =
                FileConfiguration.valueOf("AZURE").getConfiguration().buildConfiguration(config);

        Assertions.assertEquals(CONTAINER, hadoopConf.get(FS_DEFAULT_NAME_KEY));
        Assertions.assertEquals(
                "org.apache.hadoop.fs.azure.NativeAzureFileSystem", hadoopConf.get("fs.wasb.impl"));
        Assertions.assertEquals(
                "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure",
                hadoopConf.get("fs.wasbs.impl"));
        Assertions.assertTrue(hadoopConf.getBoolean("fs.wasb.impl.disable.cache", false));
        Assertions.assertTrue(hadoopConf.getBoolean("fs.wasbs.impl.disable.cache", false));
        Assertions.assertEquals(
                "account-key",
                hadoopConf.get("fs.azure.account.key.account.blob.core.windows.net"));
        Assertions.assertEquals(
                "sas-token",
                hadoopConf.get("fs.azure.sas.seatunnel.account.blob.core.windows.net"));
        Assertions.assertNull(hadoopConf.get("fs.gs.auth.service.account.json.keyfile"));
    }

    @Test
    public void testDisableCacheCanBeTurnedOff() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("azure.container", CONTAINER);
        config.put("disable.cache", "false");

        Configuration hadoopConf = new AzureConfiguration().buildConfiguration(config);

        Assertions.assertFalse(hadoopConf.getBoolean("fs.wasb.impl.disable.cache", true));
        Assertions.assertFalse(hadoopConf.getBoolean("fs.wasbs.impl.disable.cache", true));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "wasbs://seatunnel@account.blob.core.windows.net",
                "wasbs://seatunnel@account.blob.core.windows.net/",
                "wasb://seatunnel@account.blob.core.windows.net",
                "wasb://seatunnel@storageemulator/"
            })
    public void testWasbContainerUrisAreAccepted(String container) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("azure.container", container);

        Configuration hadoopConf = new AzureConfiguration().buildConfiguration(config);

        Assertions.assertEquals(container, hadoopConf.get(FS_DEFAULT_NAME_KEY));
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
    @ValueSource(strings = {"seatunnel", "s3a://seatunnel", "gs://seatunnel", "wasbs:/seatunnel"})
    public void testNonWasbUrisAreRejected(String container) {
        Map<String, String> config = new HashMap<>();
        config.put("azure.container", container);

        IllegalArgumentException e =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new AzureConfiguration().buildConfiguration(config));
        Assertions.assertTrue(
                e.getMessage().contains("wasbs://<container>@<account>.blob.core.windows.net"),
                e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "wasbs://account.blob.core.windows.net",
                "wasbs://@account.blob.core.windows.net",
                "wasbs://seatunnel@"
            })
    public void testContainerAndAccountAreBothRequired(String container) {
        Map<String, String> config = new HashMap<>();
        config.put("azure.container", container);

        IllegalArgumentException e =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new AzureConfiguration().buildConfiguration(config));
        Assertions.assertTrue(
                e.getMessage().contains("both the container and the storage account"),
                e.getMessage());
    }
}
