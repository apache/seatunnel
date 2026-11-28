/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.lance.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class LanceCommonConfigTest {

    private Map<String, Object> configs(String datasetPath, Map<String, String> storageOptions) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(LanceCommonOptions.KEY_DATASET_PATH.key(), datasetPath);
        configs.put(LanceCommonOptions.KEY_NAMESPACE_TYPE.key(), "dir");
        if (storageOptions != null) {
            configs.put(LanceSinkOptions.WRITE_STORAGE_OPTIONS.key(), storageOptions);
        }
        return configs;
    }

    private Map<String, String> minioStorageOptions() {
        Map<String, String> storageOptions = new HashMap<>();
        storageOptions.put("aws_endpoint_url", "http://127.0.0.1:9000");
        storageOptions.put("aws_access_key_id", "minioadmin");
        storageOptions.put("aws_secret_access_key", "minioadmin");
        storageOptions.put("aws_default_region", "us-east-1");
        return storageOptions;
    }

    @Test
    void readOptionsCarryTheConfiguredStorageOptions() {
        LanceCommonConfig config =
                new LanceCommonConfig(
                        ReadonlyConfig.fromMap(
                                configs("s3://bucket/ds.lance", minioStorageOptions())));

        Assertions.assertEquals(
                config.getStorageOptions(), config.toReadOptions().getStorageOptions());
    }

    @Test
    void writeParamsCarryTheConfiguredStorageOptions() {
        LanceCommonConfig config =
                new LanceCommonConfig(
                        ReadonlyConfig.fromMap(
                                configs("s3://bucket/ds.lance", minioStorageOptions())));

        Assertions.assertEquals(
                config.getStorageOptions(), config.toWriteParams().getStorageOptions());
    }

    @Test
    void omittedStorageOptionsStillBuildUsableOptions() {
        // The option defaults to an empty map, so every open and create stays buildable without
        // storage credentials (local paths keep working unchanged).
        LanceCommonConfig config =
                new LanceCommonConfig(ReadonlyConfig.fromMap(configs("/tmp/ds.lance", null)));

        Assertions.assertTrue(config.getStorageOptions().isEmpty());
        Assertions.assertTrue(config.toReadOptions().getStorageOptions().isEmpty());
        Assertions.assertTrue(config.toWriteParams().getStorageOptions().isEmpty());
    }
}
