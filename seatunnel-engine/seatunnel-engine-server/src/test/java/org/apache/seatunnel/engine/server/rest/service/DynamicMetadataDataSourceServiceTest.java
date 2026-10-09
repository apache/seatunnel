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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.api.metadata.MetadataConfig;
import org.apache.seatunnel.api.metadata.MetadataOptions;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.YamlSeaTunnelConfigBuilder;

import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DynamicMetadataDataSourceServiceTest {

    @Test
    public void testCustomSensitiveKeysFromMetadataProperties() {
        MetadataConfig metadataConfig = new MetadataConfig();
        metadataConfig
                .getProperties()
                .put(MetadataOptions.SENSITIVE_KEYS.key(), "token, access_key");
        DynamicMetadataDataSourceService service =
                new DynamicMetadataDataSourceService(null, metadataConfig);

        assertTrue(service.isSensitiveKey("token"));
        assertTrue(service.isSensitiveKey("oss_access_key_id"));
        assertFalse(service.isSensitiveKey("password"));
    }

    /** Loads YAML config and verifies multiple sensitive key formats. */
    @Test
    public void testSensitiveKeysFromYamlConfig() throws Exception {
        String configPath =
                Paths.get(
                                DynamicMetadataDataSourceServiceTest.class
                                        .getResource("/dynamic-metadata-sensitive-keys.yaml")
                                        .toURI())
                        .toString();

        SeaTunnelConfig config =
                new YamlSeaTunnelConfigBuilder(new FileInputStream(configPath)).build();
        MetadataConfig metadataConfig = config.getEngineConfig().getMetadataConfig();
        DynamicMetadataDataSourceService service =
                new DynamicMetadataDataSourceService(null, metadataConfig);

        assertTrue(service.isSensitiveKey("password"));
        assertTrue(service.isSensitiveKey("access_token"));
        assertTrue(service.isSensitiveKey("username"));
        assertFalse(service.isSensitiveKey("url"));
    }
}
