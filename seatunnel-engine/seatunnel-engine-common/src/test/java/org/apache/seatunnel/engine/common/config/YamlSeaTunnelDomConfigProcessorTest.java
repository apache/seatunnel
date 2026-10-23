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

package org.apache.seatunnel.engine.common.config;

import org.apache.seatunnel.api.metadata.MetadataConfig;

import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link YamlSeaTunnelDomConfigProcessor} focusing on Metadata config parsing. */
public class YamlSeaTunnelDomConfigProcessorTest {

    @Test
    public void testParseMetadataConfigWithNestedProvider() throws Exception {
        String configPath =
                Paths.get(
                                YamlSeaTunnelDomConfigProcessorTest.class
                                        .getResource("/conf/datasource-nested-config.yaml")
                                        .toURI())
                        .toString();

        SeaTunnelConfig config =
                new YamlSeaTunnelConfigBuilder(new FileInputStream(configPath)).build();
        MetadataConfig metadataConfig = config.getEngineConfig().getMetadataConfig();

        // Verify basic settings
        assertTrue(metadataConfig.isEnabled());
        assertEquals("test_kind", metadataConfig.getKind());

        // Verify nested provider properties are parsed correctly
        assertEquals(3, metadataConfig.getProperties().size());
        assertEquals("http://127.0.0.1:8090", metadataConfig.getProperties().get("test_config1"));
        assertEquals("test_metalake", metadataConfig.getProperties().get("test_config2"));
        assertEquals("test", metadataConfig.getProperties().get("test_config3"));
    }
}
