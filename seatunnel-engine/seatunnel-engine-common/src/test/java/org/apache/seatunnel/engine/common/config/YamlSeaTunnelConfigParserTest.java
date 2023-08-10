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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.YamlClientConfigBuilder;

import java.io.IOException;

public class YamlSeaTunnelConfigParserTest {

    @Test
    public void testSeaTunnelConfig() {
        YamlSeaTunnelConfigLocator yamlConfigLocator = new YamlSeaTunnelConfigLocator();
        SeaTunnelConfig config;
        if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 2. Try loading YAML config from the working directory or from the classpath
            config = new YamlSeaTunnelConfigBuilder(yamlConfigLocator).setProperties(null).build();
        } else {
            throw new RuntimeException("can't find yaml in resources");
        }
        Assertions.assertNotNull(config);

        Assertions.assertEquals(1, config.getEngineConfig().getBackupCount());

        Assertions.assertEquals(2, config.getEngineConfig().getPrintExecutionInfoInterval());

        Assertions.assertFalse(config.getEngineConfig().getSlotServiceConfig().isDynamicSlot());

        Assertions.assertEquals(5, config.getEngineConfig().getSlotServiceConfig().getSlotNum());

        Assertions.assertEquals(
                6000, config.getEngineConfig().getCheckpointConfig().getCheckpointInterval());

        Assertions.assertEquals(
                7000, config.getEngineConfig().getCheckpointConfig().getCheckpointTimeout());

        Assertions.assertEquals(
                "hdfs", config.getEngineConfig().getCheckpointConfig().getStorage().getStorage());

        Assertions.assertEquals(
                3,
                config.getEngineConfig()
                        .getCheckpointConfig()
                        .getStorage()
                        .getMaxRetainedCheckpoints());
        Assertions.assertEquals(
                "file:///",
                config.getEngineConfig()
                        .getCheckpointConfig()
                        .getStorage()
                        .getStoragePluginConfig()
                        .get("fs.defaultFS"));
    }

    @Test
    public void testCustomizeClientConfig() throws IOException {
        YamlClientConfigBuilder yamlClientConfigBuilder =
                new YamlClientConfigBuilder("custmoize-client.yaml");
        ClientConfig clientConfig = yamlClientConfigBuilder.build();

        Assertions.assertEquals("custmoize", clientConfig.getClusterName());
    }
}
