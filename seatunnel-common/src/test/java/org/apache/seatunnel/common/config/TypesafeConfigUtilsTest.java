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

package org.apache.seatunnel.common.config;

import static org.apache.seatunnel.common.config.TypesafeConfigUtils.extractSubConfig;
import static org.apache.seatunnel.common.config.TypesafeConfigUtils.extractSubConfigThrowable;
import static org.apache.seatunnel.common.config.TypesafeConfigUtils.hasSubConfig;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TypesafeConfigUtilsTest {

    @Test
    public void testExtractSubConfig() {
        Config config = getConfig();
        Config subConfig = extractSubConfig(config, "test.", true);
        Map<String, String> configMap = new HashMap<>();
        configMap.put("test.t0", "v0");
        configMap.put("test.t1", "v1");
        Assertions.assertEquals(ConfigFactory.parseMap(configMap), subConfig);

        subConfig = extractSubConfig(config, "test.", false);
        configMap = new HashMap<>();
        configMap.put("t0", "v0");
        configMap.put("t1", "v1");
        Assertions.assertEquals(ConfigFactory.parseMap(configMap), subConfig);
    }

    @Test
    public void testHasSubConfig() {
        Config config = getConfig();
        boolean hasSubConfig = hasSubConfig(config, "test.");
        Assertions.assertTrue(hasSubConfig);

        hasSubConfig = hasSubConfig(config, "test1.");
        Assertions.assertFalse(hasSubConfig);
    }

    @Test
    public void testExtractSubConfigThrowable() {
        Config config = getConfig();

        assertThrows(ConfigRuntimeException.class, () -> extractSubConfigThrowable(config, "test1.", false), "config is empty");

        Config subConfig = extractSubConfigThrowable(config, "test.", false);
        Map<String, String> configMap = new HashMap<>();
        configMap.put("t0", "v0");
        configMap.put("t1", "v1");
        Assertions.assertEquals(ConfigFactory.parseMap(configMap), subConfig);
    }

    public Config getConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("test.t0", "v0");
        configMap.put("test.t1", "v1");
        configMap.put("k0", "v2");
        configMap.put("k1", "v3");
        configMap.put("l1", Long.parseLong("100"));
        return ConfigFactory.parseMap(configMap);
    }

    @Test
    public void testGetConfig() {
        Config config = getConfig();
        Assertions.assertEquals(Long.parseLong("100"), (long) TypesafeConfigUtils.getConfig(config, "l1", Long.parseLong("101")));
        Assertions.assertEquals(Long.parseLong("100"), (long) TypesafeConfigUtils.getConfig(config, "l2", Long.parseLong("100")));
    }
}
