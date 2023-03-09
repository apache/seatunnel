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

package org.apache.seatunnel.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.config.utils.FileUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

public class ConfigFactoryTest {

    @Test
    public void testBasicParseAppConf() throws URISyntaxException {

        Config config =
                ConfigFactory.parseFile(FileUtils.getFileFromResources("/factory/config.conf"));

        Assertions.assertTrue(config.hasPath("env"));
        Assertions.assertTrue(config.hasPath("source"));
        Assertions.assertTrue(config.hasPath("transform"));
        Assertions.assertTrue(config.hasPath("sink"));

        // check evn config
        Config env = config.getConfig("env");
        Assertions.assertEquals("SeaTunnel", env.getString("spark.app.name"));
        Assertions.assertEquals("2", env.getString("spark.executor.instances"));
        Assertions.assertEquals("1", env.getString("spark.executor.cores"));
        Assertions.assertEquals("1g", env.getString("spark.executor.memory"));
        Assertions.assertEquals("5", env.getString("spark.stream.batchDuration"));

        // check custom plugin
        Assertions.assertEquals(
                "c.Console", config.getConfigList("sink").get(1).getString("plugin_name"));
    }

    @Test
    public void testTransformOrder() throws URISyntaxException {

        Config config =
                ConfigFactory.parseFile(FileUtils.getFileFromResources("/factory/config.conf"));

        String[] pluginNames = {"split", "sql1", "sql2", "sql3", "json"};

        List<? extends Config> transforms = config.getConfigList("transform");
        Assertions.assertEquals(pluginNames.length, transforms.size());

        for (int i = 0; i < transforms.size(); i++) {
            String parsedPluginName =
                    String.valueOf(transforms.get(i).root().get("plugin_name").unwrapped());
            Assertions.assertEquals(pluginNames[i], parsedPluginName);
        }
    }

    @Test
    public void testQuotedString() throws URISyntaxException {
        List<String> keys =
                Arrays.asList(
                        "spark.app.name",
                        "spark.executor.instances",
                        "spark.executor.cores",
                        "spark.executor.memory",
                        "spark.stream.batchDuration");

        Config config =
                ConfigFactory.parseFile(FileUtils.getFileFromResources("/factory/config.conf"));
        Config evnConfig = config.getConfig("env");
        evnConfig.entrySet().forEach(entry -> Assertions.assertTrue(keys.contains(entry.getKey())));
    }
}
