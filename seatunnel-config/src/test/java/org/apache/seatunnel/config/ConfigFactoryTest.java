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

import org.apache.seatunnel.config.utils.FileUtils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ConfigFactoryTest {

    @Test
    public void testBasicParseAppConf() {

        Config config = ConfigFactory.parseFile(FileUtils.getFileFromResources("factory/config.conf"));

        Assert.assertTrue(config.hasPath("env"));
        Assert.assertTrue(config.hasPath("source"));
        Assert.assertTrue(config.hasPath("transform"));
        Assert.assertTrue(config.hasPath("sink"));

        // check evn config
        Config env = config.getConfig("env");
        Assert.assertEquals("SeaTunnel", env.getString("spark.app.name"));
        Assert.assertEquals("2", env.getString("spark.executor.instances"));
        Assert.assertEquals("1", env.getString("spark.executor.cores"));
        Assert.assertEquals("1g", env.getString("spark.executor.memory"));
        Assert.assertEquals("5", env.getString("spark.streaming.batchDuration"));

        // check custom plugin
        Assert.assertEquals("c.Console", config.getConfigList("sink").get(1).getString("plugin_name"));
    }

    @Test
    public void testTransformOrder() {

        Config config = ConfigFactory.parseFile(FileUtils.getFileFromResources("factory/config.conf"));

        String[] pluginNames = {"split", "sql1", "sql2", "sql3", "json"};

        List<? extends Config> transforms = config.getConfigList("transform");
        Assert.assertEquals(pluginNames.length, transforms.size());

        for (int i = 0; i < transforms.size(); i++) {
            String parsedPluginName = String.valueOf(transforms.get(i).root().get("plugin_name").unwrapped());
            Assert.assertEquals(pluginNames[i], parsedPluginName);
        }

    }

    @Test
    public void testQuotedString() {
        List<String> keys = Arrays.asList("spark.app.name", "spark.executor.instances", "spark.executor.cores",
                "spark.executor.memory", "spark.streaming.batchDuration");

        Config config = ConfigFactory.parseFile(FileUtils.getFileFromResources("factory/config.conf"));
        Config evnConfig = config.getConfig("env");
        evnConfig.entrySet().forEach(entry -> Assert.assertTrue(keys.contains(entry.getKey())));

    }
}
