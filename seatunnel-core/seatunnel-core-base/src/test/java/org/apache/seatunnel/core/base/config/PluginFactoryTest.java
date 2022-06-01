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

package org.apache.seatunnel.core.base.config;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;
import org.apache.seatunnel.spark.SparkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.Objects;

public class PluginFactoryTest {

    @Test
    public void getPluginMappingValueTest() throws Exception {

        Common.setDeployMode("cluster");
        Config config = new ConfigBuilder<>(Paths.get(Objects.requireNonNull(PluginFactoryTest.class.getResource("/flink.batch" +
                ".conf")).getPath()), EngineType.SPARK).getConfig();

        Config pluginMapping = ConfigFactory
                .parseFile(new File(Objects.requireNonNull(PluginFactoryTest.class.getResource("/plugin-mapping.properties")).getPath()))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));

        PluginFactory<SparkEnvironment> factory = new PluginFactory<>(config, EngineType.SPARK);

        Method method = factory.getClass().getDeclaredMethod("getPluginMappingValue", Config.class,
                PluginType.class, String.class);
        method.setAccessible(true);

        String jarPrefix = method.invoke(factory, pluginMapping, PluginType.SOURCE, "fake").toString();
        Assert.assertEquals(jarPrefix, "seatunnel-connector-spark-fake");

        String jarPrefix2 = method.invoke(factory, pluginMapping, PluginType.SINK, "console").toString();
        Assert.assertEquals(jarPrefix2, "seatunnel-connector-spark-console");

        String jarPrefix3 = method.invoke(factory, pluginMapping, PluginType.SOURCE, "FaKE").toString();
        Assert.assertEquals(jarPrefix3, "seatunnel-connector-spark-fake");

        String jarPrefix4 = method.invoke(factory, pluginMapping, PluginType.SINK, "HbASe").toString();
        Assert.assertEquals(jarPrefix4, "seatunnel-connector-spark-hbase");
    }

}
