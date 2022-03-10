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

package org.apache.seatunnel.flink.source;

import org.apache.seatunnel.flink.FlinkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class FileSourceTest {

    // *****************************************************************************
    // Just assert not null here, since we don't have the flink-clint in this module
    // So we cannot execute the job, maybe we can add the flink-client into the test
    // scope
    // *****************************************************************************

    @Test
    public void getJsonDate() {
        String configFile = "flink.streaming.json.conf";
        FlinkEnvironment flinkEnvironment = createFlinkStreamEnvironment(configFile);

        FileSource fileSource = createFileSource(configFile, flinkEnvironment);
        DataSet<Row> data = fileSource.getData(flinkEnvironment);
        Assert.assertNotNull(data);
    }

    @Test
    public void getTextData() {
        String configFile = "flink.streaming.text.conf";
        FlinkEnvironment flinkEnvironment = createFlinkStreamEnvironment(configFile);

        FileSource fileSource = createFileSource(configFile, flinkEnvironment);
        DataSet<Row> data = fileSource.getData(flinkEnvironment);
        Assert.assertNotNull(data);
    }

    private FlinkEnvironment createFlinkStreamEnvironment(String configFile) {
        Config rootConfig = getRootConfig(configFile);

        FlinkEnvironment flinkEnvironment = new FlinkEnvironment();
        flinkEnvironment.setConfig(rootConfig);
        flinkEnvironment.prepare(false);
        return flinkEnvironment;
    }

    private FileSource createFileSource(String configFile, FlinkEnvironment flinkEnvironment) {
        Config rootConfig = getRootConfig(configFile);
        Config sourceConfig = rootConfig.getConfigList("source")
                .stream()
                .filter(config -> config.root().get("plugin_name").unwrapped().equals("FileSource"))
                .findAny().get();

        FileSource fileSource = new FileSource();
        fileSource.setConfig(sourceConfig);
        fileSource.prepare(flinkEnvironment);
        return fileSource;
    }

    private Config getRootConfig(String configFile) {
        String configFilePath = System.getProperty("user.dir") + "/src/test/resources/" + configFile;
        return ConfigFactory
                .parseFile(new File(configFilePath))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));
    }
}
