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

package org.apache.seatunnel.core.starter.seatunnel.args;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.seatunnel.core.starter.utils.CommandLineUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

public class ClientCommandArgsTest {
    @Test
    public void testUserDefinedParamsCommand() throws URISyntaxException {
        int fakeParallelism = 16;
        String[] args = {
            "-c",
            "/args/user_defined_params.conf",
            "-e",
            "local",
            "-i",
            "fake_source_table=fake",
            "-i",
            "fake_parallelism=16",
            "-i",
            "fake_sink_table=sink",
        };
        ClientCommandArgs clientCommandArgs =
                CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel-zeta", true);
        clientCommandArgs.buildCommand();
        URL resource = ClientCommandArgsTest.class.getResource("/args/user_defined_params.conf");

        Config config =
                ConfigFactory.parseFile(Paths.get(resource.toURI()).toFile())
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        List<? extends ConfigObject> sourceConfig = config.getObjectList("source");
        for (ConfigObject configObject : sourceConfig) {
            String tableName = configObject.toConfig().getString("result_table_name");
            Assertions.assertEquals(tableName, "fake");
            int parallelism = Integer.parseInt(configObject.toConfig().getString("parallelism"));
            Assertions.assertEquals(fakeParallelism, parallelism);
        }
        List<? extends ConfigObject> sinkConfig = config.getObjectList("sink");
        for (ConfigObject sinkObject : sinkConfig) {
            String tableName = sinkObject.toConfig().getString("result_table_name");
            Assertions.assertEquals(tableName, "sink");
        }
    }
}
