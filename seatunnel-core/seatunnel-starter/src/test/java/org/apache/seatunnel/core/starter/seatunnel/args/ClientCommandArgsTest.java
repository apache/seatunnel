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
        String username = "seatunnel=2.3.1";
        String password = "dsjr42=4wfskahdsd=w1chh";
        String fakeSourceTable = "fake";
        String fakeSinkTable = "sink";
        String[] args = {
            "-c",
            "/args/user_defined_params.conf",
            "-e",
            "local",
            "-i",
            "fake_source_table=" + fakeSourceTable,
            "-i",
            "fake_parallelism=" + fakeParallelism,
            "-i",
            "fake_sink_table=" + fakeSinkTable,
            "-i",
            "password=" + password,
            "-i",
            "username=" + username
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
        List<? extends ConfigObject> sourceConfigs = config.getObjectList("source");
        for (ConfigObject configObject : sourceConfigs) {
            Config sourceConfig = configObject.toConfig();

            String tableName = sourceConfig.getString("result_table_name");
            Assertions.assertEquals(tableName, fakeSourceTable);

            int parallelism = Integer.parseInt(sourceConfig.getString("parallelism"));
            Assertions.assertEquals(fakeParallelism, parallelism);

            Assertions.assertEquals(sourceConfig.getString("username"), username);
            Assertions.assertEquals(sourceConfig.getString("password"), password);
        }
        List<? extends ConfigObject> sinkConfigs = config.getObjectList("sink");
        for (ConfigObject sinkObject : sinkConfigs) {
            Config sinkConfig = sinkObject.toConfig();
            String tableName = sinkConfig.getString("result_table_name");
            Assertions.assertEquals(tableName, fakeSinkTable);

            Assertions.assertEquals(sinkConfig.getString("username"), username);
            Assertions.assertEquals(sinkConfig.getString("password"), password);
        }
    }
}
