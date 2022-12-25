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

package org.apache.seatunnel.core.starter.flink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlinkStarterTest {

    @Test
    public void testConfigOption() {
        String[] args = {"--config", "test.conf"};
        FlinkStarter flinkStarter = new FlinkStarter(args);
        String flinkExecuteCommand = String.join(" ", flinkStarter.buildCommands());
        Assertions.assertTrue(flinkExecuteCommand.contains("--config test.conf"));
    }

    @Test
    public void testSubmitMasterOption() {
        String[] args = {"--config", "test.conf", "-n", "test-flink-job", "--master", "yarn-session"};
        FlinkStarter flinkStarter = new FlinkStarter(args);
        String flinkExecuteCommand = String.join(" ", flinkStarter.buildCommands());
        Assertions.assertTrue(flinkExecuteCommand.contains("--target yarn-session"));
    }

    @Test
    public void testDeployModeOption() {
        String[] args = {"--config", "test.conf", "-n", "test-flink-job", "--master", "yarn-per-job", "-e", "run-application"};
        FlinkStarter flinkStarter = new FlinkStarter(args);
        String flinkExecuteCommand = String.join(" ", flinkStarter.buildCommands());
        Assertions.assertTrue(flinkExecuteCommand.contains("--target yarn-per-job"));
        Assertions.assertTrue(flinkExecuteCommand.contains("flink run-application"));
    }

    @Test
    public void testExtraParametersOption() {
        String[] args = {"--config", "test.conf", "-n", "test-flink-job", "--master", "yarn-per-job", "-m", "192.168.1.1:8080"};
        FlinkStarter flinkStarter = new FlinkStarter(args);
        String flinkExecuteCommand = String.join(" ", flinkStarter.buildCommands());
        Assertions.assertTrue(flinkExecuteCommand.contains("--target yarn-per-job"));
        Assertions.assertTrue(flinkExecuteCommand.contains("-m 192.168.1.1:8080"));
    }

    @Test
    public void testExtraVariablesOption() {
        String[] args = {"--config", "test.conf", "-n", "test-flink-job", "-i", "name=tyrantlucifer", "-i", "age=26"};
        FlinkStarter flinkStarter = new FlinkStarter(args);
        String flinkExecuteCommand = String.join(" ", flinkStarter.buildCommands());
        Assertions.assertTrue(flinkExecuteCommand.contains("-Dname=tyrantlucifer"));
        Assertions.assertTrue(flinkExecuteCommand.contains("-Dage=26"));
    }
}
