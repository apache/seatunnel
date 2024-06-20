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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.utils.EnvironmentUtil;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.core.starter.utils.FileUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TestFlinkParameter {

    @Test
    public void testFlinkParameter() throws Exception {
        // Verified Map
        List<String> checkList = new ArrayList<>();
        checkList.add("execution.checkpointing.interval=5000");
        checkList.add("execution.checkpointing.unaligned.enabled=true");
        checkList.add("execution.checkpointing.aligned-checkpoint-timeout=100000");
        checkList.add("jobstore.cache-size=52428801");
        checkList.add("state.backend.rocksdb.predefined-options=SPINNING_DISK_OPTIMIZED_HIGH_MEM");
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setDeployMode(DeployMode.RUN);
        flinkCommandArgs.setJobName("SeaTunnelFlinkParameter");
        flinkCommandArgs.setEncrypt(false);
        flinkCommandArgs.setDecrypt(false);
        flinkCommandArgs.setHelp(false);
        flinkCommandArgs.setConfigFile("src/test/java/resources/test_flink_run_parameter.conf");
        flinkCommandArgs.setVariables(null);
        Path configFile = FileUtils.getConfigPath(flinkCommandArgs);
        Config config = ConfigBuilder.of(configFile).getConfig("env");

        // set Flink Configuration
        Configuration configurations = new Configuration();
        EnvironmentUtil.initConfiguration(config, configurations);
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment(configurations);
        List<String> ExternalSettingLists = new ArrayList<>();
        // Replace excess conceits for easy validation of parameters
        String[] split =
                executionEnvironment
                        .getConfiguration()
                        .toString()
                        .replaceAll(" ", "")
                        .replaceAll("\\{", "")
                        .replaceAll("\\}", "")
                        .replaceAll("\"", "")
                        .trim()
                        .split(",");
        for (String value : split) {
            if (checkList.contains(value)) {
                ExternalSettingLists.add(value);
            }
        }
        // Sort keeping order
        checkList.sort(null);
        ExternalSettingLists.sort(null);
        Assertions.assertIterableEquals(checkList, ExternalSettingLists);
    }
}
