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

package org.apache.seatunnel.e2e.flink.local;

import org.apache.seatunnel.core.starter.Seatunnel;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.command.FlinkCommandBuilder;
import org.apache.seatunnel.core.starter.flink.config.FlinkRunMode;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created 2022/8/30
 */

@Builder
@AllArgsConstructor
public class FlinkLocalContainer {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkLocalContainer.class);

    private List<String> parameters = new ArrayList<>();

    public void executeSeaTunnelFlinkJob(String path) throws CommandException {
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setRunMode(FlinkRunMode.RUN);
        flinkCommandArgs.setConfigFile(getResource(path));
        flinkCommandArgs.setCheckConfig(false);
        flinkCommandArgs.setVariables(parameters);
        Command<FlinkCommandArgs> flinkCommandArgsCommand = new FlinkCommandBuilder()
            .buildCommand(flinkCommandArgs);
        Seatunnel.run(flinkCommandArgsCommand);
    }

    @SneakyThrows
    private String getResource(String confFile) {
        return Paths.get(FlinkLocalContainer.class.getClassLoader().getResource(confFile).toURI())
            .toString();
    }
}
