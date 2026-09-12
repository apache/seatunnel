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

package org.apache.seatunnel.edge.agent.starter.command;

import com.beust.jcommander.Parameter;
import lombok.Getter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@Getter
public class RunCommandArgs extends EdgeAgentCommandArgs {

    @Parameter(
            names = {"-c", "--config"},
            description = "Path to agent.yaml")
    private String configFile;

    @Parameter(description = "Path to agent.yaml (shorthand for --config)")
    private List<String> positionalConfig;

    @Override
    public EdgeAgentCommand<?> buildCommand() {
        return new RunAgentCommand(this);
    }

    public Path getExplicitConfigPath() {
        if (configFile != null && !configFile.trim().isEmpty()) {
            return Paths.get(configFile.trim());
        }
        if (positionalConfig != null && !positionalConfig.isEmpty()) {
            return Paths.get(positionalConfig.get(0));
        }
        return null;
    }
}
