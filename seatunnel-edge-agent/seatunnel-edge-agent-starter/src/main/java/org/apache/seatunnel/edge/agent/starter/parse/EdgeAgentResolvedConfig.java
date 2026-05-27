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

package org.apache.seatunnel.edge.agent.starter.parse;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.edge.agent.starter.config.AgentRuntimeConfig;
import org.apache.seatunnel.edge.agent.starter.yaml.AgentYamlConfig;

import lombok.Getter;

@Getter
public class EdgeAgentResolvedConfig {

    private final AgentYamlConfig yaml;
    private final ReadonlyConfig inputConfig;
    private final ReadonlyConfig outputConfig;
    private final AgentRuntimeConfig runtimeConfig;
    private final String inputType;
    private final String outputType;

    EdgeAgentResolvedConfig(
            AgentYamlConfig yaml,
            ReadonlyConfig inputConfig,
            ReadonlyConfig outputConfig,
            AgentRuntimeConfig runtimeConfig,
            String inputType,
            String outputType) {
        this.yaml = yaml;
        this.inputConfig = inputConfig;
        this.outputConfig = outputConfig;
        this.runtimeConfig = runtimeConfig;
        this.inputType = inputType;
        this.outputType = outputType;
    }

    public String getAgentId() {
        return runtimeConfig.getAgentId();
    }

    public String getInputId() {
        return yaml.getInput() != null ? yaml.getInput().getId() : null;
    }

    public String getOutputId() {
        return yaml.getOutput() != null ? yaml.getOutput().getId() : null;
    }
}
