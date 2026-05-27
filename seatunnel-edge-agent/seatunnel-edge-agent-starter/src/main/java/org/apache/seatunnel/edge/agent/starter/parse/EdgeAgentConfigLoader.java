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
import org.apache.seatunnel.edge.agent.starter.config.AgentSchedulerConfig;
import org.apache.seatunnel.edge.agent.starter.config.AgentSectionConfig;
import org.apache.seatunnel.edge.agent.starter.config.QueueConfig;
import org.apache.seatunnel.edge.agent.starter.config.RetryConfig;
import org.apache.seatunnel.edge.agent.starter.yaml.AgentYamlConfig;
import org.apache.seatunnel.edge.agent.starter.yaml.AgentYamlLoader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class EdgeAgentConfigLoader {

    /**
     * Loads {@code agent.yaml}, bridges sections to {@link ReadonlyConfig}, and validates runtime
     * options.
     *
     * <p>Entry point for {@code EdgeAgentRuntime.start()}. Resolves default input type {@code file}
     * and output type {@code console} when omitted.
     *
     * @param agentYamlPath path to the agent YAML file
     * @return immutable resolved snapshot for assembly and runtime
     * @throws Exception if the file is missing, YAML is invalid, or validation fails
     */
    public static EdgeAgentResolvedConfig load(Path agentYamlPath) throws Exception {
        return load(agentYamlPath, Paths.get("").toAbsolutePath());
    }

    public static EdgeAgentResolvedConfig load(Path agentYamlPath, Path installRoot)
            throws Exception {
        Path configPath = Objects.requireNonNull(agentYamlPath, "configPath");
        Path root = Objects.requireNonNull(installRoot, "installRoot").toAbsolutePath().normalize();
        AgentYamlConfig yaml = AgentYamlLoader.load(configPath);
        if (yaml.getInput() == null) {
            throw new IllegalArgumentException("input must be defined.");
        }

        EdgeAgentIdResolver.resolve(yaml, root);

        ReadonlyConfig agentConfig = AgentConfigBridge.agent(yaml.getAgent());
        ReadonlyConfig queueConfig = AgentConfigBridge.queue(yaml.getQueue());
        ReadonlyConfig retryConfig = AgentConfigBridge.retry(yaml.getRetry());
        AgentRuntimeConfig runtimeConfig =
                AgentRuntimeConfig.compose(
                        AgentSectionConfig.from(agentConfig),
                        QueueConfig.from(queueConfig),
                        AgentSchedulerConfig.from(agentConfig),
                        RetryConfig.from(retryConfig));

        ReadonlyConfig rawInputConfig = AgentConfigBridge.input(yaml.getInput());
        String inputType = EdgeAgentTypeResolver.resolveInputType(rawInputConfig);
        ReadonlyConfig inputConfig = EdgeAgentTypeResolver.withInputType(rawInputConfig, inputType);

        ReadonlyConfig rawOutputConfig = AgentConfigBridge.output(yaml.getOutput());
        String outputType = EdgeAgentTypeResolver.resolveOutputType(rawOutputConfig);
        ReadonlyConfig outputConfig =
                EdgeAgentTypeResolver.withOutputType(rawOutputConfig, outputType);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        EdgeAgentFactoryValidation.validateInput(classLoader, inputConfig, inputType);
        EdgeAgentFactoryValidation.validateOutput(classLoader, outputConfig, outputType);

        return new EdgeAgentResolvedConfig(
                yaml, inputConfig, outputConfig, runtimeConfig, inputType, outputType);
    }
}
