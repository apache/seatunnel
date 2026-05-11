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

package org.apache.seatunnel.edge.agent.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Loads {@link AgentYamlConfig} from a YAML file path using Jackson {@link YAMLMapper}.
 *
 * <p>Semantics align with {@code conf/agent.yaml} documentation:
 *
 * <ul>
 *   <li>Unknown YAML keys are ignored ({@link
 *       com.fasterxml.jackson.databind.DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES}
 *       disabled).
 *   <li>After deserialization, {@link AgentYamlConfig#validate(AgentYamlConfig)} enforces required
 *       inputs/output/queue fields and per-input typing constraints ({@code file}/{@code
 *       log}/{@code event}).
 * </ul>
 *
 * <p>{@code output}: {@code cluster-addresses} bootstrap SeaTunnelClient (Hazelcast members);
 * {@code job-id}, {@code auth-token}, and {@code port} configure EdgeSocket batch sends against
 * hosts from {@code getJobTaskGroupAddresses}. Optional {@code connect-timeout-ms}/{@code
 * read-timeout-ms} override socket timeouts when set.
 */
public final class AgentYamlLoader {

    private static final YAMLMapper YAML =
            YAMLMapper.builder().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();

    private AgentYamlLoader() {}

    /**
     * Reads YAML from {@code yamlPath} and applies {@link
     * AgentYamlConfig#validate(AgentYamlConfig)}.
     */
    public static AgentYamlConfig load(Path yamlPath) throws IOException {
        if (!Files.isRegularFile(yamlPath)) {
            throw new IOException("Agent config is not a readable file: " + yamlPath);
        }
        AgentYamlConfig cfg = YAML.readValue(yamlPath.toFile(), AgentYamlConfig.class);
        AgentYamlConfig.validate(cfg);
        return cfg;
    }
}
