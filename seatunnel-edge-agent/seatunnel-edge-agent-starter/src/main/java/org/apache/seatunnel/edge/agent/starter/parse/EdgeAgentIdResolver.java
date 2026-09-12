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

import org.apache.seatunnel.edge.agent.starter.yaml.AgentYamlConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

public class EdgeAgentIdResolver {

    public static final String ID_FILE_NAME = "edge-agent.id";
    static final String KEY_AGENT_ID = "agent.id";
    static final String KEY_INPUT_ID = "input.id";
    static final String KEY_OUTPUT_ID = "output.id";

    public static void resolve(AgentYamlConfig yaml, Path installRoot) throws IOException {
        Objects.requireNonNull(yaml, "yaml");
        Objects.requireNonNull(installRoot, "installRoot");
        if (yaml.getInput() == null) {
            throw new IllegalArgumentException("input must be defined.");
        }

        Path idFilePath = resolveIdFilePath(installRoot);
        Properties properties = loadOrCreate(idFilePath);

        resolveAgentId(yaml, properties);
        resolveInputId(yaml, properties);
        resolveOutputId(yaml, properties);

        syncResolvedIdsToProperties(yaml, properties);
        persist(idFilePath, properties);
    }

    private static void resolveAgentId(AgentYamlConfig yaml, Properties properties) {
        AgentYamlConfig.AgentSection agent = yaml.getAgent();
        if (agent == null) {
            yaml.setAgent(new AgentYamlConfig.AgentSection());
            agent = yaml.getAgent();
        }
        if (hasText(agent.getId())) {
            return;
        }
        agent.setId(resolveId(properties, KEY_AGENT_ID));
    }

    private static void resolveInputId(AgentYamlConfig yaml, Properties properties) {
        AgentYamlConfig.ReaderDefinition input = yaml.getInput();
        if (hasText(input.getId())) {
            return;
        }
        input.setId(resolveId(properties, KEY_INPUT_ID));
    }

    private static void resolveOutputId(AgentYamlConfig yaml, Properties properties) {
        AgentYamlConfig.OutputDefinition output = yaml.getOutput();
        if (hasText(output.getId())) {
            return;
        }
        output.setId(resolveId(properties, KEY_OUTPUT_ID));
    }

    private static void syncResolvedIdsToProperties(AgentYamlConfig yaml, Properties properties) {
        properties.setProperty(KEY_AGENT_ID, yaml.getAgent().getId());
        properties.setProperty(KEY_INPUT_ID, yaml.getInput().getId());
        properties.setProperty(KEY_OUTPUT_ID, yaml.getOutput().getId());
    }

    private static String resolveId(Properties properties, String key) {
        String existing = properties.getProperty(key);
        if (hasText(existing)) {
            return existing.trim();
        }
        String generated = UUID.randomUUID().toString();
        properties.setProperty(key, generated);
        return generated;
    }

    static Path resolveIdFilePath(Path installRoot) {
        return installRoot.toAbsolutePath().normalize().resolve(ID_FILE_NAME);
    }

    private static Properties loadOrCreate(Path idFilePath) throws IOException {
        Properties properties = new Properties();
        if (Files.isRegularFile(idFilePath)) {
            try (InputStream in = Files.newInputStream(idFilePath)) {
                properties.load(in);
            }
        }
        return properties;
    }

    private static void persist(Path idFilePath, Properties properties) throws IOException {
        Path parent = idFilePath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        String content =
                KEY_AGENT_ID
                        + '='
                        + requireProperty(properties, KEY_AGENT_ID)
                        + '\n'
                        + KEY_INPUT_ID
                        + '='
                        + requireProperty(properties, KEY_INPUT_ID)
                        + '\n'
                        + KEY_OUTPUT_ID
                        + '='
                        + requireProperty(properties, KEY_OUTPUT_ID)
                        + '\n';
        Files.write(idFilePath, content.getBytes(StandardCharsets.UTF_8));
    }

    private static String requireProperty(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (!hasText(value)) {
            throw new IllegalStateException("Missing " + key + " after identity resolution.");
        }
        return value.trim();
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
