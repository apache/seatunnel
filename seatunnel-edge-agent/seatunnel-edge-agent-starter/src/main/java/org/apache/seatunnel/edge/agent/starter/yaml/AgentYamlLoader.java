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

package org.apache.seatunnel.edge.agent.starter.yaml;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.PropertyAccessor;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class AgentYamlLoader {

    private static final YAMLMapper YAML =
            YAMLMapper.builder()
                    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                    .visibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                    .visibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE)
                    .visibility(PropertyAccessor.SETTER, JsonAutoDetect.Visibility.NONE)
                    .build();

    /**
     * Reads YAML from {@code yamlPath} and applies {@code normalize}. Does not validate — callers
     * must validate ReadonlyConfig slices after bridge conversion.
     */
    public static AgentYamlConfig load(Path yamlPath) throws IOException {
        if (!Files.isRegularFile(yamlPath)) {
            throw new IOException("Agent config is not a readable file: " + yamlPath);
        }
        AgentYamlConfig cfg = YAML.readValue(yamlPath.toFile(), AgentYamlConfig.class);
        normalize(cfg);
        return cfg;
    }

    /** Normalizes legacy input shapes and applies null-safe defaults for optional sections. */
    public static void normalize(AgentYamlConfig cfg) {
        if (cfg == null) {
            return;
        }
        cfg.ensureDefaults();
        AgentYamlConfig.ReaderDefinition input = cfg.getInput();
        if (input != null) {
            input.normalizeLegacyPath();
        }
    }
}
