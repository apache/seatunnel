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
import org.apache.seatunnel.edge.agent.starter.yaml.AgentYamlLoader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class EdgeAgentIdResolverTest {

    @TempDir Path tempDir;

    @Test
    void persistWritesThreeKeyValueLinesWithoutCommentHeader() throws Exception {
        String yaml =
                "input:\n" + "  paths: [\"/tmp/a.log\"]\n" + "output:\n" + "  type: console\n";
        Path yamlPath = tempDir.resolve("agent.yaml");
        Files.write(yamlPath, yaml.getBytes(StandardCharsets.UTF_8));
        AgentYamlConfig config = AgentYamlLoader.load(yamlPath);

        EdgeAgentIdResolver.resolve(config, tempDir);

        Path idFile = tempDir.resolve(EdgeAgentIdResolver.ID_FILE_NAME);
        String content = new String(Files.readAllBytes(idFile), StandardCharsets.UTF_8);
        Assertions.assertFalse(content.startsWith("#"));
        Assertions.assertEquals(
                EdgeAgentIdResolver.KEY_AGENT_ID
                        + '='
                        + config.getAgent().getId()
                        + '\n'
                        + EdgeAgentIdResolver.KEY_INPUT_ID
                        + '='
                        + config.getInput().getId()
                        + '\n'
                        + EdgeAgentIdResolver.KEY_OUTPUT_ID
                        + '='
                        + config.getOutput().getId()
                        + '\n',
                content);
    }
}
