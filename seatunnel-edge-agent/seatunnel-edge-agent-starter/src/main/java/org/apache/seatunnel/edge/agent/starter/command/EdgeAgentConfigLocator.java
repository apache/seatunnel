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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class EdgeAgentConfigLocator {

    private static final Logger LOG = LoggerFactory.getLogger(EdgeAgentConfigLocator.class);

    public static Path resolve(Path explicitConfigPath) throws IOException {
        if (explicitConfigPath != null) {
            return requireRegularFile(explicitConfigPath);
        }

        String env = System.getenv(EdgeAgentEnvConstants.ENV_EDGE_AGENT_CONFIG);
        if (env != null && !env.trim().isEmpty()) {
            return requireRegularFile(Paths.get(env.trim()));
        }

        Path cwd = Paths.get("").toAbsolutePath();
        for (String candidate : new String[] {"config/agent.yaml", "agent.yaml"}) {
            Path path = cwd.resolve(candidate).normalize();
            if (Files.isRegularFile(path)) {
                LOG.debug("Using agent config: {}", path);
                return path;
            }
        }
        throw new FileNotFoundException(
                "No agent.yaml found. Use --config /path/to/agent.yaml, set "
                        + EdgeAgentEnvConstants.ENV_EDGE_AGENT_CONFIG
                        + ", or place config/agent.yaml under the working directory.");
    }

    private static Path requireRegularFile(Path path) throws FileNotFoundException {
        Path normalized = path.toAbsolutePath().normalize();
        if (!Files.isRegularFile(normalized)) {
            throw new FileNotFoundException("Agent config file not found: " + normalized);
        }
        LOG.debug("Using agent config: {}", normalized);
        return normalized;
    }
}
