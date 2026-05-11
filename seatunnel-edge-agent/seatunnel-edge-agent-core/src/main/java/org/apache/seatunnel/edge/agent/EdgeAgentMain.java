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

package org.apache.seatunnel.edge.agent;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** JVM entry point for the SeaTunnel Edge Agent process. */
public final class EdgeAgentMain {

    private EdgeAgentMain() {}

    public static void main(String[] args) {
        try {
            Path config = resolveConfigPath(args);
            EdgeAgentBootstrap.start(config);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    /**
     * Resolves {@code agent.yaml}: explicit CLI ({@code --config}/{@code -c} or first positional
     * arg), {@code EDGE_AGENT_CONFIG}, then {@code ./conf/agent.yaml}, then {@code ./agent.yaml}.
     */
    static Path resolveConfigPath(String[] args) throws IOException {
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                if ("--help".equals(args[i]) || "-h".equals(args[i])) {
                    printHelp();
                    System.exit(0);
                }
                if ("--config".equals(args[i]) || "-c".equals(args[i])) {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException(
                                "Missing path after "
                                        + args[i]
                                        + "; expected --config /path/to/agent.yaml");
                    }
                    Path p = Paths.get(args[i + 1]).toAbsolutePath().normalize();
                    if (!Files.isRegularFile(p)) {
                        throw new FileNotFoundException("Agent config file not found: " + p);
                    }
                    return p;
                }
            }
            if (args.length == 1 && !args[0].startsWith("-")) {
                Path p = Paths.get(args[0]).toAbsolutePath().normalize();
                if (!Files.isRegularFile(p)) {
                    throw new FileNotFoundException("Agent config file not found: " + p);
                }
                return p;
            }
        }

        String env = System.getenv("EDGE_AGENT_CONFIG");
        if (env != null && !env.trim().isEmpty()) {
            Path p = Paths.get(env.trim()).toAbsolutePath().normalize();
            if (!Files.isRegularFile(p)) {
                throw new FileNotFoundException("EDGE_AGENT_CONFIG points to missing file: " + p);
            }
            return p;
        }

        Path cwd = Paths.get("").toAbsolutePath();
        String[] candidates = new String[] {"conf/agent.yaml", "agent.yaml"};
        for (String candidate : candidates) {
            Path p = cwd.resolve(candidate).normalize();
            if (Files.isRegularFile(p)) {
                return p;
            }
        }
        throw new FileNotFoundException(
                "No agent.yaml found. Use --config /path/to/agent.yaml, set EDGE_AGENT_CONFIG, "
                        + "or place conf/agent.yaml under the working directory.");
    }

    private static void printHelp() {
        System.out.println(
                "SeaTunnel Edge Agent\n"
                        + "  --config, -c <path>   Path to agent.yaml\n"
                        + "  <path>                Shorthand for config path\n"
                        + "  EDGE_AGENT_CONFIG     Env override for config path\n"
                        + "  Default search: ./conf/agent.yaml, ./agent.yaml\n"
                        + "\n"
                        + "agent.yaml output (SeaTunnel EdgeSocket only):\n"
                        + "  cluster-name          Hazelcast cluster name (default: seatunnel)\n"
                        + "  cluster-addresses     YAML list of ingress hostnames or IPs\n"
                        + "  job-id                Running SeaTunnel job id (positive integer)\n"
                        + "  auth-token            Token matching EdgeSocket source on the engine\n"
                        + "  port                  TCP port for EdgeSocket ingress on each host\n"
                        + "  Optional: connect-timeout-ms, read-timeout-ms\n");
    }
}
