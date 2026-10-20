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

import org.apache.seatunnel.edge.agent.starter.command.db.EdgeAgentProcessProbe;

import lombok.Getter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@Getter
public class EdgeAgentPaths {

    private final Path installRoot;
    private final Path sqlitePath;
    private final Path pidFile;

    public EdgeAgentPaths(Path installRoot, Path sqlitePath, Path pidFile) {
        this.installRoot = installRoot;
        this.sqlitePath = sqlitePath;
        this.pidFile = pidFile;
    }

    public static EdgeAgentPaths forDb(Path cliSqliteOverride) {
        Path root = installRoot();
        Path sqlite = resolveSqlitePath(cliSqliteOverride);
        Path pid = resolvePidFile(root);
        return new EdgeAgentPaths(root, sqlite, pid);
    }

    public static Path installRoot() {
        String home = System.getProperty(EdgeAgentEnvConstants.PROP_AGENT_HOME);
        if (home != null && !home.trim().isEmpty()) {
            return Paths.get(home.trim()).toAbsolutePath().normalize();
        }
        return Paths.get("").toAbsolutePath().normalize();
    }

    public static Path resolveSqlitePath(Path cliOverride) {
        Path root = installRoot();
        if (cliOverride != null) {
            return resolveUnderRoot(root, cliOverride);
        }
        String env = System.getenv(EdgeAgentEnvConstants.ENV_EDGE_AGENT_SQLITE_PATH);
        if (env != null && !env.trim().isEmpty()) {
            return resolveUnderRoot(root, Paths.get(env.trim()));
        }
        return root.resolve(EdgeAgentEnvConstants.DEFAULT_SQLITE_RELATIVE_PATH).normalize();
    }

    public static Path resolvePidFile(Path installRoot) {
        String env = System.getenv(EdgeAgentEnvConstants.ENV_EDGE_AGENT_PID_FILE);
        if (env != null && !env.trim().isEmpty()) {
            Path path = Paths.get(env.trim());
            return path.isAbsolute() ? path.normalize() : installRoot.resolve(path).normalize();
        }
        return installRoot.resolve(EdgeAgentEnvConstants.DEFAULT_PID_FILE_NAME).normalize();
    }

    private static Path resolveUnderRoot(Path installRoot, Path path) {
        if (path.isAbsolute()) {
            return path.normalize();
        }
        return installRoot.resolve(path).normalize();
    }

    public boolean agentRunning() throws IOException {
        return EdgeAgentProcessProbe.isAgentRunning(pidFile);
    }
}
