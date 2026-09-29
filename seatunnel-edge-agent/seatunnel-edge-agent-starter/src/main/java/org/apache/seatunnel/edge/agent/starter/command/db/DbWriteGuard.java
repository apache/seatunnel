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

package org.apache.seatunnel.edge.agent.starter.command.db;

import org.apache.seatunnel.edge.agent.starter.command.EdgeAgentPaths;

import java.io.IOException;

public class DbWriteGuard {

    public static void requireWriteAllowed(EdgeAgentPaths paths, DbCommandArgs cli)
            throws IOException {
        if (!cli.isDryRun() && paths.agentRunning()) {
            throw new IllegalStateException(
                    "Agent is running (pid file "
                            + paths.getPidFile()
                            + "); stop the agent before write operations.");
        }
        if (!cli.isDryRun() && !cli.isYes()) {
            throw new IllegalArgumentException("Write operations require --yes (or use --dry-run)");
        }
    }
}
