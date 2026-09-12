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

import java.io.PrintStream;

public class EdgeAgentDbUsage {

    public static void printUsage(PrintStream out) {
        out.println(
                "SeaTunnel Edge Agent — SQLite operations\n"
                        + "\n"
                        + "Usage: seatunnel-edge-agent.sh db <subcommand> [options]\n"
                        + "       java ... EdgeAgentStarter db <subcommand> [options]\n"
                        + "\n"
                        + "Read-only (safe while agent is running):\n"
                        + "  info              Database paths, file sizes, agent running state\n"
                        + "  wal-summary       Row counts and oldest updated_at per WAL status\n"
                        + "  wal-list          List WAL rows; leftmost id column is row PK for wal-show\n"
                        + "  wal-show          Show one WAL row; requires --id from wal-list\n"
                        + "  positions         List source file positions (--source-id)\n"
                        + "\n"
                        + "Writes (agent must be stopped; requires --yes):\n"
                        + "  wal-purge-dead    Delete DEAD rows\n"
                        + "  wal-retry-dead    Reset DEAD rows to PENDING (may duplicate sends)\n"
                        + "  wal-unstick-sending  Reset SENDING rows to PENDING\n"
                        + "  wal-purge-acked   Delete ACKED rows older than --older-than-ms\n"
                        + "\n"
                        + "SQLite path (first match wins):\n"
                        + "  1. --sqlite-path <path>     (highest priority)\n"
                        + "  2. EDGE_AGENT_SQLITE_PATH\n"
                        + "  3. {edge.agent.home}/data/wal.db   (default; -wal/-shm alongside)\n"
                        + "\n"
                        + "Options:\n"
                        + "  --sqlite-path <path>      Override SQLite database file\n"
                        + "  --status <STATUS>         Filter: PENDING|SENDING|ACKED|DEAD\n"
                        + "  --limit <n>               List limit (default 20)\n"
                        + "  --id <row-id>             WAL row primary key (first column of wal-list)\n"
                        + "  --source-id <id>          Filter positions\n"
                        + "  --older-than-ms <ms>      Cutoff for wal-purge-acked\n"
                        + "  --dry-run                 Print affected row count only\n"
                        + "  --yes                     Confirm write operation\n"
                        + "  -h, --help                Show this message\n"
                        + "\n"
                        + "Environment:\n"
                        + "  EDGE_AGENT_SQLITE_PATH    Override SQLite file path\n"
                        + "  EDGE_AGENT_PID_FILE       PID file for running check (writes)\n"
                        + "  edge.agent.home           Install root (set by start script)\n");
    }
}
