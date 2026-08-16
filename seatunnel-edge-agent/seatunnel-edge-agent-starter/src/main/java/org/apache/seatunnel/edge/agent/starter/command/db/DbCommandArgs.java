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

import org.apache.seatunnel.edge.agent.starter.command.EdgeAgentCommand;
import org.apache.seatunnel.edge.agent.starter.command.EdgeAgentCommandArgs;
import org.apache.seatunnel.edge.agent.starter.command.EdgeAgentStarterConstants;

import com.beust.jcommander.Parameter;
import lombok.Getter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@Getter
public class DbCommandArgs extends EdgeAgentCommandArgs {

    @Parameter(description = "Subcommand name")
    private List<String> commands;

    @Parameter(
            names = {"--sqlite-path"},
            description = "SQLite database file (highest priority)")
    private String sqlitePath;

    @Parameter(
            names = {"--yes"},
            description = "Confirm write operation")
    private boolean yes = false;

    @Parameter(
            names = {"--dry-run"},
            description = "Print affected row count only")
    private boolean dryRun = false;

    @Parameter(
            names = {"--status"},
            description = "WAL status filter: PENDING|SENDING|ACKED|DEAD")
    private String status;

    @Parameter(
            names = {"--limit"},
            description = "List limit (default 20)")
    private int limit = 20;

    @Parameter(
            names = {"--id"},
            description = "WAL row primary key from wal-list (not source-id or batch_id)")
    private Long walId;

    @Parameter(
            names = {"--source-id"},
            description = "Filter source positions")
    private String sourceId;

    @Parameter(
            names = {"--older-than-ms"},
            description = "Cutoff age in ms for wal-purge-acked")
    private long olderThanMs = 0L;

    @Override
    public EdgeAgentCommand<?> buildCommand() {
        if (getSubcommand() == null || "help".equals(getSubcommand())) {
            return new EdgeAgentDbHelpCommand();
        }
        DbSubcommand subcommand = DbSubcommand.fromCliName(getSubcommand());
        if (subcommand == null) {
            throw new IllegalArgumentException("Unknown db subcommand: " + getSubcommand());
        }
        if (subcommand == DbSubcommand.WAL_SHOW && walId == null) {
            throw new IllegalArgumentException(
                    "wal-show requires --id <row-id> (run db wal-list; use the id column)");
        }
        return new EdgeAgentDbCommand(this, subcommand);
    }

    public String getSubcommand() {
        if (commands == null || commands.isEmpty()) {
            return null;
        }
        return commands.get(0);
    }

    public Path getSqlitePathOverride() {
        if (sqlitePath == null || sqlitePath.trim().isEmpty()) {
            return null;
        }
        return Paths.get(sqlitePath.trim());
    }

    private static final class EdgeAgentDbHelpCommand implements EdgeAgentCommand<DbCommandArgs> {

        @Override
        public void execute() {
            EdgeAgentDbUsage.printUsage(System.out);
            System.exit(EdgeAgentStarterConstants.USAGE_EXIT_CODE);
        }
    }
}
