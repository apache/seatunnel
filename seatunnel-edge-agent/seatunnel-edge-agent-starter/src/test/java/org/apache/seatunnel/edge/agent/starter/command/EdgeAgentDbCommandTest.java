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

import org.apache.seatunnel.edge.agent.connector.EdgeEvent;
import org.apache.seatunnel.edge.agent.starter.command.db.DbCommandArgs;
import org.apache.seatunnel.edge.agent.starter.command.db.EdgeAgentDbCommand;
import org.apache.seatunnel.edge.agent.starter.wal.WalRecordStatus;
import org.apache.seatunnel.edge.agent.starter.wal.sqlite.SqliteWalStore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

public class EdgeAgentDbCommandTest {

    @TempDir Path tempDir;

    @AfterEach
    void clearProperties() {
        System.clearProperty(EdgeAgentEnvConstants.PROP_AGENT_HOME);
    }

    private static String[] rawDbArgs(String... subcommandAndOptions) {
        String[] raw = new String[subcommandAndOptions.length + 1];
        raw[0] = "db";
        System.arraycopy(subcommandAndOptions, 0, raw, 1, subcommandAndOptions.length);
        return raw;
    }

    private static EdgeAgentCommand<?> buildDb(String... subcommandAndOptions) {
        String[] raw = rawDbArgs(subcommandAndOptions);
        return EdgeAgentTopLevelCommand.resolve(raw).buildCommand(raw);
    }

    private static DbCommandArgs parseDb(String... subcommandAndOptions) {
        return EdgeAgentCommandLineUtils.parse(
                subcommandAndOptions,
                new DbCommandArgs(),
                EdgeAgentStarterConstants.PROGRAM_NAME_DB);
    }

    private static String captureStdout(Runnable action) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(out));
        try {
            action.run();
        } finally {
            System.setOut(original);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    @Nested
    class DbArgsParsing {

        @Test
        void parseWalListOptions() {
            DbCommandArgs args = parseDb("wal-list", "--limit", "5", "--sqlite-path", "/x");
            Assertions.assertEquals("wal-list", args.getSubcommand());
            Assertions.assertEquals(5, args.getLimit());
            Assertions.assertEquals(Paths.get("/x"), args.getSqlitePathOverride());
        }

        @Test
        void buildHelpSubcommand() {
            Assertions.assertFalse(buildDb("help") instanceof EdgeAgentDbCommand);
        }

        @Test
        void buildWithoutSubcommandShowsHelp() {
            Assertions.assertFalse(buildDb() instanceof EdgeAgentDbCommand);
        }

        @Test
        void unknownSubcommandFails() {
            IllegalArgumentException ex =
                    Assertions.assertThrows(
                            IllegalArgumentException.class, () -> buildDb("unknown-cmd"));
            Assertions.assertTrue(ex.getMessage().contains("Unknown db subcommand"));
        }

        @Test
        void walShowRequiresId() {
            IllegalArgumentException ex =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () -> buildDb("wal-show", "--sqlite-path", "/x"));
            Assertions.assertTrue(ex.getMessage().contains("--id"));
        }

        @Test
        void walPurgeDeadRequiresYes() throws Exception {
            Path dbPath = tempDir.resolve("purge.db");
            System.setProperty(EdgeAgentEnvConstants.PROP_AGENT_HOME, tempDir.toString());
            seedDeadWalRow(dbPath);

            IllegalArgumentException ex =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    buildDb("wal-purge-dead", "--sqlite-path", dbPath.toString())
                                            .execute());
            Assertions.assertTrue(ex.getMessage().contains("--yes"));
        }
    }

    @Nested
    class SqlitePaths {

        @Test
        void defaultSqliteUnderInstallRoot() {
            System.setProperty(EdgeAgentEnvConstants.PROP_AGENT_HOME, tempDir.toString());
            Path sqlite = EdgeAgentPaths.resolveSqlitePath(null);
            Assertions.assertEquals(tempDir.resolve("data/wal.db").normalize(), sqlite);
        }

        @Test
        void cliOverrideRelativeToInstallRoot() {
            System.setProperty(EdgeAgentEnvConstants.PROP_AGENT_HOME, tempDir.toString());
            Path cli = Paths.get("custom.db");
            Path resolved = EdgeAgentPaths.resolveSqlitePath(cli);
            Assertions.assertEquals(tempDir.resolve("custom.db").normalize(), resolved);
        }

        @Test
        void absoluteCliPathUnchanged() {
            Path absolute = tempDir.resolve("abs.db").toAbsolutePath().normalize();
            Path resolved = EdgeAgentPaths.resolveSqlitePath(absolute);
            Assertions.assertEquals(absolute, resolved);
        }
    }

    @Nested
    class DbCommandExecution {

        @Test
        void walSummaryAndListDeadRows() throws Exception {
            Path dbPath = tempDir.resolve("ops.db");
            System.setProperty(EdgeAgentEnvConstants.PROP_AGENT_HOME, tempDir.toString());
            long deadId = seedDeadWalRow(dbPath);

            String dbArg = dbPath.toString();
            String output =
                    captureStdout(
                            () -> {
                                try {
                                    buildDb("wal-summary", "--sqlite-path", dbArg).execute();
                                    buildDb(
                                                    "wal-list",
                                                    "--status",
                                                    WalRecordStatus.DEAD.name(),
                                                    "--sqlite-path",
                                                    dbArg,
                                                    "--limit",
                                                    "10")
                                            .execute();
                                    buildDb("wal-purge-dead", "--dry-run", "--sqlite-path", dbArg)
                                            .execute();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            Assertions.assertTrue(output.contains("DEAD"));
            Assertions.assertTrue(output.contains("DRY-RUN"));
            Assertions.assertTrue(output.contains(String.valueOf(deadId)));
        }

        @Test
        void infoOnEmptyDatabase() throws Exception {
            Path dbPath = tempDir.resolve("info.db");
            System.setProperty(EdgeAgentEnvConstants.PROP_AGENT_HOME, tempDir.toString());
            createWalDatabase(dbPath);

            String output =
                    captureStdout(
                            () -> {
                                try {
                                    buildDb("info", "--sqlite-path", dbPath.toString()).execute();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
            Assertions.assertFalse(output.isEmpty());
            Assertions.assertTrue(output.contains("wal-row-ids"));
            Assertions.assertTrue(output.contains("wal-list"));
        }

        @Test
        void walListPrintsWalShowHint() throws Exception {
            Path dbPath = tempDir.resolve("hint.db");
            System.setProperty(EdgeAgentEnvConstants.PROP_AGENT_HOME, tempDir.toString());
            long id = seedDeadWalRow(dbPath);

            String output =
                    captureStdout(
                            () -> {
                                try {
                                    buildDb("wal-list", "--sqlite-path", dbPath.toString())
                                            .execute();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            Assertions.assertTrue(output.contains("id(pk)"));
            Assertions.assertTrue(output.contains("wal-show --id"));
            Assertions.assertTrue(output.contains(String.valueOf(id)));
        }

        @Test
        void walShowPrintsRowId() throws Exception {
            Path dbPath = tempDir.resolve("show.db");
            System.setProperty(EdgeAgentEnvConstants.PROP_AGENT_HOME, tempDir.toString());
            long id;
            try (SqliteWalStore store = new SqliteWalStore(dbPath)) {
                id =
                        store.append(
                                EdgeEvent.builder()
                                        .sourceId("src")
                                        .payload(new byte[] {1})
                                        .eventTime(1L)
                                        .build());
            }

            String dbArg = dbPath.toString();
            String output =
                    captureStdout(
                            () -> {
                                try {
                                    buildDb(
                                                    "wal-show",
                                                    "--id",
                                                    String.valueOf(id),
                                                    "--sqlite-path",
                                                    dbArg)
                                            .execute();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
            Assertions.assertTrue(output.contains(String.valueOf(id)));
        }
    }

    private static void createWalDatabase(Path dbPath) throws SQLException {
        SqliteWalStore store = new SqliteWalStore(dbPath);
        store.close();
    }

    private static long seedDeadWalRow(Path dbPath) throws Exception {
        try (SqliteWalStore store = new SqliteWalStore(dbPath)) {
            long id =
                    store.append(
                            EdgeEvent.builder()
                                    .sourceId("s")
                                    .payload(new byte[] {1})
                                    .eventTime(1L)
                                    .build());
            for (int i = 0; i < 16; i++) {
                store.claimPending(10, 16);
                if (i < 15) {
                    store.resurrectSending(10);
                }
            }
            store.resurrectSending(10);
            store.markExceededAsDead(16, 10);
            return id;
        }
    }
}
