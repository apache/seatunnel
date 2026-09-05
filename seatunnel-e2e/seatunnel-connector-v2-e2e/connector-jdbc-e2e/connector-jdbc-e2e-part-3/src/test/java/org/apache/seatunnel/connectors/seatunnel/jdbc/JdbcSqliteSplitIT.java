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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.DynamicChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Tests composite primary key (multi-column) chunk splitting on embedded SQLite. */
public class JdbcSqliteSplitIT {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSqliteSplitIT.class);

    // A file-based DB (not :memory:) so all connections share the same database.
    private static final String SQLITE_URL =
            "jdbc:sqlite:" + System.getProperty("java.io.tmpdir") + "/seatunnel_split_e2e.db";
    private static final String TABLE = "composite_split_test";

    @BeforeAll
    public static void setUp() throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection connection = DriverManager.getConnection(SQLITE_URL);
                Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + TABLE);
            stmt.execute(
                    "CREATE TABLE "
                            + TABLE
                            + " (order_id BIGINT NOT NULL, line_no INT NOT NULL, "
                            + "payload VARCHAR(20), PRIMARY KEY (order_id, line_no))");
            try (PreparedStatement ps =
                    connection.prepareStatement(
                            "INSERT INTO "
                                    + TABLE
                                    + " (order_id, line_no, payload) VALUES (?, ?, ?)")) {
                for (int i = 0; i < 300; i++) {
                    ps.setLong(1, i % 3);
                    ps.setInt(2, i / 3);
                    ps.setString(3, "p" + i);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }

    @AfterAll
    public static void tearDown() throws Exception {
        // Remove the temporary file-based SQLite DB created by setUp().
        Files.deleteIfExists(
                Paths.get(System.getProperty("java.io.tmpdir"), "seatunnel_split_e2e.db"));
    }

    private static CatalogTable catalogTable() {
        TableSchema schema =
                TableSchema.builder()
                        .columns(
                                Arrays.asList(
                                        PhysicalColumn.builder()
                                                .name("order_id")
                                                .sourceType("BIGINT")
                                                .dataType(BasicType.LONG_TYPE)
                                                .build(),
                                        PhysicalColumn.builder()
                                                .name("line_no")
                                                .sourceType("INT")
                                                .dataType(BasicType.INT_TYPE)
                                                .build(),
                                        PhysicalColumn.builder()
                                                .name("payload")
                                                .sourceType("VARCHAR")
                                                .dataType(BasicType.STRING_TYPE)
                                                .build()))
                        .primaryKey(new PrimaryKey("pk", Arrays.asList("order_id", "line_no")))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("sqlite", "main", TABLE),
                schema,
                new HashMap<>(),
                Collections.emptyList(),
                null);
    }

    @Test
    public void testCompositeKeySplit() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", SQLITE_URL);
        configMap.put("driver", "org.sqlite.JDBC");
        configMap.put("table_path", TABLE);
        configMap.put("split.size", "10");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        JdbcSourceConfig sourceConfig = JdbcSourceConfig.of(readonlyConfig);

        DynamicChunkSplitter splitter = new DynamicChunkSplitter(sourceConfig);
        CatalogTable table = catalogTable();
        JdbcSourceTable jdbcSourceTable =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of(TABLE))
                        .catalogTable(table)
                        .build();

        Collection<JdbcSourceSplit> jdbcSourceSplits = splitter.generateSplits(jdbcSourceTable);

        Assertions.assertTrue(
                jdbcSourceSplits.size() > 1,
                "Composite key should split into multiple chunks, got " + jdbcSourceSplits.size());
        JdbcSourceSplit[] splitArray = jdbcSourceSplits.toArray(new JdbcSourceSplit[0]);
        Assertions.assertEquals("order_id,line_no", splitArray[0].getSplitKeyName());
        for (JdbcSourceSplit split : splitArray) {
            if (split.getSplitStart() != null) {
                Assertions.assertTrue(
                        split.getSplitStart() instanceof Object[],
                        "Composite split start should be an Object[] tuple");
            }
            if (split.getSplitEnd() != null) {
                Assertions.assertTrue(
                        split.getSplitEnd() instanceof Object[],
                        "Composite split end should be an Object[] tuple");
            }
        }

        // Data-correctness: reading through every split must reconstruct the source table exactly
        // once - 300 rows, no missing and no duplicate (order_id, line_no) keys.
        TableSchema tableSchema = table.getTableSchema();
        Set<String> readKeys = new HashSet<>();
        int readCount = 0;
        try (Connection connection = DriverManager.getConnection(SQLITE_URL)) {
            for (JdbcSourceSplit split : splitArray) {
                try (PreparedStatement ps = splitter.generateSplitStatement(split, tableSchema);
                        ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        readCount++;
                        readKeys.add(rs.getLong("order_id") + "|" + rs.getInt("line_no"));
                    }
                }
            }
        }
        Assertions.assertEquals(300, readCount, "All 300 rows must be read through the splits");
        Assertions.assertEquals(
                300, readKeys.size(), "No (order_id, line_no) key may be duplicated or missing");
    }

    @Test
    public void testBoundaryQueryOffsetForm() throws Exception {
        // The optimized boundary query form (LIMIT 1 OFFSET chunkSize-1) must be supported and
        // return exactly one row equal to the last row of LIMIT chunkSize, so the boundary query
        // transfers 1 row instead of chunkSize rows (network/temporary-object saving).
        int chunkSize = 10;
        try (Connection connection = DriverManager.getConnection(SQLITE_URL);
                Statement stmt = connection.createStatement()) {
            java.util.List<String> limitRows = new ArrayList<>();
            try (ResultSet rs =
                    stmt.executeQuery(
                            "SELECT order_id, line_no FROM "
                                    + TABLE
                                    + " ORDER BY order_id ASC, line_no ASC LIMIT "
                                    + chunkSize)) {
                while (rs.next()) {
                    limitRows.add(rs.getLong(1) + "|" + rs.getInt(2));
                }
            }
            Assertions.assertEquals(chunkSize, limitRows.size());

            java.util.List<String> offsetRows = new ArrayList<>();
            try (ResultSet rs =
                    stmt.executeQuery(
                            "SELECT order_id, line_no FROM "
                                    + TABLE
                                    + " ORDER BY order_id ASC, line_no ASC LIMIT 1 OFFSET "
                                    + (chunkSize - 1))) {
                while (rs.next()) {
                    offsetRows.add(rs.getLong(1) + "|" + rs.getInt(2));
                }
            }
            Assertions.assertEquals(
                    1, offsetRows.size(), "LIMIT 1 OFFSET must return exactly one row");
            Assertions.assertEquals(
                    limitRows.get(limitRows.size() - 1),
                    offsetRows.get(0),
                    "Boundary row must match the last row of LIMIT chunkSize");
        }
    }
}
