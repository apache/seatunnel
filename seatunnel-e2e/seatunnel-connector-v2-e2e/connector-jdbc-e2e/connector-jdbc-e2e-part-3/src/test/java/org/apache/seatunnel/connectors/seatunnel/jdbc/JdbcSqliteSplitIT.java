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
    private static final String NULL_PK_TABLE = "composite_null_pk_test";
    // order_id values of the rows whose line_no is NULL. With 90 base rows (order_id 0..89) and
    // split.size=10, the composite boundary walk yields leading-column boundaries at roughly
    // 9, 18, 27, 36, 45, 54, ... so 46/47/48 fall strictly INSIDE the middle split whose
    // leading-column range is (45, 54) — away from every boundary.
    private static final long[] NULL_ROW_ORDER_IDS = {46, 47, 48};

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

            // SQLite (legacy quirk) allows NULL values inside composite PRIMARY KEY columns.
            // The 90 base rows use DISTINCT ascending order_id values (0..89) so that a chunk
            // size of 10 produces middle splits with narrow leading-column ranges. The 3
            // NULL-line_no rows are placed at mid-range order_id values (see NULL_ROW_ORDER_IDS)
            // — strictly INSIDE a middle split's leading-column boundary range — which exercises
            // the duplication bug: without the IS NOT NULL guard on middle/last splits, such a
            // row satisfies the middle split's leading-column branch alone (order_id > start AND
            // order_id < end) AND the first split's NULL disjunct, so it is read twice.
            stmt.execute("DROP TABLE IF EXISTS " + NULL_PK_TABLE);
            stmt.execute(
                    "CREATE TABLE "
                            + NULL_PK_TABLE
                            + " (order_id BIGINT, line_no INT, "
                            + "payload VARCHAR(20), PRIMARY KEY (order_id, line_no))");
            try (PreparedStatement ps =
                    connection.prepareStatement(
                            "INSERT INTO "
                                    + NULL_PK_TABLE
                                    + " (order_id, line_no, payload) VALUES (?, ?, ?)")) {
                for (int i = 0; i < 90; i++) {
                    ps.setLong(1, i);
                    ps.setInt(2, i);
                    ps.setString(3, "p" + i);
                    ps.addBatch();
                }
                // rows whose second key component is NULL, at mid-range order_id values
                for (long orderId : NULL_ROW_ORDER_IDS) {
                    ps.setLong(1, orderId);
                    ps.setNull(2, java.sql.Types.INTEGER);
                    ps.setString(3, "null-pk-" + orderId);
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

    private static CatalogTable nullPkCatalogTable() {
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
                TableIdentifier.of("sqlite", "main", NULL_PK_TABLE),
                schema,
                new HashMap<>(),
                Collections.emptyList(),
                null);
    }

    @Test
    public void testCompositeKeyWithNullPrimaryKeyComponent() throws Exception {
        // SQLite permits NULL inside composite PRIMARY KEY columns. Rows with a NULL key
        // component cannot satisfy any tuple-comparison predicate, so without explicit NULL
        // handling they are silently dropped (data loss). They must land in the first chunk
        // and be read exactly once. The fixture places the NULL-secondary-key rows at order_id
        // values (see NULL_ROW_ORDER_IDS) that lie strictly inside a MIDDLE split's leading-column
        // boundary range — so a missing IS NOT NULL guard on middle/last splits would make at
        // least one of those rows match the middle split's leading-column branch alone
        // (order_id > start[0] AND order_id < end[0]) in ADDITION to the first split's NULL
        // disjunct, i.e. read twice (data duplication).
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", SQLITE_URL);
        configMap.put("driver", "org.sqlite.JDBC");
        configMap.put("table_path", NULL_PK_TABLE);
        configMap.put("split.size", "10");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        JdbcSourceConfig sourceConfig = JdbcSourceConfig.of(readonlyConfig);

        DynamicChunkSplitter splitter = new DynamicChunkSplitter(sourceConfig);
        CatalogTable table = nullPkCatalogTable();
        JdbcSourceTable jdbcSourceTable =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of(NULL_PK_TABLE))
                        .catalogTable(table)
                        .build();

        Collection<JdbcSourceSplit> jdbcSourceSplits = splitter.generateSplits(jdbcSourceTable);
        Assertions.assertTrue(
                jdbcSourceSplits.size() > 1,
                "Composite key should split into multiple chunks, got " + jdbcSourceSplits.size());
        JdbcSourceSplit[] splitArray = jdbcSourceSplits.toArray(new JdbcSourceSplit[0]);

        // Fixture sanity: at least one NULL key row's order_id must fall strictly inside a middle
        // split's (both bounds non-null) leading-column range; otherwise this test cannot expose
        // the duplication bug at all.
        boolean nullRowInsideMiddleSplitRange = false;
        for (JdbcSourceSplit split : splitArray) {
            Object[] start = (Object[]) split.getSplitStart();
            Object[] end = (Object[]) split.getSplitEnd();
            if (start != null && end != null) {
                long startLead = ((Number) start[0]).longValue();
                long endLead = ((Number) end[0]).longValue();
                for (long orderId : NULL_ROW_ORDER_IDS) {
                    if (startLead < orderId && endLead > orderId) {
                        nullRowInsideMiddleSplitRange = true;
                        break;
                    }
                }
            }
        }
        Assertions.assertTrue(
                nullRowInsideMiddleSplitRange,
                "Fixture broken: no middle split's leading-column range strictly contains one of "
                        + "the NULL key rows' order_id values "
                        + Arrays.toString(NULL_ROW_ORDER_IDS));

        TableSchema tableSchema = table.getTableSchema();
        Set<String> readKeys = new HashSet<>();
        int readCount = 0;
        int nullKeyRowCount = 0;
        int nullKeyRowsInFirstSplit = 0;
        try (Connection connection = DriverManager.getConnection(SQLITE_URL)) {
            for (int i = 0; i < splitArray.length; i++) {
                JdbcSourceSplit split = splitArray[i];
                try (PreparedStatement ps = splitter.generateSplitStatement(split, tableSchema);
                        ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        readCount++;
                        String orderId = rs.getString("order_id");
                        String lineNo = rs.getString("line_no");
                        boolean nullKey = orderId == null || lineNo == null;
                        if (nullKey) {
                            nullKeyRowCount++;
                            // rows with a NULL key component are captured by the first chunk
                            if (i == 0) {
                                nullKeyRowsInFirstSplit++;
                            }
                        }
                        readKeys.add(orderId + "|" + lineNo);
                    }
                }
            }
        }
        Assertions.assertEquals(
                93, readCount, "All 93 rows (including NULL-key rows) must be read");
        Assertions.assertEquals(
                93, readKeys.size(), "No (order_id, line_no) key may be duplicated or missing");
        Assertions.assertEquals(
                3, nullKeyRowCount, "The 3 NULL key component rows must be read exactly once");
        Assertions.assertEquals(
                3,
                nullKeyRowsInFirstSplit,
                "NULL key component rows must be captured by the first chunk");
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
