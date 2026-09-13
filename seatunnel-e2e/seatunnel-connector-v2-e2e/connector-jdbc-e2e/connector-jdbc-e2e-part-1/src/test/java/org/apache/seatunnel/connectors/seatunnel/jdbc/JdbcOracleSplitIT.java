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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleURLParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.DynamicChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

/** Tests composite primary key (multi-column) chunk splitting on Oracle. */
public class JdbcOracleSplitIT extends TestSuiteBase implements TestResource {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcOracleSplitIT.class);

    // gvenzl/oracle-free is a multi-arch (amd64/arm64) substitute for gvenzl/oracle-xe
    private static final String ORACLE_IMAGE = "gvenzl/oracle-free:slim-faststart";
    private static final String ORACLE_NETWORK_ALIASES = "oracle-e2e";
    private static final int ORACLE_PORT = 1521;
    private static final String USERNAME = "TESTUSER";
    private static final String PASSWORD = "testPassword";
    private static final String SCHEMA = USERNAME;
    private static final String ORACLE_TABLE = "COMPOSITE_SPLIT_TEST";

    private OracleContainer oracleContainer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        DockerImageName imageName =
                DockerImageName.parse(ORACLE_IMAGE).asCompatibleSubstituteFor("gvenzl/oracle-xe");
        Map<String, String> containerEnv = new HashMap<>();
        containerEnv.put("ORACLE_PASSWORD", PASSWORD);
        containerEnv.put("APP_USER", USERNAME);
        containerEnv.put("APP_USER_PASSWORD", PASSWORD);
        oracleContainer =
                new OracleContainer(imageName)
                        .withDatabaseName(SCHEMA)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("sql/oracle_init.sql"),
                                "/container-entrypoint-startdb.d/init.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(ORACLE_NETWORK_ALIASES)
                        .withExposedPorts(ORACLE_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(ORACLE_IMAGE)));
        oracleContainer.setPortBindings(
                java.util.Collections.singletonList(
                        String.format("%s:%s", ORACLE_PORT, ORACLE_PORT)));
        Startables.deepStart(Stream.of(oracleContainer)).join();
        Class.forName("oracle.jdbc.OracleDriver");
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.MINUTES)
                .untilAsserted(this::initializeTable);
    }

    private void initializeTable() throws SQLException {
        String createSql =
                "CREATE TABLE "
                        + SCHEMA
                        + "."
                        + ORACLE_TABLE
                        + " (ORDER_ID NUMBER(19) NOT NULL, LINE_NO NUMBER(10) NOT NULL, "
                        + "PAYLOAD VARCHAR2(20), PRIMARY KEY (ORDER_ID, LINE_NO))";
        try (Connection connection = getJdbcConnection();
                PreparedStatement ps = connection.prepareStatement(createSql)) {
            ps.execute();
        }
        // First key column repeats heavily (only 0/1/2), second column disambiguates.
        try (Connection connection = getJdbcConnection();
                PreparedStatement ps =
                        connection.prepareStatement(
                                "INSERT INTO "
                                        + SCHEMA
                                        + "."
                                        + ORACLE_TABLE
                                        + " (ORDER_ID, LINE_NO, PAYLOAD) VALUES (?, ?, ?)")) {
            for (int i = 0; i < 300; i++) {
                ps.setLong(1, i % 3);
                ps.setInt(2, i / 3);
                ps.setString(3, "p" + i);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(getJdbcUrl(), USERNAME, PASSWORD);
    }

    private String getJdbcUrl() {
        return String.format("jdbc:oracle:thin:@localhost:%s/%s", ORACLE_PORT, SCHEMA);
    }

    @Test
    public void testCompositeKeySplit() throws Exception {
        String jdbcUrl = getJdbcUrl();
        TablePath tablePath = TablePath.of(null, SCHEMA, ORACLE_TABLE);
        OracleCatalog catalog =
                new OracleCatalog(
                        "oracle",
                        USERNAME,
                        PASSWORD,
                        OracleURLParser.parse(jdbcUrl),
                        SCHEMA,
                        "oracle.jdbc.OracleDriver");
        catalog.open();
        Assertions.assertTrue(catalog.tableExists(tablePath));
        CatalogTable table = catalog.getTable(tablePath);
        JdbcSourceTable jdbcSourceTable =
                JdbcSourceTable.builder().tablePath(tablePath).catalogTable(table).build();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", jdbcUrl);
        configMap.put("driver", "oracle.jdbc.OracleDriver");
        configMap.put("user", USERNAME);
        configMap.put("password", PASSWORD);
        configMap.put("table_path", SCHEMA + "." + ORACLE_TABLE);
        configMap.put("split.size", "10");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        DynamicChunkSplitter splitter =
                new DynamicChunkSplitter(JdbcSourceConfig.of(readonlyConfig));

        Collection<JdbcSourceSplit> jdbcSourceSplits = splitter.generateSplits(jdbcSourceTable);

        Assertions.assertTrue(
                jdbcSourceSplits.size() > 1,
                "Composite key should split into multiple chunks, got " + jdbcSourceSplits.size());
        JdbcSourceSplit[] splitArray = jdbcSourceSplits.toArray(new JdbcSourceSplit[0]);
        Assertions.assertEquals("ORDER_ID,LINE_NO", splitArray[0].getSplitKeyName());
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
        // once - 300 rows, no missing and no duplicate (ORDER_ID, LINE_NO) keys.
        TableSchema tableSchema = table.getTableSchema();
        Set<String> readKeys = new HashSet<>();
        int readCount = 0;
        try (Connection connection = getJdbcConnection()) {
            for (JdbcSourceSplit split : splitArray) {
                try (PreparedStatement ps = splitter.generateSplitStatement(split, tableSchema);
                        ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        readCount++;
                        readKeys.add(rs.getLong("ORDER_ID") + "|" + rs.getInt("LINE_NO"));
                    }
                }
            }
        }
        Assertions.assertEquals(300, readCount, "All 300 rows must be read through the splits");
        Assertions.assertEquals(
                300, readKeys.size(), "No (ORDER_ID, LINE_NO) key may be duplicated or missing");
        catalog.close();
    }

    @Test
    public void testCompositeKeySplitWithCustomQuery() throws Exception {
        String jdbcUrl = getJdbcUrl();
        TablePath tablePath = TablePath.of(null, SCHEMA, ORACLE_TABLE);
        OracleCatalog catalog =
                new OracleCatalog(
                        "oracle",
                        USERNAME,
                        PASSWORD,
                        OracleURLParser.parse(jdbcUrl),
                        SCHEMA,
                        "oracle.jdbc.OracleDriver");
        catalog.open();
        CatalogTable table = catalog.getTable(tablePath);
        // Query-based tables never reach the composite-key path: findSplitKey returns empty
        // unless a partition column is explicitly configured (composite splitting and a custom
        // query are mutually exclusive), so this test exercises the pre-existing single-split
        // "FROM (...) tmp" path on Oracle with the custom query executed correctly.
        String query = "SELECT ORDER_ID, LINE_NO, PAYLOAD FROM " + SCHEMA + "." + ORACLE_TABLE;
        JdbcSourceTable jdbcSourceTable =
                JdbcSourceTable.builder()
                        .tablePath(tablePath)
                        .query(query)
                        .catalogTable(table)
                        .build();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", jdbcUrl);
        configMap.put("driver", "oracle.jdbc.OracleDriver");
        configMap.put("user", USERNAME);
        configMap.put("password", PASSWORD);
        configMap.put("query", query);
        configMap.put("split.size", "10");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        DynamicChunkSplitter splitter =
                new DynamicChunkSplitter(JdbcSourceConfig.of(readonlyConfig));

        Collection<JdbcSourceSplit> jdbcSourceSplits = splitter.generateSplits(jdbcSourceTable);

        // Query-based tables keep a single full-table split by design (findSplitKey returns empty
        // unless a partition column is explicitly configured); the point of this test is that the
        // custom query is executed correctly on Oracle and all rows are readable.
        Assertions.assertEquals(1, jdbcSourceSplits.size(), "Query-based table keeps single split");
        JdbcSourceSplit single = jdbcSourceSplits.iterator().next();
        Assertions.assertNull(single.getSplitKeyName());

        // Data-correctness with custom query: all 300 rows readable.
        TableSchema tableSchema = table.getTableSchema();
        Set<String> readKeys = new HashSet<>();
        int readCount = 0;
        try (Connection connection = getJdbcConnection()) {
            try (PreparedStatement ps = splitter.generateSplitStatement(single, tableSchema);
                    ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    readCount++;
                    readKeys.add(rs.getLong("ORDER_ID") + "|" + rs.getInt("LINE_NO"));
                }
            }
        }
        Assertions.assertEquals(300, readCount, "All 300 rows must be read through the split");
        Assertions.assertEquals(
                300, readKeys.size(), "No (ORDER_ID, LINE_NO) key may be duplicated or missing");
        catalog.close();
    }

    @Test
    public void testBoundaryQueryOffsetForm() throws Exception {
        // Oracle 12c+ supports the OFFSET ... ROWS FETCH NEXT form (no LIMIT syntax); the
        // optimized boundary query must return exactly one row equal to the last row of the
        // first chunkSize rows (network/temporary-object saving).
        int chunkSize = 10;
        String tableRef = SCHEMA + "." + ORACLE_TABLE;
        try (Connection connection = getJdbcConnection();
                Statement stmt = connection.createStatement()) {
            java.util.List<String> limitRows = new ArrayList<>();
            try (ResultSet rs =
                    stmt.executeQuery(
                            "SELECT ORDER_ID, LINE_NO FROM "
                                    + tableRef
                                    + " ORDER BY ORDER_ID ASC, LINE_NO ASC FETCH FIRST "
                                    + chunkSize
                                    + " ROWS ONLY")) {
                while (rs.next()) {
                    limitRows.add(rs.getLong(1) + "|" + rs.getInt(2));
                }
            }
            Assertions.assertEquals(chunkSize, limitRows.size());

            java.util.List<String> offsetRows = new ArrayList<>();
            try (ResultSet rs =
                    stmt.executeQuery(
                            "SELECT ORDER_ID, LINE_NO FROM "
                                    + tableRef
                                    + " ORDER BY ORDER_ID ASC, LINE_NO ASC OFFSET "
                                    + (chunkSize - 1)
                                    + " ROWS FETCH NEXT 1 ROWS ONLY")) {
                while (rs.next()) {
                    offsetRows.add(rs.getLong(1) + "|" + rs.getInt(2));
                }
            }
            Assertions.assertEquals(
                    1, offsetRows.size(), "OFFSET/FETCH must return exactly one row");
            Assertions.assertEquals(
                    limitRows.get(limitRows.size() - 1),
                    offsetRows.get(0),
                    "Boundary row must match the last row of the first chunkSize rows");
        }
    }

    @Override
    @AfterAll
    public void tearDown() throws Exception {
        if (oracleContainer != null) {
            oracleContainer.close();
        }
    }
}
