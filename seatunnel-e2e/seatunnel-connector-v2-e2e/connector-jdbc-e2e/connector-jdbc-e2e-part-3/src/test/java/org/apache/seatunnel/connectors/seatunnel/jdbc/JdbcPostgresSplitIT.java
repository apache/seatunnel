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
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog;
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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

/** Tests composite primary key (multi-column) chunk splitting on PostgreSQL. */
public class JdbcPostgresSplitIT extends TestSuiteBase implements TestResource {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcPostgresSplitIT.class);

    private static final String PG_IMAGE = "postgres:16-alpine";
    private static final String PG_CONTAINER_HOST = "postgresql-e2e";
    private static final String PG_DATABASE = "auto";
    private static final String PG_TABLE = "composite_split_test";
    private static final String PG_SCHEMA = "public";

    private static final String PG_USERNAME = "test";
    private static final String PG_PASSWORD = "test";

    private PostgreSQLContainer<?> postgresContainer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        postgresContainer =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withImagePullPolicy(PullPolicy.ageBased(Duration.ofDays(7)))
                        .withUsername(PG_USERNAME)
                        .withPassword(PG_PASSWORD)
                        .withDatabaseName(PG_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(PG_CONTAINER_HOST)
                        .waitingFor(Wait.forListeningPort())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        Startables.deepStart(Stream.of(postgresContainer)).join();
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeTable);
    }

    private void initializeTable() throws SQLException {
        String createSql =
                "CREATE TABLE IF NOT EXISTS "
                        + PG_DATABASE
                        + "."
                        + PG_SCHEMA
                        + "."
                        + PG_TABLE
                        + " (order_id BIGINT NOT NULL, line_no INT NOT NULL, payload VARCHAR(20), "
                        + "PRIMARY KEY (order_id, line_no))";
        try (Connection connection = getJdbcConnection();
                PreparedStatement ps = connection.prepareStatement(createSql)) {
            ps.execute();
        }
        // First key column repeats heavily (only 0/1/2), second column disambiguates.
        try (Connection connection = getJdbcConnection();
                PreparedStatement ps =
                        connection.prepareStatement(
                                "INSERT INTO "
                                        + PG_DATABASE
                                        + "."
                                        + PG_SCHEMA
                                        + "."
                                        + PG_TABLE
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

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(),
                postgresContainer.getPassword());
    }

    @Test
    public void testCompositeKeySplit() throws Exception {
        TablePath tablePath = TablePath.of(PG_DATABASE, PG_SCHEMA, PG_TABLE);
        PostgresCatalog catalog =
                new PostgresCatalog(
                        "postgres",
                        PG_USERNAME,
                        PG_PASSWORD,
                        JdbcUrlUtil.getUrlInfo(postgresContainer.getJdbcUrl()),
                        PG_SCHEMA,
                        null);
        catalog.open();
        Assertions.assertTrue(catalog.tableExists(tablePath));
        CatalogTable table = catalog.getTable(tablePath);
        JdbcSourceTable jdbcSourceTable =
                JdbcSourceTable.builder().tablePath(tablePath).catalogTable(table).build();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", postgresContainer.getJdbcUrl());
        configMap.put("driver", postgresContainer.getDriverClassName());
        configMap.put("user", PG_USERNAME);
        configMap.put("password", PG_PASSWORD);
        configMap.put("table_path", PG_DATABASE + "." + PG_SCHEMA + "." + PG_TABLE);
        configMap.put("split.size", "10");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        DynamicChunkSplitter splitter =
                new DynamicChunkSplitter(JdbcSourceConfig.of(readonlyConfig));

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
        try (Connection connection = getJdbcConnection()) {
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
        catalog.close();
    }

    @Test
    public void testBoundaryQueryOffsetForm() throws Exception {
        // The optimized boundary query form (LIMIT 1 OFFSET chunkSize-1) must be supported and
        // return exactly one row equal to the last row of LIMIT chunkSize, so the boundary query
        // transfers 1 row instead of chunkSize rows (network/temporary-object saving).
        int chunkSize = 10;
        String tableRef = PG_DATABASE + "." + PG_SCHEMA + "." + PG_TABLE;
        try (Connection connection = getJdbcConnection();
                Statement stmt = connection.createStatement()) {
            java.util.List<String> limitRows = new java.util.ArrayList<>();
            try (ResultSet rs =
                    stmt.executeQuery(
                            "SELECT order_id, line_no FROM "
                                    + tableRef
                                    + " ORDER BY order_id ASC, line_no ASC LIMIT "
                                    + chunkSize)) {
                while (rs.next()) {
                    limitRows.add(rs.getLong(1) + "|" + rs.getInt(2));
                }
            }
            Assertions.assertEquals(chunkSize, limitRows.size());

            java.util.List<String> offsetRows = new java.util.ArrayList<>();
            try (ResultSet rs =
                    stmt.executeQuery(
                            "SELECT order_id, line_no FROM "
                                    + tableRef
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

    @Override
    @AfterAll
    public void tearDown() throws Exception {
        if (postgresContainer != null) {
            postgresContainer.close();
        }
    }
}
