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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.common.utils.VectorUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeMapper;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.JdbcUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

/**
 * Verifies PostgreSQL pgvector reads, writes and automatic sink-table creation on Zeta.
 *
 * <p>Real catalog and JDBC metadata checks retain vector dimensions, including extension types
 * outside the search path, while exact row comparisons guard source-to-sink data conversion.
 */
@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "This pgvector suite currently validates Zeta; Flink and Spark are not covered")
public class JdbcPostgresPgVectorIT extends TestSuiteBase implements TestResource {

    private static final String PGVECTOR_IMAGE = "pgvector/pgvector:pg16";
    private static final String PG_DRIVER_JAR =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";
    private static final String PGVECTOR_SOURCE_DDL =
            "CREATE EXTENSION IF NOT EXISTS vector;\n"
                    + "CREATE TABLE IF NOT EXISTS pgvector_e2e_source_table (\n"
                    + "  id SERIAL PRIMARY KEY,\n"
                    + "  name VARCHAR(255),\n"
                    + "  embedding vector(3)\n"
                    + ")";
    private static final String PGVECTOR_SINK_DDL =
            "CREATE EXTENSION IF NOT EXISTS vector;\n"
                    + "CREATE TABLE IF NOT EXISTS pgvector_e2e_sink_table (\n"
                    + "  id SERIAL PRIMARY KEY,\n"
                    + "  name VARCHAR(255),\n"
                    + "  embedding vector(3)\n"
                    + ")";
    private static final String PGVECTOR_AUTO_CREATE_SOURCE_DDL =
            "CREATE EXTENSION IF NOT EXISTS vector;\n"
                    + "CREATE TABLE IF NOT EXISTS pgvector_e2e_auto_create_source_table (\n"
                    + "  id SERIAL PRIMARY KEY,\n"
                    + "  name VARCHAR(255),\n"
                    + "  embedding vector(3)\n"
                    + ")";

    private PostgreSQLContainer<?> PGVECTOR_CONTAINER;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + PG_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        PGVECTOR_CONTAINER =
                new PostgreSQLContainer<>(
                                DockerImageName.parse(PGVECTOR_IMAGE)
                                        .asCompatibleSubstituteFor("postgres"))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases("pgvector")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(PGVECTOR_IMAGE)));
        Startables.deepStart(Stream.of(PGVECTOR_CONTAINER)).join();
        log.info("PgVector container started");
        Class.forName(PGVECTOR_CONTAINER.getDriverClassName());
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);
        log.info("pgvector data initialization succeeded");
    }

    @TestTemplate
    public void testPgVectorSourceAndSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/jdbc_postgres_pgvector_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), "pgvector job run failed");

        List<List<Object>> src =
                querySql(
                        "select id, name, embedding::text from pgvector_e2e_source_table order by id");
        List<List<Object>> dst =
                querySql(
                        "select id, name, embedding::text from pgvector_e2e_sink_table order by id");
        Assertions.assertFalse(src.isEmpty());
        Assertions.assertEquals(src.size(), dst.size());
        for (int i = 0; i < src.size(); i++) {
            Assertions.assertEquals(src.get(i).get(0), dst.get(i).get(0));
            Assertions.assertEquals(src.get(i).get(1), dst.get(i).get(1));
            assertVectorEquals(src.get(i).get(2).toString(), dst.get(i).get(2).toString());
        }
        log.info("pgvector e2e test completed");
    }

    /**
     * Verifies vector dimensions are independent of the extension schema and search path.
     *
     * <p>The extension lives in an isolated database outside the connection search path so the
     * other source and sink scenarios keep their original database configuration.
     */
    @TestTemplate
    public void testPgVectorDimensionOutsideSearchPath(TestContainer container)
            throws SQLException {
        String database = "vector_metadata_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + database);
        }
        String url =
                PGVECTOR_CONTAINER
                        .getJdbcUrl()
                        .replace("/" + PGVECTOR_CONTAINER.getDatabaseName(), "/" + database);
        try (Connection connection =
                        DriverManager.getConnection(
                                url,
                                PGVECTOR_CONTAINER.getUsername(),
                                PGVECTOR_CONTAINER.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA extensions");
            statement.execute("CREATE SCHEMA vector_data");
            statement.execute("CREATE EXTENSION vector WITH SCHEMA extensions");
            statement.execute(
                    "CREATE TABLE vector_data.vector_dimensions (embedding extensions.vector(3))");
            // extensions is intentionally absent from the default search_path.
            try (ResultSet result =
                    statement.executeQuery("SELECT embedding FROM vector_data.vector_dimensions")) {
                Column column =
                        new PostgresTypeMapper()
                                .mappingColumn(
                                        result.getMetaData(),
                                        1,
                                        connection,
                                        "SELECT embedding FROM vector_data.vector_dimensions");
                Assertions.assertEquals(VectorType.VECTOR_FLOAT_TYPE, column.getDataType());
                Assertions.assertEquals(Integer.valueOf(3), column.getScale());
            }
            try (ResultSet result =
                    statement.executeQuery(
                            "SELECT embedding AS aliased_embedding FROM vector_data.vector_dimensions")) {
                Column column =
                        new PostgresTypeMapper()
                                .mappingColumn(
                                        result.getMetaData(),
                                        1,
                                        connection,
                                        "SELECT embedding AS aliased_embedding FROM vector_data.vector_dimensions");
                Assertions.assertEquals("aliased_embedding", column.getName());
                Assertions.assertEquals(VectorType.VECTOR_FLOAT_TYPE, column.getDataType());
                Assertions.assertEquals(Integer.valueOf(3), column.getScale());
            }
        }
        PostgresCatalog catalog =
                new PostgresCatalog(
                        "pgvector",
                        PGVECTOR_CONTAINER.getUsername(),
                        PGVECTOR_CONTAINER.getPassword(),
                        JdbcUrlUtil.getUrlInfo(url),
                        "public",
                        PGVECTOR_CONTAINER.getDriverClassName());
        catalog.open();
        try {
            Column column =
                    catalog.getTable(TablePath.of(database, "vector_data", "vector_dimensions"))
                            .getTableSchema()
                            .getColumns()
                            .get(0);
            Assertions.assertEquals(VectorType.VECTOR_FLOAT_TYPE, column.getDataType());
            Assertions.assertEquals(Integer.valueOf(3), column.getScale());
        } finally {
            catalog.close();
        }
    }

    /**
     * Tests the auto-create sink path: sink table is NOT pre-created, so SeaTunnel must infer the
     * schema from the source query (including vector dimension) and generate the sink DDL
     * automatically. This validates that vector(N) dimensions survive the full query-source ->
     * auto-create-sink pipeline.
     */
    @TestTemplate
    public void testPgVectorAutoCreateSink(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        // Exercise the real catalog path as well as query metadata: pgvector stores the
        // dimension directly in atttypmod, unlike PostgreSQL varchar's four-byte offset.
        PostgresCatalog catalog =
                new PostgresCatalog(
                        DatabaseIdentifier.POSTGRESQL,
                        PGVECTOR_CONTAINER.getUsername(),
                        PGVECTOR_CONTAINER.getPassword(),
                        JdbcUrlUtil.getUrlInfo(PGVECTOR_CONTAINER.getJdbcUrl()),
                        "public",
                        PGVECTOR_CONTAINER.getDriverClassName());
        TablePath nativeTarget =
                TablePath.of(
                        PGVECTOR_CONTAINER.getDatabaseName(),
                        "public",
                        "pgvector_native_copy_" + UUID.randomUUID().toString().replace("-", ""));
        catalog.open();
        try {
            CatalogTable source =
                    catalog.getTable(
                            TablePath.of(
                                    PGVECTOR_CONTAINER.getDatabaseName(),
                                    "public",
                                    "pgvector_e2e_auto_create_source_table"));
            Assertions.assertEquals(DatabaseIdentifier.POSTGRESQL, source.getCatalogName());
            Column embedding =
                    source.getTableSchema().getColumns().stream()
                            .filter(column -> "embedding".equals(column.getName()))
                            .findFirst()
                            .orElseThrow(
                                    () -> new AssertionError("Catalog omitted embedding column"));
            Assertions.assertEquals(VectorType.VECTOR_FLOAT_TYPE, embedding.getDataType());
            Assertions.assertEquals(Integer.valueOf(3), embedding.getScale());
            Assertions.assertEquals("vector(3)", embedding.getSourceType());
            // The native catalog shortcut takes sourceType verbatim rather than reconverting scale.
            catalog.createTable(nativeTarget, source, false);
            Column copied =
                    catalog.getTable(nativeTarget).getTableSchema().getColumns().stream()
                            .filter(column -> "embedding".equals(column.getName()))
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new AssertionError(
                                                    "Native copy omitted embedding column"));
            Assertions.assertEquals(Integer.valueOf(3), copied.getScale());
            Assertions.assertEquals("vector(3)", copied.getSourceType());
            ByteBuffer vector = VectorUtils.toByteBuffer(new Float[] {1.25f, -2.5f, 3.75f});
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {1001, "rebound", vector});
            try (Connection connection = getJdbcConnection();
                    PreparedStatement insert =
                            connection.prepareStatement(
                                    "INSERT INTO "
                                            + nativeTarget.getSchemaAndTableName("\"")
                                            + " (id, name, embedding) VALUES (?, ?, ?)")) {
                PostgresJdbcRowConverter converter = new PostgresJdbcRowConverter();
                converter.toExternal(source.getTableSchema(), null, row, insert);
                converter.toExternal(source.getTableSchema(), null, row, insert);
                Assertions.assertEquals(0, vector.position());
                Assertions.assertEquals(1, insert.executeUpdate());
                try (Statement statement = connection.createStatement();
                        ResultSet result =
                                statement.executeQuery(
                                        "SELECT embedding::text FROM "
                                                + nativeTarget.getSchemaAndTableName("\"")
                                                + " WHERE id = 1001")) {
                    Assertions.assertTrue(result.next());
                    assertVectorEquals("[1.25,-2.5,3.75]", result.getString(1));
                    Assertions.assertFalse(result.next());
                }
            }
        } finally {
            try {
                catalog.dropTable(nativeTarget, true);
            } finally {
                catalog.close();
            }
        }
        Container.ExecResult execResult =
                container.executeJob("/jdbc_postgres_pgvector_auto_create_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), "pgvector auto-create job run failed");

        // Verify data was written correctly
        List<List<Object>> src =
                querySql(
                        "select id, name, embedding::text from pgvector_e2e_auto_create_source_table order by id");
        List<List<Object>> dst =
                querySql(
                        "select id, name, embedding::text from pgvector_e2e_auto_create_sink_table order by id");
        Assertions.assertFalse(src.isEmpty());
        Assertions.assertEquals(src.size(), dst.size());
        for (int i = 0; i < src.size(); i++) {
            Assertions.assertEquals(src.get(i).get(0), dst.get(i).get(0));
            Assertions.assertEquals(src.get(i).get(1), dst.get(i).get(1));
            assertVectorEquals(src.get(i).get(2).toString(), dst.get(i).get(2).toString());
        }

        // Verify the sink table was created with the correct vector dimension
        List<List<Object>> columns =
                querySql(
                        "SELECT column_name, data_type FROM information_schema.columns "
                                + "WHERE table_name = 'pgvector_e2e_auto_create_sink_table' AND column_name = 'embedding'");
        Assertions.assertFalse(
                columns.isEmpty(), "embedding column should exist in auto-created sink table");
        // Note: information_schema.data_type may show "USER-DEFINED" for vector,
        // so we also check via pg_attribute to confirm the dimension
        List<List<Object>> vectorInfo =
                querySql(
                        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
                                + "WHERE attrelid = 'pgvector_e2e_auto_create_sink_table'::regclass AND attname = 'embedding'");
        Assertions.assertFalse(
                vectorInfo.isEmpty(), "embedding column should exist in pg_attribute");
        String fullType = vectorInfo.get(0).get(0).toString();
        Matcher dimensionMatcher = Pattern.compile("vector\\((\\d+)\\)").matcher(fullType);
        Assertions.assertTrue(
                dimensionMatcher.matches(), "Expected vector(N) type but got: " + fullType);
        int dimension = Integer.parseInt(dimensionMatcher.group(1));
        Assertions.assertEquals(
                3, dimension, "Expected vector(3) dimension but got vector(" + dimension + ")");
        log.info("pgvector auto-create e2e test completed, sink column type: {}", fullType);
    }

    private void assertVectorEquals(String expected, String actual) {
        double[] expectedArr = parseVector(expected);
        double[] actualArr = parseVector(actual);
        Assertions.assertEquals(expectedArr.length, actualArr.length);
        for (int i = 0; i < expectedArr.length; i++) {
            Assertions.assertEquals(
                    expectedArr[i], actualArr[i], 1e-6, "Vector mismatch at index " + i);
        }
    }

    private double[] parseVector(String vectorStr) {
        String content = vectorStr.replace("[", "").replace("]", "").trim();
        String[] parts = content.split(",");
        double[] result = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Double.parseDouble(parts[i].trim());
        }
        return result;
    }

    private void initializeJdbcTable() {
        try (Connection connection = getJdbcConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(PGVECTOR_SOURCE_DDL);
            statement.execute(PGVECTOR_SINK_DDL);
            statement.execute(
                    "INSERT INTO pgvector_e2e_source_table (name, embedding) VALUES "
                            + "('item1', '[0.1,0.2,0.3]'), "
                            + "('item2', '[0.4,0.5,0.6]'), "
                            + "('item3', '[0.7,0.8,0.9]')");
            // Auto-create test: only create source table (no sink table)
            statement.execute(PGVECTOR_AUTO_CREATE_SOURCE_DDL);
            statement.execute(
                    "INSERT INTO pgvector_e2e_auto_create_source_table (name, embedding) VALUES "
                            + "('item1', '[0.1,0.2,0.3]'), "
                            + "('item2', '[0.4,0.5,0.6]'), "
                            + "('item3', '[0.7,0.8,0.9]')");
        } catch (SQLException e) {
            throw new RuntimeException("Initializing pgvector table failed!", e);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                PGVECTOR_CONTAINER.getJdbcUrl(),
                PGVECTOR_CONTAINER.getUsername(),
                PGVECTOR_CONTAINER.getPassword());
    }

    private List<List<Object>> querySql(String sql) {
        return JdbcUtil.querySql(
                sql,
                () -> {
                    try {
                        return this.getJdbcConnection();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (PGVECTOR_CONTAINER != null) {
            PGVECTOR_CONTAINER.stop();
        }
    }
}
