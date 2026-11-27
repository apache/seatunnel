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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormatBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb.DuckDBTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialect;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JdbcUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class JdbcPostgresIT extends TestSuiteBase implements TestResource {
    private static final String PG_IMAGE = "postgis/postgis";

    /**
     * PostgreSQL table used to verify catalog behavior after dropping a physical column while
     * preserving the order of all remaining visible columns.
     */
    private static final String DROPPED_COLUMN_TABLE = "pg_catalog_dropped_column_test";

    private static final String UPDATE_CONDITION_TABLE = "pg_e2e_update_condition_sink";

    private static final String UPDATE_CONDITION_REWRITE_TABLE =
            "pg_e2e_update_condition_rewrite_sink";

    private static final String UPDATE_CONDITION_ABSENT_TABLE =
            "pg_e2e_update_condition_absent_sink";

    private static final String UPDATE_CONDITION_CK_TABLE = "pg_e2e_update_condition_ck_sink";

    private static final String UPDATE_CONDITION_CK_REWRITE_TABLE =
            "pg_e2e_update_condition_ck_rewrite_sink";

    private static final String UPDATE_CONDITION_CONCURRENT_TABLE =
            "pg_e2e_update_condition_concurrent_sink";

    /** Partitioned-table fixtures for the full batch read test. */
    private static final String PARTITION_SOURCE_TABLE = "pg_e2e_partition_source_table";

    private static final String PARTITION_SINK_TABLE = "pg_e2e_partition_sink_table";

    private static final String PARTITION_SOURCE_DDL =
            "CREATE TABLE IF NOT EXISTS "
                    + PARTITION_SOURCE_TABLE
                    + " (\n"
                    + "  gid INT NOT NULL,\n"
                    + "  name VARCHAR(255),\n"
                    + "  PRIMARY KEY (gid)\n"
                    + ") PARTITION BY RANGE (gid)";

    private static final String PARTITION_SINK_DDL =
            "CREATE TABLE IF NOT EXISTS "
                    + PARTITION_SINK_TABLE
                    + " (\n"
                    + "  gid INT PRIMARY KEY,\n"
                    + "  name VARCHAR(255)\n"
                    + ")";

    private static final List<String> PG_CONFIG_FILE_LIST =
            Lists.newArrayList(
                    "/jdbc_postgres_source_and_sink.conf",
                    "/jdbc_postgres_source_and_sink_copy_stmt.conf",
                    "/jdbc_postgres_source_and_sink_parallel.conf",
                    "/jdbc_postgres_source_and_sink_parallel_upper_lower.conf",
                    "/jdbc_postgres_source_and_sink_xa.conf");
    private PostgreSQLContainer<?> POSTGRESQL_CONTAINER;
    private static final String PG_SOURCE_DDL =
            "CREATE TABLE IF NOT EXISTS pg_e2e_source_table (\n"
                    + "  gid SERIAL PRIMARY KEY,\n"
                    + "  uuid_col UUID,\n"
                    + "  text_col TEXT,\n"
                    + "  varchar_col VARCHAR(255),\n"
                    + "  char_one_col CHAR(1),\n"
                    + "  char_col CHAR(10),\n"
                    + "  boolean_col bool,\n"
                    + "  smallint_col int2,\n"
                    + "  integer_col int4,\n"
                    + "  bigint_col BIGINT,\n"
                    + "  decimal_col DECIMAL(10, 2),\n"
                    + "  numeric_col NUMERIC(8, 4),\n"
                    + "  real_col float4,\n"
                    + "  double_precision_col float8,\n"
                    + "  smallserial_col SMALLSERIAL,\n"
                    + "  serial_col SERIAL,\n"
                    + "  bigserial_col BIGSERIAL,\n"
                    + "  date_col DATE,\n"
                    + "  timestamp_col TIMESTAMP,\n"
                    + "  timestamp_tz_col TIMESTAMP WITH TIME ZONE,\n"
                    + "  bpchar_col BPCHAR(10),\n"
                    + "  age INT NOT null,\n"
                    + "  name VARCHAR(255) NOT null,\n"
                    + "  point geometry(POINT, 4326),\n"
                    + "  linestring geometry(LINESTRING, 4326),\n"
                    + "  polygon_colums geometry(POLYGON, 4326),\n"
                    + "  multipoint geometry(MULTIPOINT, 4326),\n"
                    + "  multilinestring geometry(MULTILINESTRING, 4326),\n"
                    + "  multipolygon geometry(MULTIPOLYGON, 4326),\n"
                    + "  geometrycollection geometry(GEOMETRYCOLLECTION, 4326),\n"
                    + "  geog geography(POINT, 4326),\n"
                    + "  json_col json NOT NULL,\n"
                    + "  jsonb_col jsonb NOT NULL,\n"
                    + "  xml_col xml NOT NULL\n"
                    + ");comment on column pg_e2e_source_table.uuid_col is '\"#¥%……&*（）;;'',,.\\.``````//''@特殊注释''\\\\''\"'";
    private static final String PG_SINK_DDL =
            "CREATE TABLE IF NOT EXISTS pg_e2e_sink_table (\n"
                    + "    gid SERIAL PRIMARY KEY,\n"
                    + "    uuid_col UUID,\n"
                    + "    text_col TEXT,\n"
                    + "    varchar_col VARCHAR(255),\n"
                    + "    char_one_col CHAR(1),\n"
                    + "    char_col CHAR(10),\n"
                    + "    boolean_col bool,\n"
                    + "    smallint_col int2,\n"
                    + "    integer_col int4,\n"
                    + "    bigint_col BIGINT,\n"
                    + "    decimal_col DECIMAL(10, 2),\n"
                    + "    numeric_col NUMERIC(8, 4),\n"
                    + "    real_col float4,\n"
                    + "    double_precision_col float8,\n"
                    + "    smallserial_col SMALLSERIAL,\n"
                    + "    serial_col SERIAL,\n"
                    + "    bigserial_col BIGSERIAL,\n"
                    + "    date_col DATE,\n"
                    + "    timestamp_col TIMESTAMP,\n"
                    + "    timestamp_tz_col TIMESTAMP WITH TIME ZONE,\n"
                    + "    bpchar_col BPCHAR(10),\n"
                    + "    age int4 NOT NULL,\n"
                    + "    name varchar(255) NOT NULL,\n"
                    + "    point varchar(2000) NULL,\n"
                    + "    linestring varchar(2000) NULL,\n"
                    + "    polygon_colums varchar(2000) NULL,\n"
                    + "    multipoint varchar(2000) NULL,\n"
                    + "    multilinestring varchar(2000) NULL,\n"
                    + "    multipolygon varchar(2000) NULL,\n"
                    + "    geometrycollection varchar(2000) NULL,\n"
                    + "    geog varchar(2000) NULL,\n"
                    + "    json_col json NOT NULL,\n"
                    + "    jsonb_col jsonb NOT NULL,\n"
                    + "    xml_col xml NOT NULL\n"
                    + "  )";
    private static final String SOURCE_SQL =
            "select \n"
                    + "gid,\n"
                    + "uuid_col, \n"
                    + "text_col,\n"
                    + "varchar_col,\n"
                    + "char_one_col,\n"
                    + "char_col,\n"
                    + "boolean_col,\n"
                    + "smallint_col,\n"
                    + "integer_col,\n"
                    + "bigint_col,\n"
                    + "decimal_col,\n"
                    + "numeric_col,\n"
                    + "real_col,\n"
                    + "double_precision_col,\n"
                    + "smallserial_col,\n"
                    + "serial_col,\n"
                    + "bigserial_col,\n"
                    + "date_col,\n"
                    + "timestamp_col,\n"
                    + "timestamp_tz_col,\n"
                    + "bpchar_col,\n"
                    + "age,\n"
                    + "name,\n"
                    + "point,\n"
                    + "linestring,\n"
                    + "polygon_colums,\n"
                    + "multipoint,\n"
                    + "multilinestring,\n"
                    + "multipolygon,\n"
                    + "geometrycollection,\n"
                    + "geog,\n"
                    + "json_col,\n"
                    + "jsonb_col,\n"
                    + " cast(xml_col as varchar) \n"
                    + "from pg_e2e_source_table\n"
                    + "order by gid";
    private static final String SINK_SQL =
            "select\n"
                    + "  gid,\n"
                    + "uuid_col, \n"
                    + "   text_col,\n"
                    + "   varchar_col,\n"
                    + "   char_one_col,\n"
                    + "   char_col,\n"
                    + "   boolean_col,\n"
                    + "   smallint_col,\n"
                    + "   integer_col,\n"
                    + "   bigint_col,\n"
                    + "   decimal_col,\n"
                    + "   numeric_col,\n"
                    + "   real_col,\n"
                    + "   double_precision_col,\n"
                    + "   smallserial_col,\n"
                    + "   serial_col,\n"
                    + "   bigserial_col,\n"
                    + "   date_col,\n"
                    + "   timestamp_col,\n"
                    + "   timestamp_tz_col,\n"
                    + "   bpchar_col,\n"
                    + "  age,\n"
                    + "  name,\n"
                    + "  cast(point as geometry) as point,\n"
                    + "  cast(linestring as geometry) as linestring,\n"
                    + "  cast(polygon_colums as geometry) as polygon_colums,\n"
                    + "  cast(multipoint as geometry) as multipoint,\n"
                    + "  cast(multilinestring as geometry) as multilinestring,\n"
                    + "  cast(multipolygon as geometry) as multipolygon,\n"
                    + "  cast(geometrycollection as geometry) as geometrycollection,\n"
                    + "  cast(geog as geography) as geog,\n"
                    + "   json_col,\n"
                    + "   jsonb_col,\n"
                    + "  cast(xml_col as varchar) \n"
                    + "from\n"
                    + "  pg_e2e_sink_table\n"
                    + "order by gid";

    /**
     * Tables holding numeric special values (NaN, Infinity, -Infinity) which have no BigDecimal
     * representation, used to verify the numeric_special_value_as_null option. Infinity requires
     * PostgreSQL 14 or above, the tests using these tables skip on older servers.
     */
    private static final String NUMERIC_SPECIAL_VALUE_SOURCE_TABLE =
            "pg_e2e_numeric_special_value_source_table";

    private static final String NUMERIC_SPECIAL_VALUE_SINK_TABLE =
            "pg_e2e_numeric_special_value_sink_table";
    private static final String NUMERIC_SPECIAL_VALUE_SOURCE_DDL =
            "CREATE TABLE IF NOT EXISTS "
                    + NUMERIC_SPECIAL_VALUE_SOURCE_TABLE
                    + " (\n"
                    + "  gid SERIAL PRIMARY KEY,\n"
                    + "  numeric_col NUMERIC\n"
                    + ")";
    private static final String NUMERIC_SPECIAL_VALUE_SINK_DDL =
            "CREATE TABLE IF NOT EXISTS "
                    + NUMERIC_SPECIAL_VALUE_SINK_TABLE
                    + " (\n"
                    + "  gid INT PRIMARY KEY,\n"
                    + "  numeric_col NUMERIC\n"
                    + ")";
    private static final String NUMERIC_SPECIAL_VALUE_SINK_QUERY =
            "select gid, numeric_col from " + NUMERIC_SPECIAL_VALUE_SINK_TABLE + " order by gid";

    /**
     * Dedicated database created and dropped by the truncate routing test, so truncating a table
     * path that points to another database never touches the container's default fixture tables.
     */
    private static final String TRUNCATE_ROUTE_DATABASE = "pg_e2e_truncate_route_db";

    /** Same table name in both the catalog database and {@link #TRUNCATE_ROUTE_DATABASE}. */
    private static final String TRUNCATE_ROUTE_TABLE = "pg_e2e_truncate_route_table";

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                DependencyJar.ofClassName("org.postgresql.Driver")
                        .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib");
                DependencyJar.ofClassName("org.postgis.DriverWrapper")
                        .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib");
                DependencyJar.ofClassName("org.postgis.Geometry")
                        .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib");
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        POSTGRESQL_CONTAINER =
                new PostgreSQLContainer<>(
                                DockerImageName.parse(PG_IMAGE)
                                        .asCompatibleSubstituteFor("postgres"))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases("postgresql")
                        .withCommand("postgres -c max_prepared_transactions=100")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        Startables.deepStart(Stream.of(POSTGRESQL_CONTAINER)).join();
        log.info("PostgreSQL container started");
        Class.forName(POSTGRESQL_CONTAINER.getDriverClassName());
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);
        log.info("pg data initialization succeeded. Procedure");
    }

    @Test
    public void testCreateIndex() {
        String schema = "public";
        String databaseName = POSTGRESQL_CONTAINER.getDatabaseName();
        TablePath sourceTablePath = TablePath.of(databaseName, "public", "pg_e2e_source_table");
        TablePath targetTablePath = TablePath.of(databaseName, "public", "pg_ide_sink_table_2");
        PostgresCatalog postgresCatalog =
                new PostgresCatalog(
                        DatabaseIdentifier.POSTGRESQL,
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword(),
                        JdbcUrlUtil.getUrlInfo(POSTGRESQL_CONTAINER.getJdbcUrl()),
                        schema,
                        null);
        postgresCatalog.open();

        CatalogTable catalogTable = postgresCatalog.getTable(sourceTablePath);

        dropTableWithAssert(postgresCatalog, targetTablePath, true);
        // not create index
        createIndexOrNot(postgresCatalog, targetTablePath, catalogTable, false);
        Assertions.assertFalse(hasIndex(postgresCatalog, targetTablePath));

        dropTableWithAssert(postgresCatalog, targetTablePath, true);
        // create index
        createIndexOrNot(postgresCatalog, targetTablePath, catalogTable, true);
        Assertions.assertTrue(hasIndex(postgresCatalog, targetTablePath));

        dropTableWithAssert(postgresCatalog, targetTablePath, true);

        postgresCatalog.close();
    }

    @Test
    public void testDuckDbUnboundedStringAutoDdl() throws Exception {
        String label = String.join("", Collections.nCopies(400, "m"));
        String tableName = "duckdb_string_lengths";
        DuckDBTypeConverter converter = new DuckDBTypeConverter();
        Column bit =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("bits")
                                .columnType("BIT")
                                .dataType("BIT")
                                .build());
        Column en =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("en")
                                .columnType("ENUM('" + label + "')")
                                .dataType("ENUM('" + label + "')")
                                .length(0L)
                                .build());
        Column boundedBit =
                converter.convert(
                        BasicTypeDefine.builder()
                                .name("bounded_bits")
                                .columnType("BIT")
                                .dataType("BIT")
                                .length(5L)
                                .build());
        CatalogTable sourceTable =
                CatalogTable.of(
                        TableIdentifier.of("duckdb", "source", "text_lengths"),
                        TableSchema.builder().column(bit).column(en).column(boundedBit).build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");
        TablePath targetTablePath =
                TablePath.of(POSTGRESQL_CONTAINER.getDatabaseName(), "public", tableName);
        try (PostgresCatalog postgresCatalog =
                new PostgresCatalog(
                        DatabaseIdentifier.POSTGRESQL,
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword(),
                        JdbcUrlUtil.getUrlInfo(POSTGRESQL_CONTAINER.getJdbcUrl()),
                        "public",
                        null)) {
            postgresCatalog.open();
            Assertions.assertFalse(postgresCatalog.tableExists(targetTablePath));
            postgresCatalog.createTable(targetTablePath, sourceTable, false);
            try (Connection connection = getJdbcConnection()) {
                Assertions.assertEquals(
                        Arrays.asList(
                                Arrays.asList("bits", "text", null),
                                Arrays.asList("en", "text", null),
                                Arrays.asList("bounded_bits", "character varying", 5)),
                        querySql(
                                "SELECT column_name, data_type, character_maximum_length "
                                        + "FROM information_schema.columns WHERE table_schema = 'public' "
                                        + "AND table_name = '"
                                        + tableName
                                        + "' ORDER BY ordinal_position"));
                try (PreparedStatement insert =
                        connection.prepareStatement(
                                "INSERT INTO public." + tableName + " VALUES (?, ?, ?)")) {
                    insert.setString(1, "10110");
                    insert.setString(2, label);
                    insert.setString(3, "10110");
                    Assertions.assertEquals(1, insert.executeUpdate());
                    insert.setString(1, null);
                    insert.setString(2, null);
                    insert.setString(3, null);
                    Assertions.assertEquals(1, insert.executeUpdate());
                }
                try (PreparedStatement select =
                                connection.prepareStatement(
                                        "SELECT bits, en, bounded_bits FROM public."
                                                + tableName
                                                + " ORDER BY bits NULLS LAST");
                        ResultSet rs = select.executeQuery()) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals("10110", rs.getString(1));
                    Assertions.assertEquals(label, rs.getString(2));
                    Assertions.assertEquals("10110", rs.getString(3));
                    Assertions.assertTrue(rs.next());
                    Assertions.assertNull(rs.getString(1));
                    Assertions.assertNull(rs.getString(2));
                    Assertions.assertNull(rs.getString(3));
                    Assertions.assertFalse(rs.next());
                }
            } finally {
                postgresCatalog.dropTable(targetTablePath, false);
            }
        }
    }

    protected boolean hasIndex(Catalog catalog, TablePath targetTablePath) {
        TableSchema tableSchema = catalog.getTable(targetTablePath).getTableSchema();
        PrimaryKey primaryKey = tableSchema.getPrimaryKey();
        List<ConstraintKey> constraintKeys = tableSchema.getConstraintKeys();
        if (primaryKey != null && StringUtils.isNotBlank(primaryKey.getPrimaryKey())) {
            return true;
        }
        if (!constraintKeys.isEmpty()) {
            return true;
        }
        return false;
    }

    private void dropTableWithAssert(
            PostgresCatalog postgresCatalog, TablePath targetTablePath, boolean ignoreIfNotExists) {
        postgresCatalog.dropTable(targetTablePath, ignoreIfNotExists);
        Assertions.assertFalse(postgresCatalog.tableExists(targetTablePath));
    }

    private void createIndexOrNot(
            PostgresCatalog postgresCatalog,
            TablePath targetTablePath,
            CatalogTable catalogTable,
            boolean createIndex) {
        postgresCatalog.createTable(targetTablePath, catalogTable, false, createIndex);
        Assertions.assertTrue(postgresCatalog.tableExists(targetTablePath));
    }

    @TestTemplate
    public void testAutoGenerateSQL(TestContainer container)
            throws IOException, InterruptedException {
        for (String CONFIG_FILE : PG_CONFIG_FILE_LIST) {
            try {
                Container.ExecResult execResult = container.executeJob(CONFIG_FILE);
                Assertions.assertEquals(
                        0,
                        execResult.getExitCode(),
                        CONFIG_FILE
                                + " job run failed in "
                                + container.getClass().getSimpleName()
                                + ".");
                java.util.List<java.util.List<Object>> src = querySql(SOURCE_SQL);
                java.util.List<java.util.List<Object>> dst = querySql(SINK_SQL);
                if (!src.isEmpty() && !dst.isEmpty()) {
                    Object srcTz = src.get(0).size() > 19 ? src.get(0).get(19) : null;
                    Object dstTz = dst.get(0).size() > 19 ? dst.get(0).get(19) : null;
                    log.info("First row tz src={}, dst={}", srcTz, dstTz);
                }
                Assertions.assertIterableEquals(src, dst);
            } finally {
                executeSQL("truncate table pg_e2e_sink_table");
            }
            log.info(CONFIG_FILE + " e2e test completed");
        }
    }

    /**
     * Verifies that the numeric special values NaN, Infinity and -Infinity are read as null when
     * numeric_special_value_as_null is enabled, in the default (text) transfer mode.
     */
    @TestTemplate
    public void testNumericSpecialValueAsNull(TestContainer container)
            throws IOException, InterruptedException {
        assumeNumericInfinitySupported();
        prepareNumericSpecialValueTables();
        try {
            Container.ExecResult execResult =
                    container.executeJob(
                            "/jdbc_postgres_source_and_sink_numeric_special_value.conf");
            Assertions.assertEquals(
                    0,
                    execResult.getExitCode(),
                    "jdbc_postgres_source_and_sink_numeric_special_value.conf job run failed.");
            assertNumericSpecialValueSinkTable();
        } finally {
            truncateNumericSpecialValueTables();
        }
    }

    /**
     * Verifies that the numeric special values are also read as null when the driver uses the
     * binary transfer protocol, where the driver fails with a ClassCastException for NaN and an
     * IllegalArgumentException for Infinity and -Infinity instead of a PSQLException. The driver
     * only switches to binary results for server-prepared statements, so the job sets
     * prepareThreshold=-1 to prepare the statement before its first execution.
     */
    @TestTemplate
    public void testNumericSpecialValueAsNullWithBinaryTransfer(TestContainer container)
            throws IOException, InterruptedException {
        assumeNumericInfinitySupported();
        prepareNumericSpecialValueTables();
        try {
            Container.ExecResult execResult =
                    container.executeJob(
                            "/jdbc_postgres_source_and_sink_numeric_special_value_binary.conf");
            Assertions.assertEquals(
                    0,
                    execResult.getExitCode(),
                    "jdbc_postgres_source_and_sink_numeric_special_value_binary.conf job run failed.");
            assertNumericSpecialValueSinkTable();
        } finally {
            truncateNumericSpecialValueTables();
        }
    }

    /**
     * Verifies that the default behavior is unchanged: without numeric_special_value_as_null the
     * job fails on a numeric special value.
     */
    @TestTemplate
    public void testNumericSpecialValueFailsJobByDefault(TestContainer container)
            throws IOException, InterruptedException {
        assumeNumericInfinitySupported();
        prepareNumericSpecialValueTables();
        try {
            Container.ExecResult execResult =
                    container.executeJob(
                            "/jdbc_postgres_source_and_sink_numeric_special_value_default.conf");
            Assertions.assertNotEquals(
                    0,
                    execResult.getExitCode(),
                    "The job should fail when numeric_special_value_as_null is disabled.");
        } finally {
            truncateNumericSpecialValueTables();
        }
    }

    /**
     * Verifies {@code update_condition_sql}. The same changes are written with the condition (with
     * and without {@code reWriteBatchedInserts}) and without it. With the condition every change is
     * applied in arrival order, so the newest version wins in both input orders; without it the
     * sink keeps reducing each key to its last change.
     */
    @TestTemplate
    public void testUpdateConditionSql(TestContainer container)
            throws IOException, InterruptedException {
        List<String> tables =
                Arrays.asList(
                        UPDATE_CONDITION_TABLE,
                        UPDATE_CONDITION_REWRITE_TABLE,
                        UPDATE_CONDITION_ABSENT_TABLE);
        for (String table : tables) {
            executeSQL("DROP TABLE IF EXISTS " + table);
            executeSQL(
                    "CREATE TABLE "
                            + table
                            + " (id INT PRIMARY KEY, name VARCHAR(64), update_time BIGINT)");
            executeSQL(
                    "INSERT INTO "
                            + table
                            + " VALUES (1, 'stored', 100), (2, 'stored', 100), (3, 'stored', 100),"
                            + " (4, 'stored', NULL), (7, 'stored', 100)");
        }
        try {
            Container.ExecResult execResult =
                    container.executeJob("/jdbc_postgres_update_condition_sql.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

            List<List<Object>> expectedWithCondition =
                    Arrays.asList(
                            Arrays.asList(1, "v300", 300L), // older -> newer
                            Arrays.asList(2, "v300", 300L), // newer -> older
                            Arrays.asList(3, "stored", 100L), // false, then NULL predicate
                            Arrays.asList(4, "stored", null), // stored NULL, NULL predicate
                            Arrays.asList(5, "new", 10L), // missing key is inserted
                            Arrays.asList(6, "v300", 300L), // missing key, newer -> older
                            Arrays.asList(8, "v300", 300L)); // missing key, older -> newer
            // key 7 is gone: DELETE is not guarded by the condition
            Assertions.assertEquals(
                    expectedWithCondition, queryUpdateConditionTable(tables.get(0)));
            Assertions.assertEquals(
                    expectedWithCondition, queryUpdateConditionTable(tables.get(1)));

            // Without the option the last change of each key in the batch wins, as before.
            Assertions.assertEquals(
                    Arrays.asList(
                            Arrays.asList(1, "v300", 300L),
                            Arrays.asList(2, "v200", 200L),
                            Arrays.asList(3, "null-incoming", null),
                            Arrays.asList(4, "v500", 500L),
                            Arrays.asList(5, "new", 10L),
                            Arrays.asList(6, "v200", 200L),
                            Arrays.asList(8, "v300", 300L)),
                    queryUpdateConditionTable(tables.get(2)));
        } finally {
            tables.forEach(table -> executeSQL("DROP TABLE IF EXISTS " + table));
        }
    }

    @TestTemplate
    public void testUpdateConditionSqlWithCompositeKey(TestContainer container)
            throws IOException, InterruptedException {
        List<String> tables =
                Arrays.asList(UPDATE_CONDITION_CK_TABLE, UPDATE_CONDITION_CK_REWRITE_TABLE);
        for (String table : tables) {
            executeSQL("DROP TABLE IF EXISTS " + table);
            executeSQL(
                    "CREATE TABLE "
                            + table
                            + " (a INT, b INT, v VARCHAR(64), update_time BIGINT, PRIMARY KEY (a, b))");
            executeSQL(
                    "INSERT INTO "
                            + table
                            + " VALUES (1, 1, 'stored', 100), (1, 2, 'stored', 100)");
        }
        try {
            Container.ExecResult execResult =
                    container.executeJob("/jdbc_postgres_update_condition_sql_composite_key.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

            List<List<Object>> expected =
                    Arrays.asList(
                            Arrays.asList(1, 1, "v300", 300L),
                            Arrays.asList(1, 2, "fresh", 150L),
                            Arrays.asList(2, 1, "new", 10L));
            for (String table : tables) {
                Assertions.assertEquals(
                        expected,
                        querySql("SELECT a, b, v, update_time FROM " + table + " ORDER BY a, b"));
            }
        } finally {
            tables.forEach(table -> executeSQL("DROP TABLE IF EXISTS " + table));
        }
    }

    @TestTemplate
    public void testInvalidUpdateConditionSqlFailsTheJob(TestContainer container)
            throws IOException, InterruptedException {
        executeSQL("DROP TABLE IF EXISTS " + UPDATE_CONDITION_TABLE);
        executeSQL(
                "CREATE TABLE "
                        + UPDATE_CONDITION_TABLE
                        + " (id INT PRIMARY KEY, name VARCHAR(64), update_time BIGINT)");
        try {
            Container.ExecResult execResult =
                    container.executeJob("/jdbc_postgres_update_condition_sql_invalid.conf");
            Assertions.assertNotEquals(0, execResult.getExitCode());
            Assertions.assertTrue(
                    (execResult.getStdout() + execResult.getStderr())
                            .contains("unknown column 'updat_time'"),
                    execResult.getStderr());
            Assertions.assertEquals(
                    Collections.emptyList(), querySql("SELECT id FROM " + UPDATE_CONDITION_TABLE));
        } finally {
            executeSQL("DROP TABLE IF EXISTS " + UPDATE_CONDITION_TABLE);
        }
    }

    /**
     * Verifies {@code update_condition_sql} with two concurrent writers of the same key. The first
     * writer keeps its transaction open, so the second one has to wait for the row. Once the first
     * commits, the condition is evaluated against the committed row, for a stored key as well as
     * for a key that both writers insert, and in both orders of arrival.
     */
    @Test
    public void testUpdateConditionSqlWithConcurrentWriters() throws Exception {
        executeSQL("DROP TABLE IF EXISTS " + UPDATE_CONDITION_CONCURRENT_TABLE);
        executeSQL(
                "CREATE TABLE "
                        + UPDATE_CONDITION_CONCURRENT_TABLE
                        + " (id INT PRIMARY KEY, name VARCHAR(64), update_time BIGINT)");
        executeSQL(
                "INSERT INTO "
                        + UPDATE_CONDITION_CONCURRENT_TABLE
                        + " VALUES (1, 'stored', 100), (2, 'stored', 100)");
        try {
            // Stored keys 1 and 2, missing keys 3 and 4.
            writeConcurrently(1, 300L, 200L);
            writeConcurrently(2, 200L, 300L);
            writeConcurrently(3, 300L, 200L);
            writeConcurrently(4, 200L, 300L);

            Assertions.assertEquals(
                    Arrays.asList(
                            Arrays.asList(1, "v300", 300L),
                            Arrays.asList(2, "v300", 300L),
                            Arrays.asList(3, "v300", 300L),
                            Arrays.asList(4, "v300", 300L)),
                    queryUpdateConditionTable(UPDATE_CONDITION_CONCURRENT_TABLE));
        } finally {
            executeSQL("DROP TABLE IF EXISTS " + UPDATE_CONDITION_CONCURRENT_TABLE);
        }
    }

    /**
     * Writes one version of a key with each of two sink writers, so that the second write overlaps
     * the uncommitted first one.
     */
    private void writeConcurrently(int id, long firstTime, long secondTime) throws Exception {
        SimpleJdbcConnectionProvider firstConnection = updateConditionConnection();
        SimpleJdbcConnectionProvider secondConnection = updateConditionConnection();
        JdbcOutputFormat<SeaTunnelRow, ?> first = openUpdateConditionWriter(firstConnection);
        JdbcOutputFormat<SeaTunnelRow, ?> second = openUpdateConditionWriter(secondConnection);
        CompletableFuture<Void> secondWrite = null;
        try {
            first.writeRecord(new SeaTunnelRow(new Object[] {id, "v" + firstTime, firstTime}));
            first.flush();

            secondWrite =
                    CompletableFuture.runAsync(
                            () -> {
                                second.writeRecord(
                                        new SeaTunnelRow(
                                                new Object[] {id, "v" + secondTime, secondTime}));
                                try {
                                    second.flush();
                                    secondConnection.getConnection().commit();
                                } catch (IOException | SQLException e) {
                                    throw new CompletionException(e);
                                }
                            });
            // The second writer must really be blocked by the first one, not run after it.
            given().await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    !querySql(
                                                    "SELECT 1 FROM pg_stat_activity"
                                                            + " WHERE wait_event_type = 'Lock'")
                                            .isEmpty());
            Assertions.assertFalse(secondWrite.isDone());

            firstConnection.getConnection().commit();
            secondWrite.get(30, TimeUnit.SECONDS);
        } finally {
            // Releases the row lock if an assertion failed before the first writer committed.
            if (secondWrite != null && !secondWrite.isDone()) {
                firstConnection.getConnection().rollback();
                secondWrite.get(30, TimeUnit.SECONDS);
            }
            first.close();
            second.close();
            firstConnection.closeConnection();
            secondConnection.closeConnection();
        }
    }

    /**
     * Verifies {@code update_condition_sql} with {@code reWriteBatchedInserts=true} for two keys
     * that PostgreSQL treats as equal while Java does not, so they must not share a statement.
     */
    @Test
    public void testUpdateConditionSqlWithRewriteForKeysOnlyTheDatabaseFindsEqual()
            throws Exception {
        executeSQL("DROP TABLE IF EXISTS " + UPDATE_CONDITION_CONCURRENT_TABLE);
        executeSQL(
                "CREATE TABLE "
                        + UPDATE_CONDITION_CONCURRENT_TABLE
                        + " (id NUMERIC(10, 2) PRIMARY KEY, update_time BIGINT)");
        Map<String, Object> config = new HashMap<>();
        config.put("url", POSTGRESQL_CONTAINER.getJdbcUrl());
        config.put("driver", POSTGRESQL_CONTAINER.getDriverClassName());
        config.put("username", POSTGRESQL_CONTAINER.getUsername());
        config.put("password", POSTGRESQL_CONTAINER.getPassword());
        config.put("properties", Collections.singletonMap("reWriteBatchedInserts", "true"));
        config.put("database", POSTGRESQL_CONTAINER.getDatabaseName());
        config.put("table", "public." + UPDATE_CONDITION_CONCURRENT_TABLE);
        config.put("primary_keys", Collections.singletonList("id"));
        config.put("update_condition_sql", ":update_time > target.update_time");
        JdbcSinkConfig sinkConfig = JdbcSinkConfig.of(ReadonlyConfig.fromMap(config));
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", new DecimalType(10, 2), 10L, false, null, ""))
                        .column(
                                PhysicalColumn.of(
                                        "update_time", BasicType.LONG_TYPE, 22L, true, null, ""))
                        .build();
        SimpleJdbcConnectionProvider connectionProvider =
                new SimpleJdbcConnectionProvider(sinkConfig.getJdbcConnectionConfig());
        JdbcOutputFormat<SeaTunnelRow, ?> writer =
                new JdbcOutputFormatBuilder(
                                new PostgresDialect(),
                                connectionProvider,
                                sinkConfig,
                                schema,
                                schema)
                        .build();
        try {
            writer.open();
            writer.writeRecord(new SeaTunnelRow(new Object[] {new BigDecimal("1.0"), 300L}));
            writer.writeRecord(new SeaTunnelRow(new Object[] {new BigDecimal("1.00"), 200L}));
            writer.flush();

            Assertions.assertEquals(
                    Collections.singletonList(Arrays.asList(new BigDecimal("1.00"), 300L)),
                    querySql("SELECT id, update_time FROM " + UPDATE_CONDITION_CONCURRENT_TABLE));
        } finally {
            writer.close();
            connectionProvider.closeConnection();
            executeSQL("DROP TABLE IF EXISTS " + UPDATE_CONDITION_CONCURRENT_TABLE);
        }
    }

    private JdbcSinkConfig updateConditionSinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", POSTGRESQL_CONTAINER.getJdbcUrl());
        config.put("driver", POSTGRESQL_CONTAINER.getDriverClassName());
        config.put("username", POSTGRESQL_CONTAINER.getUsername());
        config.put("password", POSTGRESQL_CONTAINER.getPassword());
        config.put("database", POSTGRESQL_CONTAINER.getDatabaseName());
        config.put("table", "public." + UPDATE_CONDITION_CONCURRENT_TABLE);
        config.put("primary_keys", Collections.singletonList("id"));
        config.put("auto_commit", false);
        config.put("update_condition_sql", ":update_time > target.update_time");
        return JdbcSinkConfig.of(ReadonlyConfig.fromMap(config));
    }

    private SimpleJdbcConnectionProvider updateConditionConnection() {
        return new SimpleJdbcConnectionProvider(
                updateConditionSinkConfig().getJdbcConnectionConfig());
    }

    private JdbcOutputFormat<SeaTunnelRow, ?> openUpdateConditionWriter(
            SimpleJdbcConnectionProvider connectionProvider) throws IOException {
        TableSchema schema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 22L, false, null, ""))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 64L, true, null, ""))
                        .column(
                                PhysicalColumn.of(
                                        "update_time", BasicType.LONG_TYPE, 22L, true, null, ""))
                        .build();
        JdbcOutputFormat<SeaTunnelRow, ?> outputFormat =
                new JdbcOutputFormatBuilder(
                                new PostgresDialect(),
                                connectionProvider,
                                updateConditionSinkConfig(),
                                schema,
                                schema)
                        .build();
        outputFormat.open();
        return outputFormat;
    }

    private List<List<Object>> queryUpdateConditionTable(String table) {
        return querySql("SELECT id, name, update_time FROM " + table + " ORDER BY id");
    }

    private void assumeNumericInfinitySupported() {
        // The numeric type only accepts Infinity since PostgreSQL 14
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "select current_setting('server_version_num')::int >= 140000")) {
            Assumptions.assumeTrue(
                    resultSet.next() && resultSet.getBoolean(1),
                    "PostgreSQL 14+ is required for numeric Infinity values");
        } catch (SQLException e) {
            throw new RuntimeException("Checking PostgreSQL server version failed!", e);
        }
    }

    private void prepareNumericSpecialValueTables() {
        executeSQL(NUMERIC_SPECIAL_VALUE_SOURCE_DDL);
        executeSQL(NUMERIC_SPECIAL_VALUE_SINK_DDL);
        executeSQL("truncate table " + NUMERIC_SPECIAL_VALUE_SOURCE_TABLE);
        executeSQL("truncate table " + NUMERIC_SPECIAL_VALUE_SINK_TABLE);
        executeSQL(
                "insert into "
                        + NUMERIC_SPECIAL_VALUE_SOURCE_TABLE
                        + " (gid, numeric_col) values (1, 'NaN'), (2, 'Infinity'), (3, '-Infinity'), (4, '123.45'), (5, null)");
    }

    private void truncateNumericSpecialValueTables() {
        executeSQL("truncate table " + NUMERIC_SPECIAL_VALUE_SINK_TABLE);
        executeSQL("truncate table " + NUMERIC_SPECIAL_VALUE_SOURCE_TABLE);
    }

    private void assertNumericSpecialValueSinkTable() {
        List<List<Object>> rows = querySql(NUMERIC_SPECIAL_VALUE_SINK_QUERY);
        Assertions.assertEquals(5, rows.size());
        // NaN, Infinity and -Infinity are read as null, a real null stays null
        Assertions.assertNull(rows.get(0).get(1));
        Assertions.assertNull(rows.get(1).get(1));
        Assertions.assertNull(rows.get(2).get(1));
        Assertions.assertNull(rows.get(4).get(1));
        // Regular values are unaffected. Compare numerically to ignore BigDecimal scale
        // differences (e.g. 123.45 vs 123.450000000000000000).
        Object actual = rows.get(3).get(1);
        Assertions.assertNotNull(actual);
        Assertions.assertEquals(
                0,
                new java.math.BigDecimal("123.45")
                        .compareTo(new java.math.BigDecimal(String.valueOf(actual))));
    }

    @Test
    public void testCatalog() {
        String schema = "public";
        String databaseName = POSTGRESQL_CONTAINER.getDatabaseName();
        String tableName = "pg_e2e_sink_table";
        String catalogDatabaseName = "pg_e2e_catalog_database";
        String catalogTableName = "pg_e2e_catalog_table";

        Catalog catalog =
                new PostgresCatalog(
                        DatabaseIdentifier.POSTGRESQL,
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword(),
                        JdbcUrlUtil.getUrlInfo(POSTGRESQL_CONTAINER.getJdbcUrl()),
                        schema,
                        null);
        catalog.open();

        TablePath tablePath = new TablePath(databaseName, schema, tableName);
        TablePath catalogTablePath = new TablePath(catalogDatabaseName, schema, catalogTableName);

        Assertions.assertFalse(catalog.databaseExists(catalogTablePath.getDatabaseName()));
        catalog.createDatabase(catalogTablePath, false);
        Assertions.assertTrue(catalog.databaseExists(catalogTablePath.getDatabaseName()));

        CatalogTable catalogTable = catalog.getTable(tablePath);
        catalog.createTable(catalogTablePath, catalogTable, false);
        Assertions.assertTrue(catalog.tableExists(catalogTablePath));

        catalog.dropTable(catalogTablePath, false);
        Assertions.assertFalse(catalog.tableExists(catalogTablePath));

        catalog.dropDatabase(catalogTablePath, false);
        Assertions.assertFalse(catalog.databaseExists(catalogTablePath.getDatabaseName()));

        catalog.close();
    }

    /**
     * Truncating {@code database.schema.table} through a catalog whose own database is the
     * container default database must truncate the database of the table path, and must leave the
     * table with the same name in the catalog database untouched.
     *
     * <p>Only the dedicated database and table created here are truncated and dropped.
     */
    @Test
    public void testTruncateTableUsesTablePathDatabase() throws SQLException {
        String schema = "public";
        String catalogDatabaseName = POSTGRESQL_CONTAINER.getDatabaseName();
        TablePath catalogDatabaseTablePath =
                TablePath.of(catalogDatabaseName, schema, TRUNCATE_ROUTE_TABLE);
        TablePath routedDatabaseTablePath =
                TablePath.of(TRUNCATE_ROUTE_DATABASE, schema, TRUNCATE_ROUTE_TABLE);
        String createTableSql =
                "CREATE TABLE " + schema + "." + TRUNCATE_ROUTE_TABLE + " (gid INT)";
        String insertRowSql = "INSERT INTO " + schema + "." + TRUNCATE_ROUTE_TABLE + " VALUES (1)";

        PostgresCatalog postgresCatalog =
                new PostgresCatalog(
                        DatabaseIdentifier.POSTGRESQL,
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword(),
                        JdbcUrlUtil.getUrlInfo(POSTGRESQL_CONTAINER.getJdbcUrl()),
                        schema,
                        null);
        postgresCatalog.open();
        try {
            postgresCatalog.createDatabase(routedDatabaseTablePath, true);
            postgresCatalog.executeSql(catalogDatabaseTablePath, createTableSql);
            postgresCatalog.executeSql(routedDatabaseTablePath, createTableSql);
            postgresCatalog.executeSql(catalogDatabaseTablePath, insertRowSql);
            postgresCatalog.executeSql(routedDatabaseTablePath, insertRowSql);
            Assertions.assertEquals(1, countRows(catalogDatabaseName, TRUNCATE_ROUTE_TABLE));
            Assertions.assertEquals(1, countRows(TRUNCATE_ROUTE_DATABASE, TRUNCATE_ROUTE_TABLE));

            postgresCatalog.truncateTable(routedDatabaseTablePath, false);

            Assertions.assertEquals(
                    1,
                    countRows(catalogDatabaseName, TRUNCATE_ROUTE_TABLE),
                    "The table in the catalog database must not be truncated");
            Assertions.assertEquals(
                    0,
                    countRows(TRUNCATE_ROUTE_DATABASE, TRUNCATE_ROUTE_TABLE),
                    "The table in the database of the table path must be truncated");
        } finally {
            try {
                postgresCatalog.dropTable(routedDatabaseTablePath, true);
                postgresCatalog.dropTable(catalogDatabaseTablePath, true);
            } finally {
                try {
                    postgresCatalog.dropDatabase(routedDatabaseTablePath, true);
                } finally {
                    postgresCatalog.close();
                }
            }
        }
    }

    /**
     * Counts the rows of a table in the given database on the PostgreSQL container.
     *
     * @param databaseName database to connect to
     * @param tableName table to count rows of, in the {@code public} schema
     * @return the number of rows in the table
     */
    private int countRows(String databaseName, String tableName) throws SQLException {
        String baseJdbcUrl = POSTGRESQL_CONTAINER.getJdbcUrl();
        String databaseUrl =
                baseJdbcUrl.replace(
                        "/" + POSTGRESQL_CONTAINER.getDatabaseName(), "/" + databaseName);
        String countSql = "SELECT count(1) FROM public." + tableName;
        try (Connection connection = getJdbcConnection(databaseUrl);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(countSql)) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    /**
     * Verifies PostgreSQL catalog discovery preserves visible column order after a column is
     * dropped.
     */
    @Test
    public void testCatalogExcludesDroppedColumns() {
        String schema = "public";
        String databaseName = POSTGRESQL_CONTAINER.getDatabaseName();
        TablePath tablePath = TablePath.of(databaseName, schema, DROPPED_COLUMN_TABLE);
        PostgresCatalog postgresCatalog =
                new PostgresCatalog(
                        DatabaseIdentifier.POSTGRESQL,
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword(),
                        JdbcUrlUtil.getUrlInfo(POSTGRESQL_CONTAINER.getJdbcUrl()),
                        schema,
                        null);
        postgresCatalog.open();
        try {
            postgresCatalog.dropTable(tablePath, true);
            postgresCatalog.executeSql(
                    tablePath,
                    "CREATE TABLE "
                            + schema
                            + "."
                            + DROPPED_COLUMN_TABLE
                            + " (c1 INT, c2 VARCHAR(50), c3 INT, c4 TEXT)");
            postgresCatalog.executeSql(
                    tablePath,
                    "ALTER TABLE " + schema + "." + DROPPED_COLUMN_TABLE + " DROP COLUMN c2");

            CatalogTable table = postgresCatalog.getTable(tablePath);
            List<String> actualColumns =
                    table.getTableSchema().getColumns().stream()
                            .map(Column::getName)
                            .collect(Collectors.toList());

            Assertions.assertEquals(Arrays.asList("c1", "c3", "c4"), actualColumns);
        } finally {
            try {
                postgresCatalog.dropTable(tablePath, true);
            } finally {
                postgresCatalog.close();
            }
        }
    }

    private void initializeJdbcTable() {
        try (Connection connection = getJdbcConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(PG_SOURCE_DDL);
            statement.execute(PG_SINK_DDL);
            for (int i = 1; i <= 1000; i++) {
                statement.addBatch(
                        "INSERT INTO\n"
                                + "  pg_e2e_source_table (gid,\n"
                                + "    uuid_col,\n"
                                + "    text_col,\n"
                                + "    varchar_col,\n"
                                + "    char_one_col,\n"
                                + "    char_col,\n"
                                + "    boolean_col,\n"
                                + "    smallint_col,\n"
                                + "    integer_col,\n"
                                + "    bigint_col,\n"
                                + "    decimal_col,\n"
                                + "    numeric_col,\n"
                                + "    real_col,\n"
                                + "    double_precision_col,\n"
                                + "    smallserial_col,\n"
                                + "    serial_col,\n"
                                + "    bigserial_col,\n"
                                + "    date_col,\n"
                                + "    timestamp_col,\n"
                                + "    timestamp_tz_col,\n"
                                + "    bpchar_col,\n"
                                + "    age,\n"
                                + "    name,\n"
                                + "    point,\n"
                                + "    linestring,\n"
                                + "    polygon_colums,\n"
                                + "    multipoint,\n"
                                + "    multilinestring,\n"
                                + "    multipolygon,\n"
                                + "    geometrycollection,\n"
                                + "    geog,\n"
                                + "    json_col,\n"
                                + "    jsonb_col, \n"
                                + "    xml_col \n"
                                + "  )\n"
                                + "VALUES\n"
                                + "  (\n"
                                + "    '"
                                + i
                                + "',\n"
                                + "    gen_random_uuid(),\n"
                                + "    'Hello World',\n"
                                + "    'Test',\n"
                                + "    'T',\n"
                                + "    'Testing',\n"
                                + "    true,\n"
                                + "    10,\n"
                                + "    100,\n"
                                + "    1000,\n"
                                + "    10.55,\n"
                                + "    8.8888,\n"
                                + "    3.14,\n"
                                + "    3.14159265,\n"
                                + "    1,\n"
                                + "    100,\n"
                                + "    10000,\n"
                                + "    '2023-05-07',\n"
                                + "    '2023-05-07 14:30:00',\n"
                                + "    '2023-05-07 14:30:00+08:00',\n"
                                + "    'Testing',\n"
                                + "    21,\n"
                                + "    'Leblanc',\n"
                                + "    ST_GeomFromText('POINT(-122.3452 47.5925)', 4326),\n"
                                + "    ST_GeomFromText(\n"
                                + "      'LINESTRING(-122.3451 47.5924, -122.3449 47.5923)',\n"
                                + "      4326\n"
                                + "    ),\n"
                                + "    ST_GeomFromText(\n"
                                + "      'POLYGON((-122.3453 47.5922, -122.3453 47.5926, -122.3448 47.5926, -122.3448 47.5922, -122.3453 47.5922))',\n"
                                + "      4326\n"
                                + "    ),\n"
                                + "    ST_GeomFromText(\n"
                                + "      'MULTIPOINT(-122.3459 47.5927, -122.3445 47.5918)',\n"
                                + "      4326\n"
                                + "    ),\n"
                                + "    ST_GeomFromText(\n"
                                + "      'MULTILINESTRING((-122.3463 47.5920, -122.3461 47.5919),(-122.3459 47.5924, -122.3457 47.5923))',\n"
                                + "      4326\n"
                                + "    ),\n"
                                + "    ST_GeomFromText(\n"
                                + "      'MULTIPOLYGON(((-122.3458 47.5925, -122.3458 47.5928, -122.3454 47.5928, -122.3454 47.5925, -122.3458 47.5925)),((-122.3453 47.5921, -122.3453 47.5924, -122.3448 47.5924, -122.3448 47.5921, -122.3453 47.5921)))',\n"
                                + "      4326\n"
                                + "    ),\n"
                                + "    ST_GeomFromText(\n"
                                + "      'GEOMETRYCOLLECTION(POINT(-122.3462 47.5921), LINESTRING(-122.3460 47.5924, -122.3457 47.5924))',\n"
                                + "      4326\n"
                                + "    ),\n"
                                + "    ST_GeographyFromText('POINT(-122.3452 47.5925)'),\n"
                                + "    '{\"key\":\"test\"}',\n"
                                + "    '{\"key\":\"test\"}',\n"
                                + "    '<XX:NewSize>test</XX:NewSize>'\n"
                                + "  )");
            }

            statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return getJdbcConnection(POSTGRESQL_CONTAINER.getJdbcUrl());
    }

    /**
     * Opens a connection to the given PostgreSQL URL on the container.
     *
     * @param jdbcUrl JDBC URL to connect to, including the database to use
     * @return an open connection, owned by the caller
     */
    private Connection getJdbcConnection(String jdbcUrl) throws SQLException {
        return DriverManager.getConnection(
                jdbcUrl, POSTGRESQL_CONTAINER.getUsername(), POSTGRESQL_CONTAINER.getPassword());
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

    private void executeSQL(String sql) {
        try (Connection connection = getJdbcConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (POSTGRESQL_CONTAINER != null) {
            POSTGRESQL_CONTAINER.stop();
        }
    }

    @Test
    public void testCatalogForSaveMode() {
        String schema = "public";
        String databaseName = POSTGRESQL_CONTAINER.getDatabaseName();
        TablePath tablePathPG = TablePath.of(databaseName, "public", "pg_e2e_source_table");
        TablePath tablePathPgSink = TablePath.of(databaseName, "public", "pg_ide_sink_table_2");
        PostgresCatalog postgresCatalog =
                new PostgresCatalog(
                        DatabaseIdentifier.POSTGRESQL,
                        POSTGRESQL_CONTAINER.getUsername(),
                        POSTGRESQL_CONTAINER.getPassword(),
                        JdbcUrlUtil.getUrlInfo(POSTGRESQL_CONTAINER.getJdbcUrl()),
                        schema,
                        null);
        postgresCatalog.open();
        CatalogTable catalogTable = postgresCatalog.getTable(tablePathPG);
        // sink tableExists ?
        boolean tableExistsBefore = postgresCatalog.tableExists(tablePathPgSink);
        Assertions.assertFalse(tableExistsBefore);
        // create table
        postgresCatalog.createTable(tablePathPgSink, catalogTable, true);
        boolean tableExistsAfter = postgresCatalog.tableExists(tablePathPgSink);
        Assertions.assertTrue(tableExistsAfter);
        // comment
        final CatalogTable table = postgresCatalog.getTable(tablePathPgSink);
        Assertions.assertEquals(
                table.getTableSchema().getColumns().get(1).getComment(),
                "\"#¥%……&*（）;;',,.\\.``````//'@特殊注释'\\\\'\"");
        // isExistsData ?
        boolean existsDataBefore = postgresCatalog.isExistsData(tablePathPgSink);
        Assertions.assertFalse(existsDataBefore);
        // insert one data
        String customSql =
                "INSERT INTO\n"
                        + "  pg_ide_sink_table_2 (gid,\n"
                        + "    text_col,\n"
                        + "    varchar_col,\n"
                        + "    char_one_col,\n"
                        + "    char_col,\n"
                        + "    boolean_col,\n"
                        + "    smallint_col,\n"
                        + "    integer_col,\n"
                        + "    bigint_col,\n"
                        + "    decimal_col,\n"
                        + "    numeric_col,\n"
                        + "    real_col,\n"
                        + "    double_precision_col,\n"
                        + "    smallserial_col,\n"
                        + "    serial_col,\n"
                        + "    bigserial_col,\n"
                        + "    date_col,\n"
                        + "    timestamp_col,\n"
                        + "    bpchar_col,\n"
                        + "    age,\n"
                        + "    name,\n"
                        + "    point,\n"
                        + "    linestring,\n"
                        + "    polygon_colums,\n"
                        + "    multipoint,\n"
                        + "    multilinestring,\n"
                        + "    multipolygon,\n"
                        + "    geometrycollection,\n"
                        + "    geog,\n"
                        + "    json_col,\n"
                        + "    jsonb_col, \n"
                        + "    xml_col \n"
                        + "  )\n"
                        + "VALUES\n"
                        + "  (\n"
                        + "    '"
                        + 999
                        + "',\n"
                        + "    'Hello World',\n"
                        + "    'Test',\n"
                        + "    'T',\n"
                        + "    'Testing',\n"
                        + "    true,\n"
                        + "    10,\n"
                        + "    100,\n"
                        + "    1000,\n"
                        + "    10.55,\n"
                        + "    8.8888,\n"
                        + "    3.14,\n"
                        + "    3.14159265,\n"
                        + "    1,\n"
                        + "    100,\n"
                        + "    10000,\n"
                        + "    '2023-05-07',\n"
                        + "    '2023-05-07 14:30:00',\n"
                        + "    'Testing',\n"
                        + "    21,\n"
                        + "    'Leblanc',\n"
                        + "    ST_GeomFromText('POINT(-122.3452 47.5925)', 4326),\n"
                        + "    ST_GeomFromText(\n"
                        + "      'LINESTRING(-122.3451 47.5924, -122.3449 47.5923)',\n"
                        + "      4326\n"
                        + "    ),\n"
                        + "    ST_GeomFromText(\n"
                        + "      'POLYGON((-122.3453 47.5922, -122.3453 47.5926, -122.3448 47.5926, -122.3448 47.5922, -122.3453 47.5922))',\n"
                        + "      4326\n"
                        + "    ),\n"
                        + "    ST_GeomFromText(\n"
                        + "      'MULTIPOINT(-122.3459 47.5927, -122.3445 47.5918)',\n"
                        + "      4326\n"
                        + "    ),\n"
                        + "    ST_GeomFromText(\n"
                        + "      'MULTILINESTRING((-122.3463 47.5920, -122.3461 47.5919),(-122.3459 47.5924, -122.3457 47.5923))',\n"
                        + "      4326\n"
                        + "    ),\n"
                        + "    ST_GeomFromText(\n"
                        + "      'MULTIPOLYGON(((-122.3458 47.5925, -122.3458 47.5928, -122.3454 47.5928, -122.3454 47.5925, -122.3458 47.5925)),((-122.3453 47.5921, -122.3453 47.5924, -122.3448 47.5924, -122.3448 47.5921, -122.3453 47.5921)))',\n"
                        + "      4326\n"
                        + "    ),\n"
                        + "    ST_GeomFromText(\n"
                        + "      'GEOMETRYCOLLECTION(POINT(-122.3462 47.5921), LINESTRING(-122.3460 47.5924, -122.3457 47.5924))',\n"
                        + "      4326\n"
                        + "    ),\n"
                        + "    ST_GeographyFromText('POINT(-122.3452 47.5925)'),\n"
                        + "    '{\"key\":\"test\"}',\n"
                        + "    '{\"key\":\"test\"}',\n"
                        + "    '<XX:NewSize>test</XX:NewSize>'\n"
                        + "  )";
        postgresCatalog.executeSql(tablePathPgSink, customSql);
        boolean existsDataAfter = postgresCatalog.isExistsData(tablePathPgSink);
        Assertions.assertTrue(existsDataAfter);
        // truncateTable
        postgresCatalog.truncateTable(tablePathPgSink, true);
        Assertions.assertFalse(postgresCatalog.isExistsData(tablePathPgSink));
        // drop table
        postgresCatalog.dropTable(tablePathPgSink, true);
        Assertions.assertFalse(postgresCatalog.tableExists(tablePathPgSink));
        postgresCatalog.close();
    }

    /**
     * Verifies a full batch read of a PostgreSQL partitioned table. The parent table has a primary
     * key, so the JDBC source splits it with the dynamic splitter and reads the approximate row
     * count from pg_class; a partitioned parent is exposed there with relkind 'p' instead of 'r'.
     * Before the fix the row count lookup returned no row for the parent table and the job failed
     * during split generation.
     */
    @TestTemplate
    public void testPartitionedTableSource(TestContainer container)
            throws IOException, InterruptedException {
        preparePartitionedTables();
        try {
            Container.ExecResult execResult =
                    container.executeJob("/jdbc_postgres_partition_source_and_sink.conf");
            Assertions.assertEquals(
                    0,
                    execResult.getExitCode(),
                    "jdbc_postgres_partition_source_and_sink.conf job run failed.");
            Assertions.assertIterableEquals(
                    querySql("select gid, name from " + PARTITION_SOURCE_TABLE + " order by gid"),
                    querySql("select gid, name from " + PARTITION_SINK_TABLE + " order by gid"));
        } finally {
            executeSQL("truncate table " + PARTITION_SINK_TABLE);
            executeSQL("truncate table " + PARTITION_SOURCE_TABLE);
        }
    }

    private void preparePartitionedTables() {
        executeSQL("DROP TABLE IF EXISTS " + PARTITION_SOURCE_TABLE + " CASCADE");
        executeSQL(PARTITION_SOURCE_DDL);
        executeSQL(
                "CREATE TABLE "
                        + PARTITION_SOURCE_TABLE
                        + "_p0 PARTITION OF "
                        + PARTITION_SOURCE_TABLE
                        + " FOR VALUES FROM (0) TO (1000)");
        executeSQL(
                "CREATE TABLE "
                        + PARTITION_SOURCE_TABLE
                        + "_p1 PARTITION OF "
                        + PARTITION_SOURCE_TABLE
                        + " FOR VALUES FROM (1000) TO (2000)");
        executeSQL(PARTITION_SINK_DDL);
        executeSQL("truncate table " + PARTITION_SINK_TABLE);
        executeSQL("truncate table " + PARTITION_SOURCE_TABLE);
        executeSQL(
                "insert into "
                        + PARTITION_SOURCE_TABLE
                        + " (gid, name) values (1, 'a'), (2, 'b'), (3, 'c'), (1001, 'x'), (1002, 'y'), (1003, 'z')");
    }
}
