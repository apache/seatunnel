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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class JdbcPostgresIdentifierIT extends TestSuiteBase implements TestResource {
    private static final String PG_IMAGE = "postgis/postgis";
    private static final String PG_DRIVER_JAR =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";
    private static final String PG_JDBC_JAR =
            "https://repo1.maven.org/maven2/net/postgis/postgis-jdbc/2.5.1/postgis-jdbc-2.5.1.jar";
    private static final String PG_GEOMETRY_JAR =
            "https://repo1.maven.org/maven2/net/postgis/postgis-geometry/2.5.1/postgis-geometry-2.5.1.jar";
    private static final List<String> PG_CONFIG_FILE_LIST =
            Lists.newArrayList("/jdbc_postgres_ide_source_and_sink.conf");
    private PostgreSQLContainer<?> POSTGRESQL_CONTAINER;
    private static final String PG_SOURCE_DDL =
            "CREATE TABLE IF NOT EXISTS pg_ide_source_table (\n"
                    + "  gid SERIAL PRIMARY KEY,\n"
                    + "  text_col TEXT,\n"
                    + "  varchar_col VARCHAR(255),\n"
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
                    + "  inet_col INET,\n"
                    + "  char_one_col CHAR(1)\n"
                    + ")";
    private static final String PG_SINK_DDL =
            "CREATE TABLE IF NOT EXISTS test.public.\"PG_IDE_SINK_TABLE\" (\n"
                    + "    \"GID\" SERIAL PRIMARY KEY,\n"
                    + "    \"TEXT_COL\" TEXT,\n"
                    + "    \"VARCHAR_COL\" VARCHAR(255),\n"
                    + "    \"CHAR_COL\" CHAR(10),\n"
                    + "    \"BOOLEAN_COL\" bool,\n"
                    + "    \"SMALLINT_COL\" int2,\n"
                    + "    \"INTEGER_COL\" int4,\n"
                    + "    \"BIGINT_COL\" BIGINT,\n"
                    + "    \"DECIMAL_COL\" DECIMAL(10, 2),\n"
                    + "    \"NUMERIC_COL\" NUMERIC(8, 4),\n"
                    + "    \"REAL_COL\" float4,\n"
                    + "    \"DOUBLE_PRECISION_COL\" float8,\n"
                    + "    \"SMALLSERIAL_COL\" SMALLSERIAL,\n"
                    + "    \"SERIAL_COL\" SERIAL,\n"
                    + "    \"BIGSERIAL_COL\" BIGSERIAL,\n"
                    + "    \"DATE_COL\" DATE,\n"
                    + "    \"TIMESTAMP_COL\" TIMESTAMP,\n"
                    + "    \"BPCHAR_COL\" BPCHAR(10),\n"
                    + "    \"AGE\" int4 NOT NULL,\n"
                    + "    \"NAME\" varchar(255) NOT NULL,\n"
                    + "    \"POINT\" varchar(2000) NULL,\n"
                    + "    \"LINESTRING\" varchar(2000) NULL,\n"
                    + "    \"POLYGON_COLUMS\" varchar(2000) NULL,\n"
                    + "    \"MULTIPOINT\" varchar(2000) NULL,\n"
                    + "    \"MULTILINESTRING\" varchar(2000) NULL,\n"
                    + "    \"MULTIPOLYGON\" varchar(2000) NULL,\n"
                    + "    \"GEOMETRYCOLLECTION\" varchar(2000) NULL,\n"
                    + "    \"GEOG\" varchar(2000) NULL,\n"
                    + "    \"INET_COL\" INET NULL,\n"
                    + "    \"CHAR_ONE_COL\" CHAR(1) NULL\n"
                    + "  )";

    private static final String SOURCE_SQL =
            "select \n"
                    + "gid,\n"
                    + "text_col,\n"
                    + "varchar_col,\n"
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
                    + "inet_col,\n"
                    + "char_one_col\n"
                    + " from pg_ide_source_table";
    private static final String SINK_SQL =
            "SELECT\n"
                    + "  \"GID\",\n"
                    + "  \"TEXT_COL\",\n"
                    + "  \"VARCHAR_COL\",\n"
                    + "  \"CHAR_COL\",\n"
                    + "  \"BOOLEAN_COL\",\n"
                    + "  \"SMALLINT_COL\",\n"
                    + "  \"INTEGER_COL\",\n"
                    + "  \"BIGINT_COL\",\n"
                    + "  \"DECIMAL_COL\",\n"
                    + "  \"NUMERIC_COL\",\n"
                    + "  \"REAL_COL\",\n"
                    + "  \"DOUBLE_PRECISION_COL\",\n"
                    + "  \"SMALLSERIAL_COL\",\n"
                    + "  \"SERIAL_COL\",\n"
                    + "  \"BIGSERIAL_COL\",\n"
                    + "  \"DATE_COL\",\n"
                    + "  \"TIMESTAMP_COL\",\n"
                    + "  \"BPCHAR_COL\",\n"
                    + "  \"AGE\",\n"
                    + "  \"NAME\",\n"
                    + "  CAST(\"POINT\" AS GEOMETRY) AS POINT,\n"
                    + "  CAST(\"LINESTRING\" AS GEOMETRY) AS LINESTRING,\n"
                    + "  CAST(\"POLYGON_COLUMS\" AS GEOMETRY) AS POLYGON_COLUMS,\n"
                    + "  CAST(\"MULTIPOINT\" AS GEOMETRY) AS MULTIPOINT,\n"
                    + "  CAST(\"MULTILINESTRING\" AS GEOMETRY) AS MULTILINESTRING,\n"
                    + "  CAST(\"MULTIPOLYGON\" AS GEOMETRY) AS MULTILINESTRING,\n"
                    + "  CAST(\"GEOMETRYCOLLECTION\" AS GEOMETRY) AS GEOMETRYCOLLECTION,\n"
                    + "  CAST(\"GEOG\" AS GEOGRAPHY) AS GEOG,\n"
                    + "  \"INET_COL\",\n"
                    + "  \"CHAR_ONE_COL\"\n"
                    + "FROM\n"
                    + "  \"PG_IDE_SINK_TABLE\";";

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + PG_DRIVER_JAR
                                        + " && curl -O "
                                        + PG_JDBC_JAR
                                        + " && curl -O "
                                        + PG_GEOMETRY_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode());
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

    @TestTemplate
    public void testAutoGenerateSQL(TestContainer container)
            throws IOException, InterruptedException {
        for (String CONFIG_FILE : PG_CONFIG_FILE_LIST) {
            Container.ExecResult execResult = container.executeJob(CONFIG_FILE);
            Assertions.assertEquals(0, execResult.getExitCode());
            Assertions.assertIterableEquals(querySql(SOURCE_SQL), querySql(SINK_SQL));
            executeSQL("truncate table \"PG_IDE_SINK_TABLE\"");
            log.info(CONFIG_FILE + " e2e test completed");
        }
    }

    private void initializeJdbcTable() {
        try (Connection connection = getJdbcConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(PG_SOURCE_DDL);
            statement.execute(PG_SINK_DDL);
            for (int i = 1; i <= 10; i++) {
                statement.addBatch(
                        "INSERT INTO\n"
                                + "  pg_ide_source_table (gid,\n"
                                + "    text_col,\n"
                                + "    varchar_col,\n"
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
                                + "    inet_col,\n"
                                + "    char_one_col\n"
                                + "  )\n"
                                + "VALUES\n"
                                + "  (\n"
                                + "    '"
                                + i
                                + "',\n"
                                + "    'Hello World',\n"
                                + "    'Test',\n"
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
                                + "    '192.168.1.1',\n"
                                + "    'T'\n"
                                + "  )");
            }

            statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRESQL_CONTAINER.getJdbcUrl(),
                POSTGRESQL_CONTAINER.getUsername(),
                POSTGRESQL_CONTAINER.getPassword());
    }

    private List<List<Object>> querySql(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getObject(i));
                }
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
}
