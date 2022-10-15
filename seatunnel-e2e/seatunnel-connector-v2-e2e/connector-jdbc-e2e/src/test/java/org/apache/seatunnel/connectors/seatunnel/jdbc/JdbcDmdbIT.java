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

import static org.awaitility.Awaitility.given;

import org.apache.seatunnel.connectors.seatunnel.jdbc.util.JdbcCompareUtil;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class JdbcDmdbIT extends TestSuiteBase implements TestResource {

    private static final String DOCKER_IMAGE = "laglangyue/dmdb8";
    private static final String DRIVER_CLASS = "dm.jdbc.driver.DmDriver";
    private static final String HOST = "e2e_dmdb";
    private static final String URL = "jdbc:dm://%s:5236";
    private static final String USERNAME = "SYSDBA";
    private static final String PASSWORD = "SYSDBA";
    private static final String DATABASE = "SYSDBA";
    private static final String SOURCE_TABLE = "e2e_table_source";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String DM_DRIVER_JAR = "https://repo1.maven.org/maven2/com/dameng/DmJdbcDriver18/8.1.1.193/DmJdbcDriver18-8.1.1.193.jar";

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory = container -> {
        Container.ExecResult extraCommands = container.execInContainer("bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O " + DM_DRIVER_JAR);
        Assertions.assertEquals(0, extraCommands.getExitCode());
    };

    private Connection jdbcConnection;
    private GenericContainer<?> dbServer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        dbServer = new GenericContainer<>(DOCKER_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases(HOST)
                .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)));
        dbServer.setPortBindings(Lists.newArrayList(
                String.format("%s:%s", 5236, 5236)));
        Startables.deepStart(Stream.of(dbServer)).join();
        log.info("Dmdb container started");
        // wait for Dmdb fully start
        Class.forName(DRIVER_CLASS);
        given().ignoreExceptions()
                .await()
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initializeJdbcConnection);
        initializeJdbcTable();
    }

    private void initializeJdbcConnection() throws SQLException {
        jdbcConnection = DriverManager.getConnection(String.format(
                URL, dbServer.getHost()), USERNAME, PASSWORD);
    }

    /**
     * init the table for DM_SERVER, DDL and DML for source and sink
     */
    private void initializeJdbcTable() {
        File file = ContainerUtil.getResourcesFile("/init/dm_init.conf");
        Config config = ConfigFactory.parseFile(file);
        assert config.hasPath("dm_table_source") && config.hasPath("DML") && config.hasPath("dm_table_sink");
        try (Statement statement = jdbcConnection.createStatement()) {
            // source
            String sourceTableDDL = config.getString("dm_table_source");
            statement.execute(sourceTableDDL);
            String insertSQL = config.getString("DML");
            statement.execute(insertSQL);
            // sink
            String sinkTableDDL = config.getString("dm_table_sink");
            statement.execute(sinkTableDDL);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing table failed!", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (dbServer != null) {
            dbServer.close();
        }
    }

    @TestTemplate
    @DisplayName("JDBC-DM end to end test")
    public void testJdbcDmdb(TestContainer container) throws IOException, InterruptedException, SQLException {
        assertHasData(SOURCE_TABLE);
        Container.ExecResult execResult = container.executeJob("/jdbc_dm_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        assertHasData(SINK_TABLE);
        JdbcCompareUtil.compare(jdbcConnection, String.format("select * from %s.%s limit 1", DATABASE, SOURCE_TABLE),
                String.format("select * from %s.%s limit 1", DATABASE, SINK_TABLE),
                "DM_BIT, DM_INT, DM_INTEGER, DM_PLS_INTEGER, DM_TINYINT, DM_BYTE, DM_SMALLINT, DM_BIGINT, DM_NUMERIC, DM_NUMBER, "
                        + "DM_DECIMAL, DM_DEC, DM_REAL, DM_FLOAT, DM_DOUBLE_PRECISION, DM_DOUBLE, DM_CHAR, DM_CHARACTER, DM_VARCHAR, DM_VARCHAR2,"
                        + " DM_TEXT, DM_LONG, DM_LONGVARCHAR, DM_CLOB, DM_TIMESTAMP, DM_DATETIME, DM_DATE, DM_BLOB, DM_BINARY, DM_VARBINARY, DM_LONGVARBINARY");
        clearSinkTable();
    }

    private void assertHasData(String table) {
        try (Statement statement = jdbcConnection.createStatement()) {
            String sql = String.format("select * from %s.%s limit 1", DATABASE, table);
            ResultSet source = statement.executeQuery(sql);
            Assertions.assertTrue(source.next());
        } catch (SQLException e) {
            throw new RuntimeException("test dm server image error", e);
        }
    }

    private void clearSinkTable() {
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE %s.%s", DATABASE, SINK_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("test dm server image error", e);
        }
    }
}
