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

package org.apache.seatunnel.e2e.flink.v2.jdbc;

import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

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
public class JdbcDmdbIT extends FlinkContainer {

    private static final String DOCKER_IMAGE = "laglangyue/dmdb8";
    private static final String DRIVER_CLASS = "dm.jdbc.driver.DmDriver";
    private static final String HOST = "flink_e2e_dmdb";
    private static final String LOCAL_HOST = "localhost";
    private static final String URL = "jdbc:dm://" + LOCAL_HOST + ":5236";
    private static final String USERNAME = "SYSDBA";
    private static final String PASSWORD = "SYSDBA";
    private static final String DATABASE = "SYSDBA";
    private static final String SOURCE_TABLE = "e2e_table_source";
    private static final String SINK_TABLE = "e2e_table_sink";
    private Connection jdbcConnection;
    private GenericContainer<?> dbServer;

    @BeforeEach
    public void startDmdbContainer() throws ClassNotFoundException, SQLException {
        dbServer = new GenericContainer<>(DOCKER_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases(HOST)
            .withLogConsumer(new Slf4jLogConsumer(log));
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
        jdbcConnection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
    }

    /**
     * init the table for DM_SERVER, DDL and DML for source and sink
     */
    private void initializeJdbcTable() {
        java.net.URL resource = FlinkContainer.class.getResource("/jdbc/init_sql/dm_init.conf");
        if (resource == null) {
            throw new IllegalArgumentException("can't find find file");
        }
        String file = resource.getFile();
        Config config = ConfigFactory.parseFile(new File(file));
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

    private void assertHasData(String table) {
        try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {
            Statement statement = connection.createStatement();
            String sql = String.format("select * from %s.%s limit 1", DATABASE, table);
            ResultSet source = statement.executeQuery(sql);
            Assertions.assertTrue(source.next());
        } catch (SQLException e) {
            throw new RuntimeException("test dm server image error", e);
        }
    }

    @AfterEach
    public void closeDmdbContainer() throws SQLException {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (dbServer != null) {
            dbServer.close();
        }
    }

    @Test
    @DisplayName("JDBC-DM container can be pull")
    public void testDMDBImage() {
        assertHasData(SOURCE_TABLE);
    }

    @Test
    @DisplayName("flink JDBC-DM test")
    public void testJdbcDmdbSourceAndSink() throws IOException, InterruptedException, SQLException {
        assertHasData(SOURCE_TABLE);
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/jdbc_dm_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        assertHasData(SINK_TABLE);
    }

}
