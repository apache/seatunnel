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

import org.apache.seatunnel.connectors.seatunnel.jdbc.util.JdbcCompareUtil;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JdbcDb2IT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDb2IT.class);
    /**
     * <a href="https://hub.docker.com/r/ibmcom/db2">db2 in dockerhub</a>
     */
    private static final String IMAGE = "ibmcom/db2";
    private static final String HOST = "e2e_db2";
    private static final int PORT = 50000;
    private static final int LOCAL_PORT = 50000;
    private static final String USER = "db2inst1";
    private static final String PASSWORD = "123456";
    public static final String DB2_DRIVER_JAR = "https://repo1.maven.org/maven2/com/ibm/db2/jcc/db2jcc/db2jcc4/db2jcc-db2jcc4.jar";

    private static final String DATABASE = "E2E";
    private static final String SOURCE_TABLE = "E2E_TABLE_SOURCE";
    private static final String SINK_TABLE = "E2E_TABLE_SINK";
    private String jdbcUrl;
    private Db2Container db2;
    private Connection jdbcConnection;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory = container -> {
        Container.ExecResult extraCommands = container.execInContainer("bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O " + DB2_DRIVER_JAR);
        Assertions.assertEquals(0, extraCommands.getExitCode());
    };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        db2 = new Db2Container(IMAGE)
            .withExposedPorts(PORT)
            .withNetwork(NETWORK)
            .withNetworkAliases(HOST)
            .withDatabaseName(DATABASE)
            .withUsername(USER)
            .withPassword(PASSWORD)
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
            .acceptLicense();
        db2.setPortBindings(Lists.newArrayList(String.format("%s:%s", LOCAL_PORT, PORT)));
        jdbcUrl = String.format("jdbc:db2://%s:%s/%s", db2.getHost(), LOCAL_PORT, DATABASE);
        LOG.info("DB2 container started");
        db2.start();
        initializeJdbcConnection();
        initializeJdbcTable();
    }

    @Override
    public void tearDown() throws Exception {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (db2 != null) {
            db2.close();
        }
    }

    private void initializeJdbcConnection() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        properties.setProperty("password", PASSWORD);
        Driver driver = (Driver) Class.forName(db2.getDriverClassName()).newInstance();
        jdbcConnection = driver.connect(jdbcUrl, properties);
        Statement statement = jdbcConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select 1 from SYSSTAT.TABLES");
        Assertions.assertTrue(resultSet.next());
        resultSet.close();
        statement.close();
    }

    /**
     * init the table
     */
    private void initializeJdbcTable() {
        URL resource = JdbcDb2IT.class.getResource("/init/db2_init.conf");
        if (resource == null) {
            throw new IllegalArgumentException("can't find find file");
        }
        String file = resource.getFile();
        Config config = ConfigFactory.parseFile(new File(file));
        assert config.hasPath("table_source") && config.hasPath("DML") && config.hasPath("table_sink");
        try (Statement statement = jdbcConnection.createStatement()) {
            // source
            LOG.info("source DDL start");
            String sourceTableDDL = config.getString("table_source");
            statement.execute(sourceTableDDL);
            LOG.info("source DML start");
            String insertSQL = config.getString("DML");
            statement.execute(insertSQL);
            LOG.info("sink DDL start");
            String sinkTableDDL = config.getString("table_sink");
            statement.execute(sinkTableDDL);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing table failed!", e);
        }
    }

    private void assertHasData(String table) {
        try (Statement statement = jdbcConnection.createStatement()) {
            String sql = String.format("select * from \"%s\".%s", USER, table);
            ResultSet source = statement.executeQuery(sql);
            Assertions.assertTrue(source.next(), "result is null when sql is " + sql);
        } catch (SQLException e) {
            throw new RuntimeException("server image error", e);
        }
    }

    @Test
    void pullImageOK() {
        assertHasData(SOURCE_TABLE);
    }

    @TestTemplate
    @DisplayName("JDBC-Db2 end to end test")
    public void testJdbcSourceAndSink(TestContainer container) throws IOException, InterruptedException, SQLException {
        assertHasData(SOURCE_TABLE);
        Container.ExecResult execResult = container.executeJob("/jdbc_db2_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        assertHasData(SINK_TABLE);
        JdbcCompareUtil.compare(jdbcConnection, String.format("select * from \"%s\".%s", USER, SOURCE_TABLE),
            String.format("select * from \"%s\".%s", USER, SINK_TABLE),
            "COL_BOOLEAN, COL_INT, COL_INTEGER, COL_SMALLINT, COL_BIGINT, COL_DECIMAL, COL_DEC," +
                "COL_NUMERIC, COL_NUMBER, COL_REAL, COL_FLOAT,COL_DOUBLE_PRECISION, COL_DOUBLE, COL_DECFLOAT, COL_CHAR, COL_VARCHAR," +
                "COL_LONG_VARCHAR, COL_GRAPHIC, COL_VARGRAPHIC, COL_LONG_VARGRAPHIC");
        clearSinkTable();
    }

    private void clearSinkTable() {
        try (Statement statement = jdbcConnection.createStatement()) {
            String truncate = String.format("delete from \"%s\".%s where 1=1;", USER, SINK_TABLE);
            statement.execute(truncate);
        } catch (SQLException e) {
            throw new RuntimeException("test db2 server image error", e);
        }
    }
}
