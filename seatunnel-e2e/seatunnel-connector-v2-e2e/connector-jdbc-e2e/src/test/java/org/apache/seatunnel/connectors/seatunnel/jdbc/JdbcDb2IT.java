package org.apache.seatunnel.connectors.seatunnel.jdbc;

import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class JdbcDb2IT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDb2IT.class);
    /**
     * <a href="https://hub.docker.com/r/ibmcom/db2">db2 in dockerhub</a>
     */
    private static final String IMAGE = "ibmcom/db2:latest";
    private static final String HOST = "spark_e2e_db2";
    private static final int PORT = 50000;
    private static final int LOCAL_PORT = 50000;
    private static final String USER = "DB2INST1";
    private static final String PASSWORD = "123456";
    private static final String DRIVER = "com.ibm.db2.jcc.DB2Driver";

    private static final String DATABASE = "testdb";
    private static final String SOURCE_TABLE = "E2E_TABLE_SOURCE";
    private static final String SINK_TABLE = "E2E_TABLE_SINK";
    private String jdbcUrl;
    private GenericContainer<?> dbserver;
    private Connection jdbcConnection;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        dbserver = new GenericContainer<>(IMAGE)
            .withNetwork(TestContainer.NETWORK)
            .withNetworkAliases(HOST)
            .withPrivilegedMode(true)
            .withLogConsumer(new Slf4jLogConsumer(LOG))
            .withEnv("DB2INST1_PASSWORD", PASSWORD)
            .withEnv("DBNAME", DATABASE)
            .withEnv("LICENSE", "accept")
        ;
        dbserver.setPortBindings(Lists.newArrayList(String.format("%s:%s", LOCAL_PORT, PORT)));
        Startables.deepStart(Stream.of(dbserver)).join();
        jdbcUrl = String.format("jdbc:db2://%s:%s/%s", dbserver.getHost(), LOCAL_PORT, DATABASE);
        LOG.info("DB2 container started");
        given().ignoreExceptions()
            .await()
            .atMost(180, TimeUnit.SECONDS)
            .untilAsserted(this::initializeJdbcConnection);
        initializeJdbcTable();
    }

    @Override
    public void tearDown() throws Exception {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (dbserver != null) {
            dbserver.close();
        }
    }

    private void initializeJdbcConnection() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        properties.setProperty("password", PASSWORD);
        Driver driver = (Driver) Class.forName(DRIVER).newInstance();
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

    private void assertHasData(String table) throws SQLException {
        try (Statement statement = jdbcConnection.createStatement()) {
            String sql = String.format("select * from \"%s\".%s", USER, table);
            ResultSet source = statement.executeQuery(sql);
            Assertions.assertTrue(source.next(), "result is null when sql is " + sql);
        } catch (SQLException e) {
            throw new RuntimeException("server image error", e);
        }
    }

    @Test
    void pullImageOK() throws SQLException {
        assertHasData(SOURCE_TABLE);
    }

    @TestTemplate
    @DisplayName("JDBC-DM end to end test")
    public void testJdbcSourceAndSink(TestContainer container) throws IOException, InterruptedException, SQLException {
        assertHasData(SOURCE_TABLE);
        Container.ExecResult execResult = container.executeJob("/jdbc_db2_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        assertHasData(SINK_TABLE);
    }
}
