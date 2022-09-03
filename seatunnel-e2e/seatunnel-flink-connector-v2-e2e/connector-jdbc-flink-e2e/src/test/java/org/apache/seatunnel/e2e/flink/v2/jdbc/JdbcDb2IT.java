package org.apache.seatunnel.e2e.flink.v2.jdbc;

import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class JdbcDb2IT extends FlinkContainer {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDb2IT.class);
    /**
     * <a href="https://hub.docker.com/r/ibmcom/db2">db2 in dockerhub</a>
     */
    private static final String IMAGE = "ibmcom/db2:latest";
    private static final String HOST = "spark_e2e_db2";
    private static final int PORT = 50000;
    private static final String LOCAL_HOST = "localhost";
    private static final int LOCAL_PORT = 50000;
    private static final String USER = "DB2INST1";
    private static final String PASSWORD = "123456";
    private static final String DRIVER = "com.ibm.db2.jcc.DB2Driver";
    private static final String JDBC_URL = String.format("jdbc:db2://%s:%s/testdb", LOCAL_HOST, LOCAL_PORT);

    private static final String SOURCE_TABLE = "e2e_table_source";
    private static final String SINK_TABLE = "e2e_table_sink";

    private GenericContainer<?> server;
    private Connection jdbcConnection;

    @BeforeEach
    public void startDB2Container() throws ClassNotFoundException {
        server = new GenericContainer<>(IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases(HOST)
            .withPrivilegedMode(true)
            .withLogConsumer(new Slf4jLogConsumer(LOG))
            .withEnv("DB2INST1_PASSWORD", "123456")
            .withEnv("DBNAME", "testdb")
            .withEnv("LICENSE", "accept")
        ;
        server.setPortBindings(Lists.newArrayList(String.format("%s:%s", LOCAL_PORT, PORT)));
        Startables.deepStart(Stream.of(server)).join();
        LOG.info("DB2 container started");
        Class.forName(DRIVER);
        given().ignoreExceptions()
            .await()
            .atMost(180, TimeUnit.SECONDS)
            .untilAsserted(this::initializeJdbcConnection);
        initializeJdbcTable();
        LOG.info("db2 init success");
    }

    @AfterEach
    public void closeGreenplumContainer() throws SQLException {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (server != null) {
            server.close();
        }
    }

    private void initializeJdbcConnection() throws SQLException {
        jdbcConnection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
    }

    /**
     * init the table
     */
    private void initializeJdbcTable() {
        URL resource = JdbcDb2IT.class.getResource("/jdbc/init_sql/db2_init.conf");
        if (resource == null) {
            throw new IllegalArgumentException("can't find find file");
        }
        String file = resource.getFile();
        Config config = ConfigFactory.parseFile(new File(file));
        assert config.hasPath("table_source") && config.hasPath("DML") && config.hasPath("table_sink");
        try (Statement statement = jdbcConnection.createStatement()) {
            // source
            String sourceTableDDL = config.getString("table_source");
            statement.execute(sourceTableDDL);
            LOG.info("source DDL success");
            String insertSQL = config.getString("DML");
            statement.execute(insertSQL);
            LOG.info("source DML success");
            // sink
            String sinkTableDDL = config.getString("table_sink");
            statement.execute(sinkTableDDL);
            LOG.info("sink DDL success");
        } catch (SQLException e) {
            throw new RuntimeException("Initializing table failed!", e);
        }
    }

    private void assertHasData(String table) {
        try (Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
            Statement statement = connection.createStatement();
            String sql = String.format("select * from %s.%s limit 1", USER, table);
            ResultSet source = statement.executeQuery(sql);
            Assertions.assertTrue(source.next());
        } catch (SQLException e) {
            throw new RuntimeException("server image error", e);
        }
    }

    @Test
    void pullImageOK() {
        assertHasData(SOURCE_TABLE);
    }

    @Test
    public void testJdbcSourceAndSink() throws IOException, InterruptedException {
        assertHasData(SOURCE_TABLE);
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/jdbc_db2_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        assertHasData(SINK_TABLE);
    }
}
