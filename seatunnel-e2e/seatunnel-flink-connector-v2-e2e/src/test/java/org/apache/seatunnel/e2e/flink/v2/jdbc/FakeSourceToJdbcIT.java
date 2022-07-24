package org.apache.seatunnel.e2e.flink.v2.jdbc;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Stream;

public class FakeSourceToJdbcIT extends FlinkContainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(FakeSourceToJdbcIT.class);
    private MySQLContainer<?> mysql;
    private Connection connection;

    @SuppressWarnings("checkstyle:MagicNumber")
    @Before
    public void startMysqlContainer() throws InterruptedException, ClassNotFoundException, SQLException {
        mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                .withNetwork(NETWORK)
                .withNetworkAliases("jdbc")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER));
        mysql.setPortBindings(Lists.newArrayList("3306:3306"));
        Startables.deepStart(Stream.of(mysql)).join();
        LOGGER.info("Jdbc container started");
        Thread.sleep(5000L);
        Class.forName(mysql.getDriverClassName());
        connection = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        initializeJdbcTable();
    }

    private void initializeJdbcTable() throws SQLException {
        Statement statement = connection.createStatement();
        String sql = "CREATE TABLE test (\n" +
                "  name varchar(255) NOT NULL,\n" +
                "  age int NOT NULL\n" +
                ")";
        statement.executeUpdate(sql);
        statement.close();
    }

    @Test
    public void testFakeSourceToJdbcSink() throws SQLException, IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/fakesource_to_jdbc.conf");
        Assert.assertEquals(0, execResult.getExitCode());
        // query result
        String sql = "select * from test";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        List<String> result = Lists.newArrayList();
        while (resultSet.next()) {
            result.add(resultSet.getString("name"));
        }
        Assert.assertFalse(result.isEmpty());
    }

    @After
    public void closeMysqlContainer() {
        if (mysql != null) {
            mysql.stop();
        }
    }
}
