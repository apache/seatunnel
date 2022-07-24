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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

public class JdbcSourceToConsoleIT extends FlinkContainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSourceToConsoleIT.class);
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
        // wait for clickhouse fully start
        Thread.sleep(5000L);
        Class.forName(mysql.getDriverClassName());
        connection = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        initializeJdbcTable();
        batchInsertData();
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

    @SuppressWarnings("checkstyle:MagicNumber")
    private void batchInsertData() throws SQLException {
        String sql = "insert into test(name,age) values(?,?)";
        connection.setAutoCommit(false);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (int i = 0; i < 10; i++) {
            preparedStatement.setString(1, "Mike");
            preparedStatement.setInt(2, 20);
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        connection.commit();
        connection.close();
    }

    @Test
    public void testFakeSourceToJdbcSink() throws SQLException, IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/jdbcsource_to_console.conf");
        Assert.assertEquals(0, execResult.getExitCode());
    }

    @After
    public void closeMysqlContainer() {
        if (mysql != null) {
            mysql.stop();
        }
    }
}
