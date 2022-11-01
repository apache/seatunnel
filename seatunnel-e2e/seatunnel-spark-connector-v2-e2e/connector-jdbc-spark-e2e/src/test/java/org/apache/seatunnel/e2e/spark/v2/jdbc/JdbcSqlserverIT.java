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

package org.apache.seatunnel.e2e.spark.v2.jdbc;

import org.apache.seatunnel.e2e.spark.SparkContainer;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.google.common.collect.Lists;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@Slf4j
public class JdbcSqlserverIT extends SparkContainer {

    private static final String DOCKER_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest";
    private MSSQLServerContainer<?> mssqlServerContainer;
    private static final String THIRD_PARTY_PLUGINS_URL = "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.4.1.jre8/mssql-jdbc-9.4.1.jre8.jar";

    @SuppressWarnings("checkstyle:MagicNumber")
    @BeforeEach
    public void startSqlServerContainer() throws ClassNotFoundException, SQLException {
        mssqlServerContainer = new MSSQLServerContainer<>(DockerImageName.parse(DOCKER_IMAGE))
            .withNetwork(NETWORK)
            .withNetworkAliases("sqlserver")
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)));
        Startables.deepStart(Stream.of(mssqlServerContainer)).join();
        log.info("Sqlserver container started");
        Class.forName(mssqlServerContainer.getDriverClassName());
        Awaitility.given().ignoreExceptions()
            .await()
            .atMost(Duration.ofMinutes(3))
            .untilAsserted(this::initializeJdbcTable);
        batchInsertData();
    }

    private void initializeJdbcTable() {
        try (Connection connection = DriverManager.getConnection(mssqlServerContainer.getJdbcUrl(), mssqlServerContainer.getUsername(), mssqlServerContainer.getPassword())) {
            Statement statement = connection.createStatement();
            String sourceSql = "CREATE TABLE [source] (\n" +
                "  [ids] bigint  NOT NULL,\n" +
                "  [name] text COLLATE Chinese_PRC_CI_AS  NULL,\n" +
                "  [sfzh] varchar(255) COLLATE Chinese_PRC_CI_AS  NULL,\n" +
                "  [sort] int  NULL,\n" +
                "  [dz] nvarchar(255) COLLATE Chinese_PRC_CI_AS  NULL,\n" +
                "  [xchar] char(255) COLLATE Chinese_PRC_CI_AS  NULL,\n" +
                "  [xdecimal] decimal(18)  NULL,\n" +
                "  [xfloat] float(53)  NULL,\n" +
                "  [xnumeric] numeric(18)  NULL,\n" +
                "  [xsmall] smallint  NULL,\n" +
                "  [xbit] bit  NULL,\n" +
                "  [rq] datetime DEFAULT NULL NULL,\n" +
                "  [xrq] smalldatetime  NULL,\n" +
                "  [xreal] real  NULL,\n" +
                "  [ximage] image  NULL\n" +
                ")";
            String sinkSql = "CREATE TABLE [sink] (\n" +
                "  [ids] bigint  NOT NULL,\n" +
                "  [name] text COLLATE Chinese_PRC_CI_AS  NULL,\n" +
                "  [sfzh] varchar(255) COLLATE Chinese_PRC_CI_AS  NULL,\n" +
                "  [sort] int  NULL,\n" +
                "  [dz] nvarchar(255) COLLATE Chinese_PRC_CI_AS  NULL,\n" +
                "  [xchar] char(255) COLLATE Chinese_PRC_CI_AS  NULL,\n" +
                "  [xdecimal] decimal(18)  NULL,\n" +
                "  [xfloat] float(53)  NULL,\n" +
                "  [xnumeric] numeric(18)  NULL,\n" +
                "  [xsmall] smallint  NULL,\n" +
                "  [xbit] bit  NULL,\n" +
                "  [rq] datetime DEFAULT NULL NULL,\n" +
                "  [xrq] smalldatetime  NULL,\n" +
                "  [xreal] real  NULL,\n" +
                "  [ximage] image  NULL\n" +
                ")";
            statement.execute(sourceSql);
            statement.execute(sinkSql);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Sqlserver table failed!", e);
        }
    }

    @SuppressWarnings("checkstyle:RegexpSingleline")
    private void batchInsertData() {
        try (Connection connection = DriverManager.getConnection(mssqlServerContainer.getJdbcUrl(), mssqlServerContainer.getUsername(), mssqlServerContainer.getPassword())) {
            String sql =
                "INSERT INTO [source] ([ids], [name], [sfzh], [sort], [dz], [xchar], [xdecimal], [xfloat], [xnumeric], [xsmall], [xbit], [rq], [xrq], [xreal], [ximage]) " +
                    "VALUES (1504057, '张三', '3ee98c990e2011eda8fd00ff27b3340d', 1, N'3232', 'qwq', 1, 19.1, 2, 1, '0', '2022-07-26 11:58:46.000', '2022-07-26 13:49:00', 2, 0x)";
            Statement statement = connection.createStatement();
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Batch insert data failed!", e);
        }
    }

    @Test
    public void tesSqlserverSourceAndSink() throws SQLException, IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/jdbc/jdbc_sqlserver_source_to_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        // query result
        String sourceSql = "select * from source";
        String sinkSql = "select * from sink";
        List<String> columns = Lists.newArrayList("ids", "name", "sfzh", "sort", "dz", "xchar", "xdecimal", "xfloat", "xnumeric", "xsmall", "xbit", "rq", "xrq", "xreal", "ximage");

        try (Connection connection = DriverManager.getConnection(mssqlServerContainer.getJdbcUrl(), mssqlServerContainer.getUsername(), mssqlServerContainer.getPassword())) {
            Statement sourceStatement = connection.createStatement();
            Statement sinkStatement = connection.createStatement();
            ResultSet sourceResultSet = sourceStatement.executeQuery(sourceSql);
            ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql);
            while (sourceResultSet.next()) {
                if (sinkResultSet.next()) {
                    for (String column : columns) {
                        Object source = sourceResultSet.getObject(column);
                        int sourceIndex = sourceResultSet.findColumn(column);
                        int sinkIndex = sinkResultSet.findColumn(column);
                        Object sink = sinkResultSet.getObject(column);
                        if (!Objects.deepEquals(source, sink)) {
                            InputStream sourceAsciiStream = sourceResultSet.getBinaryStream(sourceIndex);
                            InputStream sinkAsciiStream = sourceResultSet.getBinaryStream(sinkIndex);
                            String sourceValue = IOUtils.toString(sourceAsciiStream, StandardCharsets.UTF_8);
                            String sinkValue = IOUtils.toString(sinkAsciiStream, StandardCharsets.UTF_8);
                            Assertions.assertEquals(sourceValue, sinkValue);
                        }
                        Assertions.assertTrue(true);
                    }
                }
            }
        }
    }

    @AfterEach
    public void closeSqlserverContainer() {
        if (mssqlServerContainer != null) {
            mssqlServerContainer.stop();
        }
    }

    @Override
    protected void executeExtraCommands(GenericContainer<?> container) throws IOException, InterruptedException {
        Container.ExecResult extraCommands = container.execInContainer("bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O " + THIRD_PARTY_PLUGINS_URL);
        Assertions.assertEquals(0, extraCommands.getExitCode());
    }

}
