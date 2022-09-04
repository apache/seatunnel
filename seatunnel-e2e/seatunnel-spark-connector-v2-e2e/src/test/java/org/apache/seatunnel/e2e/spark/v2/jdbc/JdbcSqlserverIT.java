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
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.google.common.collect.Lists;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class JdbcSqlserverIT extends SparkContainer {

    private MSSQLServerContainer<?> mssqlServerContainer;

    @SuppressWarnings("checkstyle:MagicNumber")
    @BeforeEach
    public void startPostgreSqlContainer() throws ClassNotFoundException, SQLException {
        mssqlServerContainer = new MSSQLServerContainer<>(DockerImageName.parse("mcr.microsoft.com/mssql/server:2022-latest"))
            .withNetwork(NETWORK)
            .withNetworkAliases("sqlserver")
            .withLogConsumer(new Slf4jLogConsumer(log));
        Startables.deepStart(Stream.of(mssqlServerContainer)).join();
        log.info("Sqlserver container started");
        Class.forName(mssqlServerContainer.getDriverClassName());
        Awaitility.given().ignoreExceptions()
            .await()
            .atMost(Duration.ofMinutes(1))
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
        String sql = "select * from sink";
        try (Connection connection = DriverManager.getConnection(mssqlServerContainer.getJdbcUrl(), mssqlServerContainer.getUsername(), mssqlServerContainer.getPassword())) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            List<String> result = Lists.newArrayList();
            while (resultSet.next()) {
                result.add(resultSet.getString("ids"));
            }
            Assertions.assertFalse(result.isEmpty());
        }
    }

    @AfterEach
    public void closeSqlserverContainer() {
        if (mssqlServerContainer != null) {
            mssqlServerContainer.stop();
        }
    }

}
