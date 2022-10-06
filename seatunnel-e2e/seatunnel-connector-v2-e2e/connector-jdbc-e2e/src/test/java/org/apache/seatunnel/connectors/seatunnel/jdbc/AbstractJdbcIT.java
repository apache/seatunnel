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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public abstract class AbstractJdbcIT extends TestSuiteBase implements TestResource {

    protected Connection jdbcConnection;
    protected GenericContainer<?> dbServer;
    private JdbcCase jdbcCase;

    abstract JdbcCase getJdbcCase();

    abstract int compareResult() throws SQLException;

    abstract SeaTunnelRow initTestData();

    private void getContainer() throws ClassNotFoundException, SQLException {
        jdbcCase = this.getJdbcCase();
        dbServer = new GenericContainer<>(jdbcCase.getDockerImage())
            .withNetwork(NETWORK)
            .withNetworkAliases(jdbcCase.getHost())
            .withLogConsumer(new Slf4jLogConsumer(log));
        dbServer.setPortBindings(Lists.newArrayList(
            String.format("%s:%s", jdbcCase.getPort(), jdbcCase.getPort())));
        Startables.deepStart(Stream.of(dbServer)).join();
        Class.forName(jdbcCase.getDriverClass());
        given().ignoreExceptions()
            .await()
            .atMost(180, TimeUnit.SECONDS)
            .untilAsserted(this::initializeJdbcConnection);
        initializeJdbcTable();
    }

    protected Connection initializeJdbcConnection() throws SQLException {
        jdbcConnection = DriverManager.getConnection(jdbcCase.getJdbcUrl(), jdbcCase.getUserName(), jdbcCase.getPassword());
        return jdbcConnection;
    }

    private void batchInsertData() throws SQLException {
        try {
            jdbcConnection.setAutoCommit(false);
            try (PreparedStatement preparedStatement = jdbcConnection.prepareStatement(jdbcCase.getInitDataSql())) {

                for (int index = 0; index < jdbcCase.getSeaTunnelRow().getFields().length; index++) {
                    preparedStatement.setObject(index + 1, jdbcCase.getSeaTunnelRow().getFields()[index]);
                }
                preparedStatement.execute();
            }
            jdbcConnection.commit();
        } catch (SQLException e) {
            jdbcConnection.rollback();
            throw e;
        }
    }

    private void initializeJdbcTable() throws SQLException {
        try (Statement statement = jdbcConnection.createStatement()) {
            String createSource = jdbcCase.getDdlSource();
            String createSink = jdbcCase.getDdlSink();
            statement.execute(createSource);
            statement.execute(createSink);
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        this.batchInsertData();
    }

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.getContainer();
    }

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
    public void testJdbcDb(TestContainer container) throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult = container.executeJob(jdbcCase.getConfigFile());
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        Assertions.assertEquals(0, this.compareResult());
    }

}
