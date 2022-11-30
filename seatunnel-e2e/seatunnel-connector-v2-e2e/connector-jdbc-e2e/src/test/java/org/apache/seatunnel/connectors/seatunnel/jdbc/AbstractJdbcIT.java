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
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public abstract class AbstractJdbcIT extends TestSuiteBase implements TestResource {

    protected static final String HOST = "HOST";
    protected GenericContainer<?> dbServer;
    protected JdbcCase jdbcCase;

    abstract JdbcCase getJdbcCase();

    abstract void compareResult() throws SQLException, IOException;

    abstract void clearSinkTable();

    abstract SeaTunnelRow initTestData();

    protected Connection createAndChangeDatabase(Connection connection) {
        //do nothing
        return connection;
    }

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory = container -> {
        Container.ExecResult extraCommands = container.execInContainer("bash", "-c", "mkdir -p /tmp/jars && cd /tmp/jars && curl -O " + jdbcCase.getDriverJar());
        Assertions.assertEquals(0, extraCommands.getExitCode());
    };

    private void getContainer() throws SQLException, MalformedURLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        jdbcCase = this.getJdbcCase();
        dbServer = new GenericContainer<>(jdbcCase.getDockerImage())
            .withNetwork(NETWORK)
            .withNetworkAliases(jdbcCase.getNetworkAliases())
            .withEnv(jdbcCase.getContainerEnv())
            .withLogConsumer(new Slf4jLogConsumer(log));
        dbServer.setPortBindings(Lists.newArrayList(
            String.format("%s:%s", jdbcCase.getLocalPort(), jdbcCase.getPort())));
        Startables.deepStart(Stream.of(dbServer)).join();

        given().ignoreExceptions()
            .await()
            .atMost(360, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                this.initializeJdbcConnection(jdbcCase.getJdbcUrl());
            });
        this.initializeJdbcTable();
    }

    protected Connection initializeJdbcConnection(String jdbcUrl) throws SQLException, ClassNotFoundException, MalformedURLException, InstantiationException, IllegalAccessException {
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{new URL(jdbcCase.getDriverJar())}, AbstractJdbcIT.class.getClassLoader());
        Thread.currentThread().setContextClassLoader(urlClassLoader);
        Driver driver = (Driver) urlClassLoader.loadClass(jdbcCase.getDriverClass()).newInstance();
        Properties props = new Properties();
        props.put("user", jdbcCase.getUserName());
        props.put("password", jdbcCase.getPassword());
        return driver.connect(jdbcUrl.replace(HOST, dbServer.getHost()), props);
    }

    private void batchInsertData() {
        try (Connection connection = initializeJdbcConnection(String.format(jdbcCase.getJdbcTemplate(), jdbcCase.getLocalPort(), jdbcCase.getDataBase()))) {
            connection.setAutoCommit(false);
            try (PreparedStatement preparedStatement = connection.prepareStatement(jdbcCase.getInitDataSql())) {

                for (int index = 0; index < jdbcCase.getSeaTunnelRow().getFields().length; index++) {
                    preparedStatement.setObject(index + 1, jdbcCase.getSeaTunnelRow().getFields()[index]);
                }
                preparedStatement.execute();
            }
            connection.commit();
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new RuntimeException("get connection error", exception);
        }
    }

    private void initializeJdbcTable() {
        try (Connection connection = initializeJdbcConnection(jdbcCase.getJdbcUrl())) {
            Connection newConnection = createAndChangeDatabase(connection);
            Statement statement = newConnection.createStatement();
            String createSource = jdbcCase.getDdlSource();
            String createSink = jdbcCase.getDdlSink();
            statement.execute(createSource);
            if (StringUtils.isNotEmpty(createSink)) {
                statement.execute(createSink);
            }
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new RuntimeException("get connection error", exception);
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
        if (dbServer != null) {
            dbServer.close();
        }
    }

    @TestTemplate
    public void testJdbcDb(TestContainer container) throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult = container.executeJob(jdbcCase.getConfigFile());
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        this.compareResult();
    }

}
