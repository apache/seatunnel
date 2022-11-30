/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import com.teradata.jdbc.TeraDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.sql.Connection;
import java.sql.Statement;

@Disabled("Disabled because it needs user's personal teradata account to run this test!")
public class JdbcTeradataIT extends TestSuiteBase implements TestResource {
    private static final String HOST = "1.2.3.4";
    private static final String PORT = "1025";
    private static final String USERNAME = "dbc";
    private static final String PASSWORD = "dbc";
    private static final String DATABASE = "test";
    private static final String SINK_TABLE = "sink_table";
    private static final String TERADATA_DRIVER_JAR = "https://repo1.maven.org/maven2/com/teradata/jdbc/terajdbc4/17.20.00.12/terajdbc4-17.20.00.12.jar";
    private final ContainerExtendedFactory extendedFactory = container -> {
        container.execInContainer("bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O " + TERADATA_DRIVER_JAR);
    };

    private Connection connection;

    @TestTemplate
    public void testTeradata(TestContainer container) throws Exception {
        container.executeExtraCommands(extendedFactory);
        Container.ExecResult execResult = container.executeJob("/jdbc_teradata_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        clearSinkTable();
    }

    private void clearSinkTable() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("delete from %s", SINK_TABLE));
        } catch (Exception e) {
            throw new RuntimeException("Test teradata server failed!", e);
        }
    }

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        TeraDataSource teraDataSource = new TeraDataSource();
        teraDataSource.setDSName(HOST);
        teraDataSource.setDbsPort(PORT);
        teraDataSource.setUser(USERNAME);
        teraDataSource.setPassword(PASSWORD);
        teraDataSource.setDATABASE(DATABASE);
        this.connection = teraDataSource.getConnection();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (connection != null) {
            this.connection.close();
        }
    }
}
