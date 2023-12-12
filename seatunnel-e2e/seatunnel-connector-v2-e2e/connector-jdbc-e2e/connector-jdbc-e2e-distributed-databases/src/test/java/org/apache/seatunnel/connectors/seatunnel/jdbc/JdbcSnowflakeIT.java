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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import net.snowflake.client.jdbc.SnowflakeBasicDataSource;

import java.sql.Connection;

@Disabled("Disabled because it needs user's personal snowflake account to run this test!")
public class JdbcSnowflakeIT extends TestSuiteBase implements TestResource {
    private static final String URL = "jdbc:snowflake://<account_name>.snowflakecomputing.com";
    private static final String USERNAME = "user";
    private static final String PASSWORD = "password";
    private static final String SNOWFLAKE_DRIVER_JAR =
            "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.29/snowflake-jdbc-3.13.29.jar";
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                container.execInContainer(
                        "bash",
                        "-c",
                        "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                + SNOWFLAKE_DRIVER_JAR);
            };

    private Connection connection;

    @TestTemplate
    public void testSnowflake(TestContainer container) throws Exception {
        container.executeExtraCommands(extendedFactory);
        Container.ExecResult execResult =
                container.executeJob("/jdbc_snowflake_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        SnowflakeBasicDataSource dataSource = new SnowflakeBasicDataSource();
        dataSource.setUrl(URL);
        dataSource.setUser(USERNAME);
        dataSource.setPassword(PASSWORD);
        this.connection = dataSource.getConnection();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (connection != null) {
            this.connection.close();
        }
    }
}
