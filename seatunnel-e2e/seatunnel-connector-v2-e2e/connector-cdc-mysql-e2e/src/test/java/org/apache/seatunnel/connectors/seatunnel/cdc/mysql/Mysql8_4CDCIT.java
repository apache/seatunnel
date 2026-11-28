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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class Mysql8_4CDCIT extends AbstractMysqlCDCITBase {

    public Mysql8_4CDCIT() {
        // Initialize the container
        this.MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_4);
        this.inventoryDatabase =
                new UniqueDatabase(
                        MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);
    }

    @Test
    public void testNegotiatedBinlogStatusHasDiagnosticFields() throws Exception {
        Configuration configuration =
                Configuration.create()
                        .with("database.hostname", MYSQL_CONTAINER.getHost())
                        .with("database.port", MYSQL_CONTAINER.getMappedPort(3306))
                        .with("database.user", MYSQL_USER_NAME)
                        .with("database.password", MYSQL_USER_PASSWORD)
                        .build();
        try (MySqlConnection connection =
                MySqlConnectionUtils.createMySqlConnection(configuration)) {
            Assertions.assertEquals(
                    MySqlConnection.BINARY_LOG_STATUS_STATEMENT,
                    connection.binaryLogStatusStatement());
            connection.query(
                    connection.binaryLogStatusStatement(),
                    result -> {
                        Assertions.assertTrue(result.next());
                        Assertions.assertFalse(result.getString("File").isEmpty());
                        Assertions.assertTrue(result.getLong("Position") > 0);
                        Assertions.assertNotNull(result.getString("Executed_Gtid_Set"));
                    });
        }
    }

    @Override
    protected MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my8-4.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }
}
