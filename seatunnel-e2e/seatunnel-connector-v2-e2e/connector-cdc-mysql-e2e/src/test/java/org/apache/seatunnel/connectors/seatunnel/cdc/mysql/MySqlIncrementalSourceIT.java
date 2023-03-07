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
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "")
public class MySqlIncrementalSourceIT extends TestSuiteBase implements TestResource {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlIncrementalSourceIT.class);

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);
    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", "mysqluser", "mysqlpw");

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withDatabaseName("seatunnel-test")
                .withUsername("st_user")
                .withPassword("seatunnel")
                .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @Test
    @Disabled("Offline jobs are not currently supported")
    public void testMysql() {
        inventoryDatabase.createAndInitialize();
        LOG.info("-------mysql port:{}", MYSQL_CONTAINER.getDatabasePort());
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        MYSQL_CONTAINER.close();
    }
}
