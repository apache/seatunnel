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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config;

import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the PG-base-backed PostgreSQL source config factory behavior that must stay compatible. */
public class PostgresSourceConfigFactoryTest {

    @Test
    public void testCreateFormatsSchemaQualifiedTableIdentifiers() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.tableList("inventory.orders", "db1.public.customers");

        PostgresSourceConfig sourceConfig = factory.create(0);

        Assertions.assertEquals(
                "inventory.orders,public.customers",
                sourceConfig.getDbzConfiguration().getString("table.include.list"));
    }

    @Test
    public void testCreateRejectsInvalidTableIdentifier() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.tableList("orders");

        Assertions.assertThrows(IllegalArgumentException.class, () -> factory.create(0));
    }

    @Test
    public void shouldDisableDebeziumSnapshotForCommittedOffsetStartup() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.startupOptions(new StartupConfig(StartupMode.COMMITTED_OFFSET, null, null, null));

        Assertions.assertEquals(
                "never", factory.create(0).getDbzConfiguration().getString("snapshot.mode"));
    }

    private PostgresSourceConfigFactory baseFactory() {
        PostgresSourceConfigFactory factory = new PostgresSourceConfigFactory();
        factory.hostname("127.0.0.1");
        factory.port(5432);
        factory.username("user");
        factory.password("pwd");
        factory.originUrl("jdbc:postgresql://127.0.0.1:5432/test");
        factory.databaseList("inventory");
        factory.startupOptions(new StartupConfig(StartupMode.INITIAL, null, null, null));
        factory.stopOptions(new StopConfig(StopMode.NEVER, null, null, null));
        return factory;
    }
}
