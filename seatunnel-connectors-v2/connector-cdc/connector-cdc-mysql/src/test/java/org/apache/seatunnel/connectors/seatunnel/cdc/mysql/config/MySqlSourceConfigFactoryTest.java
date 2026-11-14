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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

/**
 * Tests MySQL source configuration behavior required by dynamic binlog table discovery.
 *
 * <p>The covered regression risk is losing Debezium DDL records that are needed for runtime table
 * registration.
 */
public class MySqlSourceConfigFactoryTest {

    /**
     * Verifies that enabling binlog newly-added-table discovery also enables Debezium schema
     * records internally.
     */
    @Test
    public void testBinlogNewlyAddedTableEnablesSchemaChangeRecords() {
        MySqlSourceConfigFactory factory = new MySqlSourceConfigFactory();
        factory.hostname("localhost");
        factory.username("test");
        factory.password("test");
        factory.scanBinlogNewlyAddedTableEnabled(true);

        MySqlSourceConfig sourceConfig = factory.create(0);

        Assertions.assertTrue(
                sourceConfig
                        .getDbzConfiguration()
                        .getBoolean(MySqlSourceConfigFactory.SCHEMA_CHANGE_KEY));
    }

    /**
     * Ensures an internal DDL dependency cannot be disabled through Debezium pass-through options.
     */
    @Test
    public void testBinlogNewlyAddedTableOverridesSchemaChangeProperty() {
        MySqlSourceConfigFactory factory = new MySqlSourceConfigFactory();
        factory.hostname("localhost");
        factory.username("test");
        factory.password("test");
        factory.scanBinlogNewlyAddedTableEnabled(true);
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty(MySqlSourceConfigFactory.SCHEMA_CHANGE_KEY, "false");
        factory.debeziumProperties(debeziumProperties);

        MySqlSourceConfig sourceConfig = factory.create(0);

        Assertions.assertTrue(
                sourceConfig
                        .getDbzConfiguration()
                        .getBoolean(MySqlSourceConfigFactory.SCHEMA_CHANGE_KEY));
    }
}
