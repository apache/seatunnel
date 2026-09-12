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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.relational.Column;

import java.util.Optional;

/**
 * Tests MySQL CDC type normalization in {@link MySqlTypeUtils}.
 *
 * <p>This guards the Debezium edge case where a MySQL SET column is reported as {@code SET
 * UNSIGNED}.
 */
public class MySqlTypeUtilsTest {

    /** Verifies that {@code SET UNSIGNED} is normalized onto the regular SET conversion path. */
    @Test
    void testConvertToSeaTunnelColumnSupportsSetUnsigned() {
        Column column = Mockito.mock(Column.class);
        Mockito.when(column.name()).thenReturn("status_flags");
        Mockito.when(column.typeName()).thenReturn("SET UNSIGNED");
        Mockito.when(column.length()).thenReturn(64);
        Mockito.when(column.scale()).thenReturn(Optional.empty());
        Mockito.when(column.defaultValueExpression()).thenReturn(Optional.empty());
        Mockito.when(column.isOptional()).thenReturn(true);

        org.apache.seatunnel.api.table.catalog.Column seatunnelColumn =
                MySqlTypeUtils.convertToSeaTunnelColumn(column, createDbzConnectorConfig());

        Assertions.assertEquals("status_flags", seatunnelColumn.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, seatunnelColumn.getDataType());
        Assertions.assertEquals(64L, seatunnelColumn.getColumnLength());
        Assertions.assertEquals("SET UNSIGNED", seatunnelColumn.getSourceType());
    }

    /** Creates the minimal Debezium connector config needed by {@link MySqlTypeUtils}. */
    private io.debezium.connector.mysql.MySqlConnectorConfig createDbzConnectorConfig() {
        MySqlSourceConfigFactory factory = new MySqlSourceConfigFactory();
        factory.hostname("localhost");
        factory.port(3306);
        factory.username("test");
        factory.password("test");
        factory.databaseList("test_db");
        factory.tableList("test_db.test_table");
        return factory.create(0).getDbzConnectorConfig();
    }
}
