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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.utils;

import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.Column;

/**
 * Verifies that the documented {@code int_type_narrowing} option is honored on the MariaDB-CDC
 * path.
 */
public class MariaDbTypeUtilsIntTypeNarrowingTest {

    private static MySqlConnectorConfig config(Boolean intTypeNarrowing) {
        Configuration.Builder builder =
                Configuration.create()
                        .with(MySqlConnectorConfig.SERVER_NAME, "test_server")
                        .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                        .with(MySqlConnectorConfig.USER, "test")
                        .with(MySqlConnectorConfig.PASSWORD, "test");
        if (intTypeNarrowing != null) {
            builder.with("int_type_narrowing", String.valueOf(intTypeNarrowing));
        }
        return new MySqlConnectorConfig(builder.build());
    }

    private static Column tinyint1() {
        return Column.editor()
                .name("flag")
                .type("TINYINT", "TINYINT")
                .jdbcType(java.sql.Types.TINYINT)
                .length(1)
                .optional(true)
                .create();
    }

    @Test
    void tinyint1NarrowsToBooleanWhenEnabled() {
        Assertions.assertEquals(
                BasicType.BOOLEAN_TYPE,
                MariaDbTypeUtils.convertToSeaTunnelColumn(tinyint1(), config(true)).getDataType());
    }

    @Test
    void tinyint1StaysTinyintWhenDisabled() {
        Assertions.assertEquals(
                BasicType.BYTE_TYPE,
                MariaDbTypeUtils.convertToSeaTunnelColumn(tinyint1(), config(false)).getDataType());
    }

    @Test
    void defaultsToNarrowingWhenAbsent() {
        Assertions.assertEquals(
                BasicType.BOOLEAN_TYPE,
                MariaDbTypeUtils.convertToSeaTunnelColumn(tinyint1(), config(null)).getDataType());
    }
}
