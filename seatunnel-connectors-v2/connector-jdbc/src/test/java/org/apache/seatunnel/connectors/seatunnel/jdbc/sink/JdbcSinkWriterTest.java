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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.shade.com.zaxxer.hikari.HikariDataSource;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionValidationUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests JDBC sink connection pool validation query customization. */
class JdbcSinkWriterTest {

    /** Verifies that Xugu pools use a validation query compatible with the driver. */
    @Test
    void testApplyConnectionValidationSetsXuguValidationQuery() {
        HikariDataSource dataSource = new HikariDataSource();
        JdbcConnectionConfig jdbcConnectionConfig =
                JdbcConnectionConfig.builder()
                        .driverName(JdbcConnectionValidationUtils.XUGU_DRIVER)
                        .url("jdbc:xugu://localhost:5138/SYSTEM")
                        .build();

        JdbcSinkWriter.applyConnectionValidation(dataSource, jdbcConnectionConfig);

        Assertions.assertEquals(
                JdbcConnectionValidationUtils.XUGU_VALIDATION_QUERY,
                dataSource.getConnectionTestQuery());
        dataSource.close();
    }

    /** Verifies that other drivers keep Hikari's default validation behavior. */
    @Test
    void testApplyConnectionValidationKeepsDefaultDriverValidation() {
        HikariDataSource dataSource = new HikariDataSource();
        JdbcConnectionConfig jdbcConnectionConfig =
                JdbcConnectionConfig.builder()
                        .driverName("org.postgresql.Driver")
                        .url("jdbc:postgresql://localhost:5432/test")
                        .build();

        JdbcSinkWriter.applyConnectionValidation(dataSource, jdbcConnectionConfig);

        Assertions.assertNull(dataSource.getConnectionTestQuery());
        dataSource.close();
    }
}
