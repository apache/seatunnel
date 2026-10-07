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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import org.apache.seatunnel.api.table.factory.SupportSourceDryRunValidation;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.mysql.MySqlConnection;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MySqlIncrementalSourceFactoryTest {
    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new MySqlIncrementalSourceFactory()).optionRule());
    }

    @Test
    public void testSupportsSourceDryRunValidation() {
        Assertions.assertTrue(
                new MySqlIncrementalSourceFactory() instanceof SupportSourceDryRunValidation);
    }

    @Test
    public void testValidateReplicationPrivilegesRequiresBothGrants() {
        MySqlConnection connection = mock(MySqlConnection.class);
        when(connection.userHasPrivileges("REPLICATION SLAVE")).thenReturn(true);
        when(connection.userHasPrivileges("REPLICATION CLIENT")).thenReturn(true);

        Assertions.assertDoesNotThrow(
                () ->
                        MySqlIncrementalSourceFactory.validateReplicationPrivileges(
                                connection, "seatunnel"));
    }

    @Test
    public void testValidateReplicationPrivilegesReportsMissingGrant() {
        MySqlConnection connection = mock(MySqlConnection.class);
        when(connection.userHasPrivileges("REPLICATION SLAVE")).thenReturn(true);
        when(connection.userHasPrivileges("REPLICATION CLIENT")).thenReturn(false);

        SeaTunnelException exception =
                assertThrows(
                        SeaTunnelException.class,
                        () ->
                                MySqlIncrementalSourceFactory.validateReplicationPrivileges(
                                        connection, "seatunnel"));
        Assertions.assertTrue(exception.getMessage().contains("REPLICATION CLIENT"));
        Assertions.assertTrue(exception.getMessage().contains("GRANT REPLICATION CLIENT"));
        Assertions.assertTrue(exception.getMessage().contains("'seatunnel'@'%';"));
    }
}
