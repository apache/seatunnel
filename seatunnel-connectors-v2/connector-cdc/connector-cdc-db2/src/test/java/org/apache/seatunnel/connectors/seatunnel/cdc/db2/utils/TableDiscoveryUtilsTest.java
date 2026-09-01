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

package org.apache.seatunnel.connectors.seatunnel.cdc.db2.utils;

import org.apache.seatunnel.common.utils.SeaTunnelException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Arrays;
import java.util.Collections;

/**
 * Covers the explicit table validation that keeps DB2 CDC startup fail-fast for non-captured
 * tables.
 */
class TableDiscoveryUtilsTest {

    /**
     * Explicitly configured tables must not disappear silently when Debezium only reports the
     * capture-enabled subset.
     */
    @Test
    void shouldFailWhenConfiguredTableIsMissingFromCaptureSet() {
        SeaTunnelException exception =
                Assertions.assertThrows(
                        SeaTunnelException.class,
                        () ->
                                TableDiscoveryUtils.validateExplicitCaptureTables(
                                        Arrays.asList(
                                                "testdb.DB2INST1.CUSTOMERS",
                                                "testdb.DB2INST1.ORDERS"),
                                        Collections.singletonList(
                                                new TableId("", "DB2INST1", "CUSTOMERS"))));

        Assertions.assertTrue(exception.getMessage().contains("testdb.DB2INST1.ORDERS"));
    }

    /**
     * Startup validation should ignore the catalog/database segment because Debezium Db2 capture
     * metadata uses empty-catalog table ids for the captured tables.
     */
    @Test
    void shouldMatchConfiguredTablesAfterDroppingDatabaseSegment() {
        Assertions.assertDoesNotThrow(
                () ->
                        TableDiscoveryUtils.validateExplicitCaptureTables(
                                Collections.singletonList("testdb.DB2INST1.CUSTOMERS"),
                                Collections.singletonList(
                                        new TableId("", "DB2INST1", "CUSTOMERS"))));
    }
}
