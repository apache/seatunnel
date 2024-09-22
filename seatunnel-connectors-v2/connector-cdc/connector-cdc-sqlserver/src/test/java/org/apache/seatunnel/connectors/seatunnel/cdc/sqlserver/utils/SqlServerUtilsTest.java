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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

public class SqlServerUtilsTest {
    @Test
    public void testSplitScanQuery() {
        String splitScanSQL =
                SqlServerUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        false,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM [schema1].[table1] WHERE [id] >= ? AND NOT ([id] = ?) AND [id] <= ?",
                splitScanSQL);

        splitScanSQL =
                SqlServerUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        true,
                        true);
        Assertions.assertEquals("SELECT * FROM [schema1].[table1]", splitScanSQL);

        splitScanSQL =
                SqlServerUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        true,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM [schema1].[table1] WHERE [id] <= ? AND NOT ([id] = ?)",
                splitScanSQL);

        splitScanSQL =
                SqlServerUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        false,
                        true);
        Assertions.assertEquals("SELECT * FROM [schema1].[table1] WHERE [id] >= ?", splitScanSQL);
    }
}
