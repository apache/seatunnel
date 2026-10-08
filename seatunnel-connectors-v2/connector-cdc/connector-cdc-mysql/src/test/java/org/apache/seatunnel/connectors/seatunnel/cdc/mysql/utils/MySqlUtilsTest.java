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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

public class MySqlUtilsTest {

    @Test
    public void testSplitScanQuery() {
        String splitScanSQL =
                MySqlUtils.buildSplitScanQuery(
                        TableId.parse("db1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        false,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM `db1`.`table1` WHERE `id` >= ? AND NOT (`id` = ?) AND `id` <= ?",
                splitScanSQL);

        splitScanSQL =
                MySqlUtils.buildSplitScanQuery(
                        TableId.parse("db1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        true,
                        true);
        Assertions.assertEquals("SELECT * FROM `db1`.`table1`", splitScanSQL);

        splitScanSQL =
                MySqlUtils.buildSplitScanQuery(
                        TableId.parse("db1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        true,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM `db1`.`table1` WHERE `id` <= ? AND NOT (`id` = ?)", splitScanSQL);

        splitScanSQL =
                MySqlUtils.buildSplitScanQuery(
                        TableId.parse("db1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        false,
                        true);
        Assertions.assertEquals("SELECT * FROM `db1`.`table1` WHERE `id` >= ?", splitScanSQL);
    }

    @Test
    public void testCompositePrimaryKeySplitScanQuery() {
        // Test composite primary key with 2 columns: middle split
        String splitScanSQL =
                MySqlUtils.buildSplitScanQuery(
                        TableId.parse("db1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"order_id", "line_no"},
                                new SeaTunnelDataType[] {BasicType.LONG_TYPE, BasicType.INT_TYPE}),
                        false,
                        false);
        // Middle split uses lexicographic tuple comparison:
        // (`order_id` > ? OR (`order_id` = ? AND `line_no` >= ?)) -- lower bound
        // AND NOT (`order_id` = ? AND `line_no` = ?) -- exclude start boundary
        // AND (`order_id` < ? OR (`order_id` = ? AND `line_no` <= ?)) -- upper bound
        // Assert the exact SQL so parenthesization and clause order are locked down.
        Assertions.assertEquals(
                "SELECT * FROM `db1`.`table1` WHERE (`order_id` > ? OR (`order_id` = ? AND `line_no` >= ?))"
                        + " AND NOT (`order_id` = ? AND `line_no` = ?)"
                        + " AND (`order_id` < ? OR (`order_id` = ? AND `line_no` <= ?))",
                splitScanSQL);

        // Test composite primary key: first split (upper bound only)
        splitScanSQL =
                MySqlUtils.buildSplitScanQuery(
                        TableId.parse("db1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"order_id", "line_no"},
                                new SeaTunnelDataType[] {BasicType.LONG_TYPE, BasicType.INT_TYPE}),
                        true,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM `db1`.`table1` WHERE (`order_id` < ? OR (`order_id` = ? AND `line_no` <= ?))"
                        + " AND NOT (`order_id` = ? AND `line_no` = ?)",
                splitScanSQL);

        // Test composite primary key: last split (lower bound only)
        splitScanSQL =
                MySqlUtils.buildSplitScanQuery(
                        TableId.parse("db1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"order_id", "line_no"},
                                new SeaTunnelDataType[] {BasicType.LONG_TYPE, BasicType.INT_TYPE}),
                        false,
                        true);
        Assertions.assertEquals(
                "SELECT * FROM `db1`.`table1` WHERE (`order_id` > ? OR (`order_id` = ? AND `line_no` >= ?))",
                splitScanSQL);

        // Test composite primary key with 3 columns: middle split, verifying the OR chain and
        // placeholder count for N >= 3 (parameter binding previously assumed 2*N-1 placeholders).
        splitScanSQL =
                MySqlUtils.buildSplitScanQuery(
                        TableId.parse("db1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"a", "b", "c"},
                                new SeaTunnelDataType[] {
                                    BasicType.LONG_TYPE, BasicType.INT_TYPE, BasicType.INT_TYPE
                                }),
                        false,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM `db1`.`table1` WHERE (`a` > ? OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` >= ?))"
                        + " AND NOT (`a` = ? AND `b` = ? AND `c` = ?)"
                        + " AND (`a` < ? OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))",
                splitScanSQL);

        // Test composite primary key: full table scan (first and last)
        splitScanSQL =
                MySqlUtils.buildSplitScanQuery(
                        TableId.parse("db1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"order_id", "line_no"},
                                new SeaTunnelDataType[] {BasicType.LONG_TYPE, BasicType.INT_TYPE}),
                        true,
                        true);
        Assertions.assertEquals("SELECT * FROM `db1`.`table1`", splitScanSQL);
    }
}
