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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.converter;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;
import org.tikv.common.meta.CIStr;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.IntegerType;
import org.tikv.common.types.StringType;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DefaultDataConverterTest {

    private static final long TABLE_ID = 42L;

    @Test
    void convertShouldEmitNullForColumnMissingFromLiveTableInfo() throws Exception {
        // The planned row type still contains "dropped_col" while the live TiKV table info
        // no longer has it (e.g. the column was dropped after job planning).
        TiTableInfo tableInfo = tableInfo();
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "dropped_col"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.LONG_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });
        Object[] values = {7L, "Alice"};

        SeaTunnelRow row = new DefaultDataConverter().convert(values, tableInfo, rowType);

        assertEquals(3, row.getArity());
        assertEquals(7L, row.getField(0));
        assertEquals("Alice", row.getField(1));
        assertNull(row.getField(2));
    }

    @Test
    void convertShouldMapAllColumnsWhenSchemasMatch() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE, BasicType.STRING_TYPE});
        Object[] values = {7L, "Alice"};

        SeaTunnelRow row = new DefaultDataConverter().convert(values, tableInfo(), rowType);

        assertEquals(2, row.getArity());
        assertEquals(7L, row.getField(0));
        assertEquals("Alice", row.getField(1));
    }

    private static TiTableInfo tableInfo() {
        return new TiTableInfo(
                TABLE_ID,
                CIStr.newCIStr("test_table"),
                "utf8mb4",
                "utf8mb4_bin",
                true,
                Arrays.asList(
                        new TiColumnInfo(1L, "id", 0, IntegerType.BIGINT, true),
                        new TiColumnInfo(2L, "name", 1, StringType.VARCHAR, false)),
                Collections.emptyList(),
                "",
                0L,
                2L,
                0L,
                0L,
                null,
                null,
                null,
                0L,
                0L,
                0L,
                null);
    }
}
