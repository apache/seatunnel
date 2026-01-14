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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class SQLLateralViewFunctionsTest {

    private List<SeaTunnelRow> runSqlForAllRows(
            String query, SeaTunnelRowType rowType, Object... values) {
        CatalogTable table = CatalogTableUtil.getCatalogTable("test", rowType);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.singletonMap("query", query));
        SQLTransform transform = new SQLTransform(config, table);
        // Initialize schema to ensure outRowType is available for lateral view processing
        transform.transformTableSchema();
        return transform.transformRow(new SeaTunnelRow(values));
    }

    @Test
    public void testLateralViewExplodeWithSplit() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

        List<SeaTunnelRow> out =
                runSqlForAllRows(
                        "select id, name"
                                + " from dual"
                                + " LATERAL VIEW EXPLODE(SPLIT(name, ',')) AS name",
                        rowType,
                        1,
                        "a,b,c");

        Assertions.assertEquals(3, out.size());
        Assertions.assertEquals(1, out.get(0).getField(0));
        Assertions.assertEquals("a", out.get(0).getField(1));
        Assertions.assertEquals("b", out.get(1).getField(1));
        Assertions.assertEquals("c", out.get(2).getField(1));
    }

    @Test
    public void testLateralViewExplodeWithArrayColumn() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "nums"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, ArrayType.INT_ARRAY_TYPE});

        List<SeaTunnelRow> out =
                runSqlForAllRows(
                        "select id, nums" + " from dual" + " LATERAL VIEW EXPLODE(nums) AS v",
                        rowType,
                        1,
                        (Object) new Object[] {1, 2, 3});

        Assertions.assertEquals(3, out.size());
        Assertions.assertEquals(1, out.get(0).getField(0));
        // Original array column remains as nums, exploded elements are in alias column v.
        Assertions.assertEquals(1, out.get(0).getField(2));
        Assertions.assertEquals(2, out.get(1).getField(2));
        Assertions.assertEquals(3, out.get(2).getField(2));
    }

    @Test
    public void testLateralViewOuterExplodeOnNullArray() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "nums"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, ArrayType.INT_ARRAY_TYPE});

        List<SeaTunnelRow> out =
                runSqlForAllRows(
                        "select id, nums" + " from dual" + " LATERAL VIEW OUTER EXPLODE(nums) AS v",
                        rowType,
                        1,
                        (Object) null);

        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals(1, out.get(0).getField(0));
        // OUTER EXPLODE ensures at least one row with alias column v = null
        Assertions.assertNull(out.get(0).getField(2));
    }

    @Test
    public void testLateralViewOuterExplodeOnEmptyArray() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "nums"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, ArrayType.INT_ARRAY_TYPE});

        List<SeaTunnelRow> out =
                runSqlForAllRows(
                        "select id, nums" + " from dual" + " LATERAL VIEW OUTER EXPLODE(nums) AS v",
                        rowType,
                        1,
                        (Object) new Object[] {});

        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals(1, out.get(0).getField(0));
        // For empty array, OUTER EXPLODE also yields a single row with v = null
        Assertions.assertNull(out.get(0).getField(2));
    }
}
