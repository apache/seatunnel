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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SQLSystemFunctionsTest {

    private SeaTunnelRow runSql(String query, SeaTunnelRowType rowType, Object... values) {
        CatalogTable table = CatalogTableUtil.getCatalogTable("test", rowType);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.singletonMap("query", query));
        SQLTransform transform = new SQLTransform(config, table);
        List<SeaTunnelRow> out = transform.transformRow(new SeaTunnelRow(values));
        Assertions.assertNotNull(out);
        Assertions.assertFalse(out.isEmpty());
        return out.get(0);
    }

    @Test
    public void testTryCastFunction() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"str_v"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select TRY_CAST(str_v as INT) as v1, TRY_CAST('not_int' as INT) as v2 from dual",
                        rowType,
                        "123");

        Assertions.assertEquals(123, outRow.getField(0));
        Assertions.assertNull(outRow.getField(1));
    }

    @Test
    public void testNullIfFunction() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"a", "b"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.INT_TYPE});

        SeaTunnelRow outRow1 = runSql("select NULLIF(a, b) as r from dual", rowType, 1, 1);
        Assertions.assertNull(outRow1.getField(0));

        SeaTunnelRow outRow2 = runSql("select NULLIF(a, b) as r from dual", rowType, 2, 1);
        Assertions.assertEquals(2, outRow2.getField(0));

        SeaTunnelRow outRow3 = runSql("select NULLIF(a, b) as r from dual", rowType, null, 1);
        Assertions.assertNull(outRow3.getField(0));
    }

    @Test
    public void testMultiIfFunction() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"age"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        List<SeaTunnelRow> results = new ArrayList<>();
        results.add(
                runSql(
                        "select MULTI_IF(age < 18, 'Minor', age < 30, 'Young', 'Adult') as category from dual",
                        rowType,
                        16));
        results.add(
                runSql(
                        "select MULTI_IF(age < 18, 'Minor', age < 30, 'Young', 'Adult') as category from dual",
                        rowType,
                        25));
        results.add(
                runSql(
                        "select MULTI_IF(age < 18, 'Minor', age < 30, 'Young', 'Adult') as category from dual",
                        rowType,
                        40));

        Assertions.assertEquals("Minor", results.get(0).getField(0));
        Assertions.assertEquals("Young", results.get(1).getField(0));
        Assertions.assertEquals("Adult", results.get(2).getField(0));
    }

    @Test
    public void testUuidFunction() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        SeaTunnelRow outRow = runSql("select UUID() as uuid from dual", rowType, 1);

        Object uuidObj = outRow.getField(0);
        Assertions.assertNotNull(uuidObj);
        Assertions.assertTrue(uuidObj instanceof String);
        String uuid = (String) uuidObj;
        Assertions.assertEquals(36, uuid.length());
        Assertions.assertEquals(4, uuid.chars().filter(ch -> ch == '-').count());
    }

    @Test
    public void testCoalesceFunction() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "stringField", "intField"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });

        SeaTunnelRow row1 =
                runSql(
                        "select id, COALESCE(stringField, intField) as result from dual",
                        rowType,
                        1,
                        "test",
                        123);
        Assertions.assertEquals("test", row1.getField(1));
        Assertions.assertTrue(row1.getField(1) instanceof String);

        SeaTunnelRow row2 =
                runSql(
                        "select id, COALESCE(stringField, intField) as result from dual",
                        rowType,
                        1,
                        null,
                        123);
        Assertions.assertEquals("123", row2.getField(1));
        Assertions.assertTrue(row2.getField(1) instanceof String);
    }

    @Test
    public void testIfNullFunction() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "stringField", "intField"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });

        SeaTunnelRow row1 =
                runSql(
                        "select id, IFNULL(stringField, intField) as result from dual",
                        rowType,
                        1,
                        "test",
                        123);
        Assertions.assertEquals("test", row1.getField(1));
        Assertions.assertTrue(row1.getField(1) instanceof String);

        SeaTunnelRow row2 =
                runSql(
                        "select id, IFNULL(stringField, intField) as result from dual",
                        rowType,
                        1,
                        null,
                        123);
        Assertions.assertEquals("123", row2.getField(1));
        Assertions.assertTrue(row2.getField(1) instanceof String);
    }

    @Test
    public void testNestedSystemAndStringFunctions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name", "default_name"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

        // when name is not null, TRIM(COALESCE(name, default_name))
        SeaTunnelRow row1 =
                runSql(
                        "select TRIM(COALESCE(name, default_name)) as res from dual",
                        rowType,
                        " John ",
                        "Default");
        Assertions.assertEquals("John", row1.getField(0));

        // when name is null, use default_name and TRIM to remove spaces
        SeaTunnelRow row2 =
                runSql(
                        "select TRIM(COALESCE(name, default_name)) as res from dual",
                        rowType,
                        null,
                        " Default ");
        Assertions.assertEquals("Default", row2.getField(0));
    }
}
