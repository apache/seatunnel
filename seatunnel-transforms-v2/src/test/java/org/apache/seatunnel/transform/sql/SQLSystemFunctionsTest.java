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
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private SeaTunnelRowType runSqlReturnType(String query, SeaTunnelRowType rowType) {
        CatalogTable table = CatalogTableUtil.getCatalogTable("test", rowType);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.singletonMap("query", query));
        SQLTransform transform = new SQLTransform(config, table);
        return transform.getProducedCatalogTable().getSeaTunnelRowType();
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
    public void testIfFunction() {
        SeaTunnelRowType ageRowType =
                new SeaTunnelRowType(
                        new String[] {"age"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        SeaTunnelRow minorRow =
                runSql(
                        "select IF(age < 18, 'Minor', 'Adult') as category from dual",
                        ageRowType,
                        16);
        Assertions.assertEquals("Minor", minorRow.getField(0));

        SeaTunnelRow adultRow =
                runSql(
                        "select IF(age < 18, 'Minor', 'Adult') as category from dual",
                        ageRowType,
                        40);
        Assertions.assertEquals("Adult", adultRow.getField(0));

        // A string branch widens the result to string, the other branch follows CAST semantics
        SeaTunnelRow mixedRow =
                runSql("select IF(age < 18, 'Minor', age) as category from dual", ageRowType, 40);
        Assertions.assertEquals("40", mixedRow.getField(0));

        // Numeric branches widen to the wider type
        SeaTunnelRow numericRow =
                runSql("select IF(age < 18, 1, 2.5) as r from dual", ageRowType, 40);
        Assertions.assertEquals(2.5, numericRow.getField(0));

        SeaTunnelRowType flagRowType =
                new SeaTunnelRowType(
                        new String[] {"flag"}, new SeaTunnelDataType[] {BasicType.BOOLEAN_TYPE});

        // Boolean column condition
        SeaTunnelRow trueRow =
                runSql("select IF(flag, 'Yes', 'No') as r from dual", flagRowType, Boolean.TRUE);
        Assertions.assertEquals("Yes", trueRow.getField(0));

        // Null condition is treated as false
        SeaTunnelRow nullFlagRow =
                runSql(
                        "select IF(flag, 'Yes', 'No') as r from dual",
                        flagRowType,
                        new Object[] {null});
        Assertions.assertEquals("No", nullFlagRow.getField(0));

        // Null branch value stays null
        SeaTunnelRow nullBranchRow =
                runSql("select IF(flag, 'Yes', NULL) as r from dual", flagRowType, Boolean.FALSE);
        Assertions.assertNull(nullBranchRow.getField(0));

        // Both branches NULL keeps the result null
        SeaTunnelRow bothNullRow =
                runSql("select IF(flag, NULL, NULL) as r from dual", flagRowType, Boolean.TRUE);
        Assertions.assertNull(bothNullRow.getField(0));

        // A non-boolean condition is an error
        SeaTunnelRowType nameRowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        Assertions.assertThrows(
                TransformException.class,
                () -> runSql("select IF(name, 'a', 'b') as r from dual", nameRowType, "Alice"));

        // Nested IF: the outer branch is itself an IF, resolved recursively
        SeaTunnelRow nestedRow =
                runSql(
                        "select IF(age < 18, IF(age < 12, 'Child', 'Teenager'), 'Adult') as r"
                                + " from dual",
                        ageRowType,
                        15);
        Assertions.assertEquals("Teenager", nestedRow.getField(0));

        SeaTunnelRow nestedAdultRow =
                runSql(
                        "select IF(age < 18, IF(age < 12, 'Child', 'Teenager'), 'Adult') as r"
                                + " from dual",
                        ageRowType,
                        40);
        Assertions.assertEquals("Adult", nestedAdultRow.getField(0));
    }

    @Test
    public void testIfFunctionPredicateForms() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name", "age"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});

        // IS NULL condition follows the filter evaluator, null value selects the true branch
        SeaTunnelRow nullNameRow =
                runSql(
                        "select IF(name IS NULL, 'missing', 'present') as r from dual",
                        rowType,
                        null,
                        16);
        Assertions.assertEquals("missing", nullNameRow.getField(0));

        SeaTunnelRow presentNameRow =
                runSql(
                        "select IF(name IS NULL, 'missing', 'present') as r from dual",
                        rowType,
                        "Alice",
                        16);
        Assertions.assertEquals("present", presentNameRow.getField(0));

        // IN condition
        SeaTunnelRow inRow =
                runSql(
                        "select IF(age IN (15, 16), 'yes', 'no') as r from dual",
                        rowType,
                        "Bob",
                        15);
        Assertions.assertEquals("yes", inRow.getField(0));

        SeaTunnelRow notInRow =
                runSql(
                        "select IF(age IN (15, 16), 'yes', 'no') as r from dual",
                        rowType,
                        "Bob",
                        20);
        Assertions.assertEquals("no", notInRow.getField(0));

        // Parenthesized comparison
        SeaTunnelRow parenRow =
                runSql(
                        "select IF((age < 18), 'minor', 'adult') as r from dual",
                        rowType,
                        "Bob",
                        15);
        Assertions.assertEquals("minor", parenRow.getField(0));

        // Parenthesized boolean column keeps the value path, null is treated as false
        SeaTunnelRowType flagRowType =
                new SeaTunnelRowType(
                        new String[] {"flag"}, new SeaTunnelDataType[] {BasicType.BOOLEAN_TYPE});
        SeaTunnelRow nullParenFlagRow =
                runSql(
                        "select IF((flag), 'y', 'n') as r from dual",
                        flagRowType,
                        new Object[] {null});
        Assertions.assertEquals("n", nullParenFlagRow.getField(0));
    }

    @Test
    public void testIfFunctionLazyBranches() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"flag", "age"},
                        new SeaTunnelDataType[] {BasicType.BOOLEAN_TYPE, BasicType.INT_TYPE});

        // The false branch, which would error on a non-boolean condition if evaluated,
        // must not be evaluated when the condition is true
        SeaTunnelRow lazyTrueRow =
                runSql(
                        "select IF(flag, 'ok', IF(age, 'a', 'b')) as r from dual",
                        rowType,
                        Boolean.TRUE,
                        40);
        Assertions.assertEquals("ok", lazyTrueRow.getField(0));

        SeaTunnelRow lazyFalseRow =
                runSql(
                        "select IF(flag, IF(age, 'a', 'b'), 'safe') as r from dual",
                        rowType,
                        Boolean.FALSE,
                        40);
        Assertions.assertEquals("safe", lazyFalseRow.getField(0));
    }

    @Test
    public void testIfFunctionIdentityPreservedValues() {
        SeaTunnelRowType bytesRowType =
                new SeaTunnelRowType(
                        new String[] {"flag", "data"},
                        new SeaTunnelDataType[] {
                            BasicType.BOOLEAN_TYPE, PrimitiveByteArrayType.INSTANCE
                        });

        // A BYTES branch is returned unchanged, not re-encoded through a string round trip
        byte[] raw = new byte[] {1, -2, 3, -4, 5};
        SeaTunnelRow bytesRow =
                runSql(
                        "select IF(flag, data, NULL) as r from dual",
                        bytesRowType,
                        Boolean.TRUE,
                        raw.clone());
        Assertions.assertTrue(bytesRow.getField(0) instanceof byte[]);
        Assertions.assertArrayEquals(raw, (byte[]) bytesRow.getField(0));

        // A MAP branch of equal key/value types is returned as-is
        MapType<String, Integer> mapType = new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE);
        SeaTunnelRowType mapRowType =
                new SeaTunnelRowType(
                        new String[] {"flag", "metrics"},
                        new SeaTunnelDataType[] {BasicType.BOOLEAN_TYPE, mapType});
        Map<String, Integer> metrics = new HashMap<>();
        metrics.put("a", 1);
        SeaTunnelRow mapRow =
                runSql(
                        "select IF(flag, metrics, NULL) as r from dual",
                        mapRowType,
                        Boolean.TRUE,
                        metrics);
        Assertions.assertSame(metrics, mapRow.getField(0));

        // A decimal branch value keeps its exact scale and value
        SeaTunnelRowType decimalRowType =
                new SeaTunnelRowType(
                        new String[] {"flag", "d1"},
                        new SeaTunnelDataType[] {BasicType.BOOLEAN_TYPE, new DecimalType(10, 8)});
        SeaTunnelRow decimalRow =
                runSql(
                        "select IF(flag, d1, 1) as r from dual",
                        decimalRowType,
                        Boolean.TRUE,
                        new BigDecimal("12.34567890"));
        Assertions.assertEquals(new BigDecimal("12.34567890"), decimalRow.getField(0));
        Assertions.assertEquals(
                new DecimalType(18, 8),
                runSqlReturnType("select IF(flag, d1, 1) as r from dual", decimalRowType)
                        .getFieldType(0));
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
