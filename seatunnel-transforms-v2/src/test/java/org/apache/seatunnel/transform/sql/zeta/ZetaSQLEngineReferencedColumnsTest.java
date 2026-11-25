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

package org.apache.seatunnel.transform.sql.zeta;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.sql.SQLOutputSlot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Covers the query analysis that schema-change translation relies on: which input columns a query
 * references outside of star projections, how every output column is derived, and which WHERE
 * comparisons are rejected once operand types no longer match.
 */
public class ZetaSQLEngineReferencedColumnsTest {

    private static final SeaTunnelRowType INPUT =
            new SeaTunnelRowType(
                    new String[] {"id", "name", "age", "c_row", "tags"},
                    new SeaTunnelDataType[] {
                        BasicType.LONG_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.INT_TYPE,
                        new SeaTunnelRowType(
                                new String[] {"c_inner"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}),
                        ArrayType.STRING_ARRAY_TYPE
                    });

    private static final SeaTunnelRowType STRING_AGE_INPUT =
            new SeaTunnelRowType(
                    new String[] {"id", "name", "age"},
                    new SeaTunnelDataType[] {
                        BasicType.LONG_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                    });

    private static ZetaSQLEngine engine(SeaTunnelRowType input, String sql) {
        ZetaSQLEngine engine = new ZetaSQLEngine();
        engine.init("test", "test", input, sql);
        return engine;
    }

    @Test
    public void testStarOnlyReferencesNothing() {
        Assertions.assertTrue(
                engine(INPUT, "select * from test").referencedInputColumns().isEmpty());
    }

    @Test
    public void testSelectItemsAndWhereReferences() {
        ZetaSQLEngine engine =
                engine(
                        INPUT,
                        "select id, upper(name) as n, `age` + 1 as a1, c_row.c_inner as inner_value,"
                                + " true as flag from test where age > 0");
        Assertions.assertEquals(
                new LinkedHashSet<>(Arrays.asList("id", "name", "age", "c_row")),
                new LinkedHashSet<>(engine.referencedInputColumns()));
    }

    @Test
    public void testLateralViewReferences() {
        ZetaSQLEngine engine =
                engine(INPUT, "select id, tags from test LATERAL VIEW EXPLODE(tags) AS tag");
        Assertions.assertEquals(
                new LinkedHashSet<>(Arrays.asList("id", "tags")),
                new LinkedHashSet<>(engine.referencedInputColumns()));
    }

    @Test
    public void testDescribeOutputSlotsFollowsTypeMappingOrder() {
        ZetaSQLEngine engine = engine(INPUT, "select *, id as uid, upper(name) as n from test");
        List<SQLOutputSlot> slots = engine.describeOutputSlots();
        SeaTunnelRowType outRowType = engine.typeMapping(new ArrayList<>());

        Assertions.assertEquals(outRowType.getTotalFields(), slots.size());
        for (int i = 0; i < 5; i++) {
            Assertions.assertEquals(SQLOutputSlot.Kind.STAR, slots.get(i).getKind());
            Assertions.assertEquals(INPUT.getFieldName(i), slots.get(i).getName());
            Assertions.assertEquals(
                    Collections.singletonList(INPUT.getFieldName(i)),
                    slots.get(i).getReferencedInputColumns());
            Assertions.assertEquals(0, slots.get(i).getSelectItemIndex());
        }
        Assertions.assertEquals(SQLOutputSlot.Kind.REFERENCE, slots.get(5).getKind());
        Assertions.assertEquals("uid", slots.get(5).getName());
        Assertions.assertEquals(
                Collections.singletonList("id"), slots.get(5).getReferencedInputColumns());
        Assertions.assertEquals(1, slots.get(5).getSelectItemIndex());
        Assertions.assertEquals(SQLOutputSlot.Kind.EXPRESSION, slots.get(6).getKind());
        Assertions.assertEquals("n", slots.get(6).getName());
        Assertions.assertEquals(
                Collections.singletonList("name"), slots.get(6).getReferencedInputColumns());
        for (int i = 0; i < slots.size(); i++) {
            Assertions.assertEquals(outRowType.getFieldName(i), slots.get(i).getName());
        }
    }

    @Test
    public void testDescribeOutputSlotsAppendsLateralViewAlias() {
        ZetaSQLEngine engine =
                engine(INPUT, "select id, tags from test LATERAL VIEW EXPLODE(tags) AS tag");
        List<SQLOutputSlot> slots = engine.describeOutputSlots();
        SeaTunnelRowType outRowType = engine.typeMapping(new ArrayList<>());

        Assertions.assertEquals(outRowType.getTotalFields(), slots.size());
        Assertions.assertEquals(3, slots.size());
        Assertions.assertEquals(SQLOutputSlot.Kind.REFERENCE, slots.get(0).getKind());
        Assertions.assertEquals(SQLOutputSlot.Kind.REFERENCE, slots.get(1).getKind());
        Assertions.assertEquals(SQLOutputSlot.Kind.LATERAL_VIEW, slots.get(2).getKind());
        Assertions.assertEquals("tag", slots.get(2).getName());
        Assertions.assertEquals(
                Collections.singletonList("tags"), slots.get(2).getReferencedInputColumns());
    }

    @Test
    public void testValidateFilterTypesRejectsIncompatibleOrderingComparisons() {
        engine(INPUT, "select id from test where age > 0").validateFilterTypes();
        engine(INPUT, "select id from test where age > 0 and name like 'a%'").validateFilterTypes();
        engine(STRING_AGE_INPUT, "select id from test where age = 0").validateFilterTypes();
        engine(STRING_AGE_INPUT, "select id from test where length(age) > 0").validateFilterTypes();
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        engine(STRING_AGE_INPUT, "select id from test where age > 0")
                                .validateFilterTypes());
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        engine(STRING_AGE_INPUT, "select id from test where id > 0 and 1 <= age")
                                .validateFilterTypes());
    }
}
