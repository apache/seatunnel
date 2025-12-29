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

package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.SQLEngine;
import org.apache.seatunnel.transform.sql.SQLEngineFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.util.Arrays;
import java.util.List;

class ArrayFunctionTest {
    private SQLEngine zeta() {
        return SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
    }

    private SeaTunnelRowType dummyInputType() {
        return new SeaTunnelRowType(
                new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
    }

    private SeaTunnelRow dummyRow() {
        return new SeaTunnelRow(new Object[] {1});
    }

    @Test
    void testNestedArrayEvaluateWithSQLEngine() {
        SQLEngine sql = zeta();
        SeaTunnelRowType inType = dummyInputType();

        String sqlText = "select ARRAY(ARRAY(1,2), ARRAY(3,4)) as a from test";
        sql.init("test", null, inType, sqlText);

        List<SeaTunnelRow> out = sql.transformBySQL(dummyRow(), inType);
        Assertions.assertEquals(1, out.size());

        Object field0 = out.get(0).getField(0);
        Assertions.assertTrue(field0 instanceof Object[], "outer should be array");
        Object[] outer = (Object[]) field0;
        Assertions.assertEquals(2, outer.length);

        Assertions.assertTrue(outer[0] instanceof Object[], "inner[0] should be array");
        Assertions.assertTrue(outer[1] instanceof Object[], "inner[1] should be array");

        Object[] inner1 = (Object[]) outer[0];
        Object[] inner2 = (Object[]) outer[1];
        Assertions.assertEquals(2, inner1.length);
        Assertions.assertEquals(2, inner2.length);

        Assertions.assertEquals(1, ((Number) inner1[0]).intValue());
        Assertions.assertEquals(2, ((Number) inner1[1]).intValue());
        Assertions.assertEquals(3, ((Number) inner2[0]).intValue());
        Assertions.assertEquals(4, ((Number) inner2[1]).intValue());
    }

    @Test
    void testArrayMaxAndMinWithIntegers() {
        Object[] values = new Object[] {1, 3, 2};
        Object max = ArrayFunction.arrayMax(java.util.Collections.singletonList((Object) values));
        Object min = ArrayFunction.arrayMin(java.util.Collections.singletonList((Object) values));

        Assertions.assertEquals(3, max);
        Assertions.assertEquals(1, min);
    }

    @Test
    void testArrayMaxAndMinWithStrings() {
        Object[] values = new Object[] {"a", "c", "b"};
        Object max = ArrayFunction.arrayMax(java.util.Collections.singletonList((Object) values));
        Object min = ArrayFunction.arrayMin(java.util.Collections.singletonList((Object) values));

        Assertions.assertEquals("c", max);
        Assertions.assertEquals("a", min);
    }

    @Test
    void testArrayMaxAndMinWithEmptyOrNullArray() {
        Object[] empty = new Object[] {};
        Assertions.assertNull(
                ArrayFunction.arrayMax(java.util.Collections.singletonList((Object) empty)));
        Assertions.assertNull(
                ArrayFunction.arrayMin(java.util.Collections.singletonList((Object) empty)));
        Assertions.assertNull(
                ArrayFunction.arrayMax(java.util.Collections.singletonList((Object) null)));
        Assertions.assertNull(
                ArrayFunction.arrayMin(java.util.Collections.singletonList((Object) null)));
    }

    @Test
    void testArrayMaxAndMinWithNullElements() {
        Object[] values = new Object[] {null, 3, 2, null, 5};
        Object max = ArrayFunction.arrayMax(java.util.Collections.singletonList((Object) values));
        Object min = ArrayFunction.arrayMin(java.util.Collections.singletonList((Object) values));

        Assertions.assertEquals(5, max);
        Assertions.assertEquals(2, min);
    }

    @Test
    void testArrayMaxAndMinWithAllNullElements() {
        Object[] values = new Object[] {null, null};
        Assertions.assertNull(
                ArrayFunction.arrayMax(java.util.Collections.singletonList((Object) values)));
        Assertions.assertNull(
                ArrayFunction.arrayMin(java.util.Collections.singletonList((Object) values)));
    }

    @Test
    void testArrayMaxAndMinWithNullElementsString() {
        Object[] values = new Object[] {null, "b", null, "a"};
        Object max = ArrayFunction.arrayMax(java.util.Collections.singletonList((Object) values));
        Object min = ArrayFunction.arrayMin(java.util.Collections.singletonList((Object) values));

        Assertions.assertEquals("b", max);
        Assertions.assertEquals("a", min);
    }

    @Test
    void testArrayMaxUnsupportedElementType() {
        Object[] values = new Object[] {true, false};
        Assertions.assertThrows(
                TransformException.class,
                () -> ArrayFunction.arrayMax(java.util.Collections.singletonList((Object) values)));
    }

    @Test
    void testArrayHomogeneousNumeric() {
        List<Object> args = Arrays.asList(1, 2, 3);
        Object[] result = ArrayFunction.array(args);

        Assertions.assertEquals(3, result.length);
        Assertions.assertTrue(result[0] instanceof Integer);
        Assertions.assertEquals(1, result[0]);
        Assertions.assertEquals(2, result[1]);
        Assertions.assertEquals(3, result[2]);
    }

    @Test
    void testArrayNumericPromotion() {
        List<Object> args = Arrays.asList(1, 2L, 3.5f);
        Object[] result = ArrayFunction.array(args);

        // numeric types should be promoted to the widest type (Double here)
        Assertions.assertEquals(3, result.length);
        for (Object o : result) {
            Assertions.assertTrue(o instanceof Number);
        }
        Assertions.assertEquals(1.0d, ((Number) result[0]).doubleValue(), 1e-9);
        Assertions.assertEquals(2.0d, ((Number) result[1]).doubleValue(), 1e-9);
        Assertions.assertEquals(3.5d, ((Number) result[2]).doubleValue(), 1e-9);
    }

    @Test
    void testArrayMixedStringAndNumeric() {
        List<Object> args = Arrays.asList(1, "2", 3);
        Object[] result = ArrayFunction.array(args);

        // mixed non-compatible types should fallback to String representation
        Assertions.assertEquals(3, result.length);
        for (Object o : result) {
            Assertions.assertTrue(o instanceof String);
        }
        Assertions.assertArrayEquals(new Object[] {"1", "2", "3"}, result);
    }

    @Test
    void testArrayWithEmptyArgsReturnsEmptyArray() {
        Object[] result = ArrayFunction.array(java.util.Collections.emptyList());
        Assertions.assertEquals(0, result.length);
    }

    @Test
    void testCastArrayTypeMappingWithLiteralArgs() {
        // ARRAY(1, 2, 3) -> element type INT
        Function function = new Function();
        function.setName("ARRAY");
        function.setParameters(
                new ExpressionList<Expression>(
                        Arrays.asList(new LongValue(1), new LongValue(2), new LongValue(3))));

        SeaTunnelRowType inputType =
                new SeaTunnelRowType(
                        new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        ArrayType resultType = ArrayFunction.castArrayTypeMapping(function, inputType);
        Assertions.assertEquals(BasicType.INT_TYPE, resultType.getElementType());
    }

    @Test
    void testCastArrayTypeMappingWithEmptyArgsDefaultsToString() {
        Function function = new Function();
        function.setName("ARRAY");
        function.setParameters(new ExpressionList<Expression>(java.util.Collections.emptyList()));

        SeaTunnelRowType inputType =
                new SeaTunnelRowType(
                        new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        ArrayType resultType = ArrayFunction.castArrayTypeMapping(function, inputType);
        Assertions.assertEquals(BasicType.STRING_TYPE, resultType.getElementType());
    }

    @Test
    void testGetElementTypeFromRowType() {
        // column "arr" is ARRAY<INT>
        SeaTunnelRowType inputType =
                new SeaTunnelRowType(
                        new String[] {"arr"}, new SeaTunnelDataType[] {ArrayType.INT_ARRAY_TYPE});

        Function function = new Function();
        function.setName("ARRAY_MAX");
        function.setParameters(new ExpressionList<Expression>(Arrays.asList(new Column("arr"))));

        SeaTunnelDataType<?> elementType = ArrayFunction.getElementType(function, inputType);
        Assertions.assertEquals(BasicType.INT_TYPE, elementType);
    }
}
