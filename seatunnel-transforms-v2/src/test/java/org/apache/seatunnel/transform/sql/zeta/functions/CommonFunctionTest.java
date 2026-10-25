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
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CommonFunctionTest {

    @Test
    public void testResolveExpressionTypeForLiteralsAndColumns() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"col_int", "col_str"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

        Assertions.assertNull(CommonFunction.resolveExpressionType(new NullValue(), rowType));

        SeaTunnelDataType<?> doubleType =
                CommonFunction.resolveExpressionType(new DoubleValue("1.23"), rowType);
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, doubleType);

        SeaTunnelDataType<?> smallLongType =
                CommonFunction.resolveExpressionType(new LongValue(100), rowType);
        Assertions.assertEquals(BasicType.INT_TYPE, smallLongType);

        long biggerThanInt = (long) Integer.MAX_VALUE + 1;
        SeaTunnelDataType<?> bigLongType =
                CommonFunction.resolveExpressionType(new LongValue(biggerThanInt), rowType);
        Assertions.assertEquals(BasicType.LONG_TYPE, bigLongType);

        SeaTunnelDataType<?> stringType =
                CommonFunction.resolveExpressionType(new StringValue("abc"), rowType);
        Assertions.assertEquals(BasicType.STRING_TYPE, stringType);

        SeaTunnelDataType<?> columnType =
                CommonFunction.resolveExpressionType(new Column("col_int"), rowType);
        Assertions.assertEquals(BasicType.INT_TYPE, columnType);

        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> CommonFunction.resolveExpressionType(new Column("unknown"), rowType));
    }

    @Test
    public void testResolveExpressionTypeForArrayAndMapFunctionsAndUnsupported() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        Function arrayFunc = new Function();
        arrayFunc.setName("ARRAY");
        arrayFunc.setParameters(
                new ExpressionList<Expression>(Arrays.asList(new LongValue(1), new LongValue(2))));
        SeaTunnelDataType<?> arrayType = CommonFunction.resolveExpressionType(arrayFunc, rowType);
        Assertions.assertTrue(arrayType instanceof ArrayType);
        Assertions.assertEquals(BasicType.INT_TYPE, ((ArrayType) arrayType).getElementType());

        Function mapFunc = new Function();
        mapFunc.setName("MAP");
        mapFunc.setParameters(
                new ExpressionList<Expression>(
                        Arrays.asList(new StringValue("k1"), new LongValue(1))));
        SeaTunnelDataType<?> mapType = CommonFunction.resolveExpressionType(mapFunc, rowType);
        Assertions.assertTrue(mapType instanceof MapType);
        MapType<?, ?> mt = (MapType<?, ?>) mapType;
        Assertions.assertEquals(BasicType.STRING_TYPE, mt.getKeyType());
        Assertions.assertEquals(BasicType.INT_TYPE, mt.getValueType());

        Function unsupportedExpression = new Function();
        unsupportedExpression.setName("UNSUPPORTED_FUNC");
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> CommonFunction.resolveExpressionType(unsupportedExpression, rowType));
    }

    @Test
    public void testUnifyCollectionTypeForNumericArrayAndMap() {
        Assertions.assertEquals(
                BasicType.LONG_TYPE,
                CommonFunction.unifyCollectionType(BasicType.INT_TYPE, BasicType.LONG_TYPE));
        Assertions.assertEquals(
                BasicType.FLOAT_TYPE,
                CommonFunction.unifyCollectionType(BasicType.FLOAT_TYPE, BasicType.SHORT_TYPE));

        Assertions.assertEquals(
                BasicType.INT_TYPE, CommonFunction.unifyCollectionType(null, BasicType.INT_TYPE));
        Assertions.assertEquals(
                BasicType.INT_TYPE,
                CommonFunction.unifyCollectionType(BasicType.VOID_TYPE, BasicType.INT_TYPE));

        ArrayType intArray = ArrayType.INT_ARRAY_TYPE;
        ArrayType longArray = ArrayType.LONG_ARRAY_TYPE;
        SeaTunnelDataType<?> unifiedArray = CommonFunction.unifyCollectionType(intArray, longArray);
        Assertions.assertTrue(unifiedArray instanceof ArrayType);
        Assertions.assertEquals(BasicType.LONG_TYPE, ((ArrayType) unifiedArray).getElementType());

        MapType<?, ?> map1 = new MapType<>(BasicType.INT_TYPE, BasicType.STRING_TYPE);
        MapType<?, ?> map2 = new MapType<>(BasicType.LONG_TYPE, BasicType.STRING_TYPE);
        SeaTunnelDataType<?> unifiedMap = CommonFunction.unifyCollectionType(map1, map2);
        Assertions.assertTrue(unifiedMap instanceof MapType);
        MapType<?, ?> um = (MapType<?, ?>) unifiedMap;
        Assertions.assertEquals(BasicType.LONG_TYPE, um.getKeyType());
        Assertions.assertEquals(BasicType.STRING_TYPE, um.getValueType());

        Assertions.assertEquals(
                BasicType.STRING_TYPE,
                CommonFunction.unifyCollectionType(BasicType.INT_TYPE, BasicType.STRING_TYPE));
    }

    @Test
    public void testIsNumericAndWidenNumeric() {
        List<SeaTunnelDataType<?>> numericTypes =
                Arrays.asList(
                        BasicType.BYTE_TYPE,
                        BasicType.SHORT_TYPE,
                        BasicType.INT_TYPE,
                        BasicType.LONG_TYPE,
                        BasicType.FLOAT_TYPE,
                        BasicType.DOUBLE_TYPE);

        for (SeaTunnelDataType<?> type : numericTypes) {
            Assertions.assertTrue(CommonFunction.isNumeric(type));
        }
        Assertions.assertFalse(CommonFunction.isNumeric(BasicType.STRING_TYPE));

        Assertions.assertEquals(
                BasicType.INT_TYPE,
                CommonFunction.widenNumeric(BasicType.BYTE_TYPE, BasicType.INT_TYPE));
        Assertions.assertEquals(
                BasicType.DOUBLE_TYPE,
                CommonFunction.widenNumeric(BasicType.FLOAT_TYPE, BasicType.DOUBLE_TYPE));
        Assertions.assertEquals(
                BasicType.LONG_TYPE,
                CommonFunction.widenNumeric(BasicType.SHORT_TYPE, BasicType.LONG_TYPE));
    }

    @Test
    public void testGetExpressions() {
        Function function = new Function();
        function.setName("TEST_FUNC");
        ExpressionList<Expression> params =
                new ExpressionList<>(Arrays.asList(new LongValue(1), new StringValue("a")));
        function.setParameters(params);

        List<Expression> expressions = CommonFunction.getExpressions(function);
        Assertions.assertEquals(2, expressions.size());
        Assertions.assertTrue(expressions.get(0) instanceof LongValue);
        Assertions.assertTrue(expressions.get(1) instanceof StringValue);

        Function noParamsFunc = new Function();
        noParamsFunc.setName("TEST_EMPTY");
        Assertions.assertEquals(
                Collections.emptyList(), CommonFunction.getExpressions(noParamsFunc));
    }
}
