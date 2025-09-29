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
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import java.util.Arrays;
import java.util.Collections;

class ZetaSQLTypeNestedTypeTest {

    private static Function arr(Expression... expressions) {
        Function function = new Function();
        function.setName("ARRAY");
        function.setParameters(new ExpressionList(Arrays.asList(expressions)));
        return function;
    }

    private static Function map(Expression key, Expression value) {
        Function function = new Function();
        function.setName("MAP");
        function.setParameters(new ExpressionList(Arrays.asList(key, value)));
        return function;
    }

    private ZetaSQLType zeta() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"col"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        return new ZetaSQLType(rowType, Collections.emptyList());
    }

    @Test
    void testArrayOfArrayTypePreserved() {
        // ARRAY(ARRAY(1,2), ARRAY(3,4))
        Function inner1 = arr(new LongValue(1), new LongValue(2));
        Function inner2 = arr(new LongValue(3), new LongValue(4));
        Function outer = arr(inner1, inner2);

        SeaTunnelDataType type = zeta().getExpressionType(outer);
        Assertions.assertEquals(ArrayType.of(ArrayType.INT_ARRAY_TYPE), type);
    }

    @Test
    void testArrayOfMapTypePreserved() {
        // ARRAY(MAP('k',1), MAP('k2',2))
        Function map1 = map(new StringValue("k"), new LongValue(1));
        Function map2 = map(new StringValue("k2"), new LongValue(2));
        Function outer = arr(map1, map2);

        SeaTunnelDataType type = zeta().getExpressionType(outer);
        Assertions.assertEquals(
                ArrayType.of(new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE)), type);
    }

    @Test
    void testMapOfArrayTypePreserved() {
        // MAP('k', ARRAY(1,2))
        Function valueArr = arr(new LongValue(1), new LongValue(2));
        Function map = map(new StringValue("k"), valueArr);

        SeaTunnelDataType type = zeta().getExpressionType(map);
        Assertions.assertEquals(
                new MapType<>(BasicType.STRING_TYPE, ArrayType.INT_ARRAY_TYPE), type);
    }

    @Test
    void testMapOfMapTypePreserved() {
        // MAP('k', Map('k',2))
        Function innerMap = map(new StringValue("k"), new LongValue(2));
        Function outerMap = map(new StringValue("k"), innerMap);

        SeaTunnelDataType type = zeta().getExpressionType(outerMap);
        Assertions.assertEquals(
                new MapType<>(
                        BasicType.STRING_TYPE,
                        new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE)),
                type);
    }
}
