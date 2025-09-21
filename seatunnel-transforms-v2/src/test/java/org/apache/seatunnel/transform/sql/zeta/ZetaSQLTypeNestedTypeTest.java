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

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import java.util.Arrays;
import java.util.Collections;

class ZetaSQLTypeNestedTypeTest {

    private static Function arr(net.sf.jsqlparser.expression.Expression... exprs) {
        Function f = new Function();
        f.setName("ARRAY");
        f.setParameters(new ExpressionList(Arrays.asList(exprs)));
        return f;
    }

    private static Function map(
            net.sf.jsqlparser.expression.Expression k, net.sf.jsqlparser.expression.Expression v) {
        Function f = new Function();
        f.setName("MAP");
        f.setParameters(new ExpressionList(Arrays.asList(k, v)));
        return f;
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

        SeaTunnelDataType t = zeta().getExpressionType(outer);
        Assertions.assertTrue(t instanceof ArrayType);
        ArrayType outerArr = (ArrayType) t;

        SeaTunnelDataType inner = outerArr.getElementType();
        Assertions.assertEquals(ArrayType.INT_ARRAY_TYPE, inner);
        ArrayType innerArr = (ArrayType) inner;

        Assertions.assertEquals(BasicType.INT_TYPE, innerArr.getElementType());
    }

    @Test
    void testArrayOfMapTypePreserved() {
        // ARRAY(MAP('k',1), MAP('k2',2))
        Function m1 = map(new StringValue("k"), new LongValue(1));
        Function m2 = map(new StringValue("k2"), new LongValue(2));
        Function outer = arr(m1, m2);

        SeaTunnelDataType t = zeta().getExpressionType(outer);
        Assertions.assertTrue(t instanceof ArrayType);
        ArrayType arrType = (ArrayType) t;

        SeaTunnelDataType elem = arrType.getElementType();
        Assertions.assertTrue(elem instanceof MapType);
        MapType mapType = (MapType) elem;

        Assertions.assertEquals(BasicType.STRING_TYPE, mapType.getKeyType());
        Assertions.assertEquals(BasicType.INT_TYPE, mapType.getValueType());
    }

    @Test
    void testMapOfArrayTypePreserved() {
        // MAP('k', ARRAY(1,2))
        Function valueArr = arr(new LongValue(1), new LongValue(2));
        Function m = map(new StringValue("k"), valueArr);

        SeaTunnelDataType t = zeta().getExpressionType(m);
        Assertions.assertTrue(t instanceof MapType);
        MapType mapType = (MapType) t;

        Assertions.assertEquals(BasicType.STRING_TYPE, mapType.getKeyType());
        Assertions.assertEquals(ArrayType.INT_ARRAY_TYPE, mapType.getValueType());
        ArrayType valueArray = (ArrayType) mapType.getValueType();
        Assertions.assertEquals(BasicType.INT_TYPE, valueArray.getElementType());
    }
}
