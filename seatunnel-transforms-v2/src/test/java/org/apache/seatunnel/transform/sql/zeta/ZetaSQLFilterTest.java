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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

import java.util.Collections;

public class ZetaSQLFilterTest {

    private ZetaSQLFilter createFilter() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "age"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });
        ZetaSQLType type = new ZetaSQLType(rowType, Collections.emptyList());
        ZetaSQLFunction function = new ZetaSQLFunction(rowType, type, Collections.emptyList());
        return new ZetaSQLFilter(function, type);
    }

    @Test
    public void testIsConditionExpr() throws Exception {
        ZetaSQLFilter filter = createFilter();
        Expression expr = CCJSqlParserUtil.parseExpression("age > 18 AND name = 'Alice'");
        Assertions.assertTrue(filter.isConditionExpr(expr));

        Expression nonBoolExpr = CCJSqlParserUtil.parseExpression("age + 1");
        Assertions.assertFalse(filter.isConditionExpr(nonBoolExpr));
    }

    @Test
    public void testComparisonAndLogicalFilters() throws Exception {
        ZetaSQLFilter filter = createFilter();
        Expression expr = CCJSqlParserUtil.parseExpression("age >= 18 AND name = 'Alice'");

        Object[] pass = new Object[] {1, "Alice", 20};
        Object[] failByAge = new Object[] {2, "Alice", 17};
        Object[] failByName = new Object[] {3, "Bob", 20};

        Assertions.assertTrue(filter.executeFilter(expr, pass));
        Assertions.assertFalse(filter.executeFilter(expr, failByAge));
        Assertions.assertFalse(filter.executeFilter(expr, failByName));
    }

    @Test
    public void testIsNullAndInExpression() throws Exception {
        ZetaSQLFilter filter = createFilter();

        Expression isNull = CCJSqlParserUtil.parseExpression("name IS NULL");
        Assertions.assertTrue(filter.executeFilter(isNull, new Object[] {1, null, 20}));
        Assertions.assertFalse(filter.executeFilter(isNull, new Object[] {1, "Alice", 20}));

        Expression isNotNull = CCJSqlParserUtil.parseExpression("name IS NOT NULL");
        Assertions.assertFalse(filter.executeFilter(isNotNull, new Object[] {1, null, 20}));
        Assertions.assertTrue(filter.executeFilter(isNotNull, new Object[] {1, "Alice", 20}));

        Expression inExpr = CCJSqlParserUtil.parseExpression("age IN (18, 20, 22)");
        Assertions.assertTrue(filter.executeFilter(inExpr, new Object[] {1, "Alice", 20}));
        Assertions.assertFalse(filter.executeFilter(inExpr, new Object[] {1, "Alice", 19}));

        Expression notInExpr = CCJSqlParserUtil.parseExpression("age NOT IN (18, 20, 22)");
        Assertions.assertFalse(filter.executeFilter(notInExpr, new Object[] {1, "Alice", 20}));
        Assertions.assertTrue(filter.executeFilter(notInExpr, new Object[] {1, "Alice", 19}));
    }

    @Test
    public void testLikeAndNotLikeExpression() throws Exception {
        ZetaSQLFilter filter = createFilter();

        Expression likeExpr = CCJSqlParserUtil.parseExpression("name LIKE 'Al%'");
        Assertions.assertTrue(filter.executeFilter(likeExpr, new Object[] {1, "Alice", 20}));
        Assertions.assertFalse(filter.executeFilter(likeExpr, new Object[] {1, "Bob", 20}));

        Expression notLikeExpr = CCJSqlParserUtil.parseExpression("name NOT LIKE 'Al%'");
        Assertions.assertFalse(filter.executeFilter(notLikeExpr, new Object[] {1, "Alice", 20}));
        Assertions.assertTrue(filter.executeFilter(notLikeExpr, new Object[] {1, "Bob", 20}));
    }

    @Test
    public void testBetweenLikePatterns() throws Exception {
        ZetaSQLFilter filter = createFilter();

        Expression likeExpr = CCJSqlParserUtil.parseExpression("name LIKE '_li%'");
        Assertions.assertTrue(filter.executeFilter(likeExpr, new Object[] {1, "Alice", 20}));
        Assertions.assertFalse(filter.executeFilter(likeExpr, new Object[] {1, "Bob", 20}));
    }
}
