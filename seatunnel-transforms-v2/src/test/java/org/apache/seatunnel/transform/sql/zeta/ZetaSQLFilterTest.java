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

import java.time.Duration;
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

    @Test
    public void testLikeWithWildcardInTheMiddle() throws Exception {
        ZetaSQLFilter filter = createFilter();
        Object[] alice = new Object[] {1, "Alice", 20};
        Object[] bob = new Object[] {2, "Bob", 20};

        for (String pattern : new String[] {"A%ce", "A%ic%e", "A_i_e", "%li%ce", "A%e"}) {
            Expression expr = CCJSqlParserUtil.parseExpression("name LIKE '" + pattern + "'");
            Assertions.assertTrue(
                    filter.executeFilter(expr, alice), "expect Alice to match " + pattern);
            Assertions.assertFalse(
                    filter.executeFilter(expr, bob), "expect Bob not to match " + pattern);
        }

        Expression notLike = CCJSqlParserUtil.parseExpression("name NOT LIKE 'A%ce'");
        Assertions.assertFalse(filter.executeFilter(notLike, alice));
        Assertions.assertTrue(filter.executeFilter(notLike, bob));
    }

    @Test
    public void testLikePatternsThatStillWork() throws Exception {
        ZetaSQLFilter filter = createFilter();
        Object[] alice = new Object[] {1, "Alice", 20};

        for (String pattern : new String[] {"Al%", "%lic%", "_li%"}) {
            Expression expr = CCJSqlParserUtil.parseExpression("name LIKE '" + pattern + "'");
            Assertions.assertTrue(
                    filter.executeFilter(expr, alice), "expect Alice to match " + pattern);
        }
    }

    @Test
    public void testLikeTreatsRegexMetacharactersAndBrokenGroupsAsLiterals() throws Exception {
        ZetaSQLFilter filter = createFilter();
        Object[] alice = new Object[] {1, "Alice", 20};

        Expression dot = CCJSqlParserUtil.parseExpression("'125' LIKE '1.5'");
        Assertions.assertFalse(filter.executeFilter(dot, alice));

        Expression unclosedGroup = CCJSqlParserUtil.parseExpression("name LIKE 'a(b%'");
        Assertions.assertFalse(filter.executeFilter(unclosedGroup, alice));

        // the literal value itself may start and end with a quote: it must not be stripped
        Expression quotedLiteral = CCJSqlParserUtil.parseExpression("name LIKE '''quoted'''");
        Assertions.assertTrue(
                filter.executeFilter(quotedLiteral, new Object[] {1, "'quoted'", 20}));
    }

    @Test
    public void testLikeNullDoesNotThrow() throws Exception {
        ZetaSQLFilter filter = createFilter();
        Object[] alice = new Object[] {1, "Alice", 20};

        Expression likeNull = CCJSqlParserUtil.parseExpression("name LIKE null");
        Assertions.assertFalse(filter.executeFilter(likeNull, alice));

        Expression notLikeNull = CCJSqlParserUtil.parseExpression("name NOT LIKE null");
        Assertions.assertFalse(filter.executeFilter(notLikeNull, alice));
    }

    @Test
    public void testRlikeAndRegexpKeepRegexSemantics() throws Exception {
        ZetaSQLFilter filter = createFilter();
        Object[] alice = new Object[] {1, "Alice", 20};
        Object[] bob = new Object[] {2, "Bob", 20};

        // RLIKE / REGEXP are parsed into LikeExpression too, but they are regular expressions
        Expression rlike = CCJSqlParserUtil.parseExpression("name RLIKE 'A.*'");
        Assertions.assertTrue(filter.executeFilter(rlike, alice));
        Assertions.assertFalse(filter.executeFilter(rlike, bob));

        Expression regexp = CCJSqlParserUtil.parseExpression("name REGEXP '^A.*e$'");
        Assertions.assertTrue(filter.executeFilter(regexp, alice));

        Expression notRlike = CCJSqlParserUtil.parseExpression("name NOT RLIKE 'A.*'");
        Assertions.assertFalse(filter.executeFilter(notRlike, alice));
        Assertions.assertTrue(filter.executeFilter(notRlike, bob));

        // a regex metacharacter must stay a metacharacter here, unlike in LIKE
        Expression metachar = CCJSqlParserUtil.parseExpression("name RLIKE 'A.ice'");
        Assertions.assertTrue(filter.executeFilter(metachar, alice));
    }

    @Test
    public void testLikeWildcardsMatchAcrossNewlines() throws Exception {
        ZetaSQLFilter filter = createFilter();
        Object[] row = new Object[] {1, "a\nb", 20};

        // SQL standard: % and _ match any character, including a line break
        Assertions.assertTrue(
                filter.executeFilter(CCJSqlParserUtil.parseExpression("name LIKE 'a%b'"), row));
        Assertions.assertTrue(
                filter.executeFilter(CCJSqlParserUtil.parseExpression("name LIKE 'a_b'"), row));
    }

    @Test
    public void testLikeWithManyWildcardsDoesNotBacktrackExponentially() throws Exception {
        ZetaSQLFilter filter = createFilter();
        // a regex built from repeated %a would blow up on this value
        String pattern = "%a%a%a%a%a%a%a%a%a%a%b";
        Object[] noMatch = new Object[] {1, repeat('a', 64), 20};
        Expression expr = CCJSqlParserUtil.parseExpression("name LIKE '" + pattern + "'");

        Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(5),
                () -> Assertions.assertFalse(filter.executeFilter(expr, noMatch)));
    }

    @Test
    public void testLikeMatchesOneCodePointPerUnderscore() throws Exception {
        ZetaSQLFilter filter = createFilter();
        String value = "a\uD83D\uDE00b";
        Object[] row = new Object[] {1, value, 20};

        Assertions.assertTrue(
                filter.executeFilter(CCJSqlParserUtil.parseExpression("name LIKE 'a_b'"), row));
        Assertions.assertTrue(
                filter.executeFilter(
                        CCJSqlParserUtil.parseExpression("name LIKE 'a\uD83D\uDE00b'"), row));
        Assertions.assertFalse(
                filter.executeFilter(CCJSqlParserUtil.parseExpression("name LIKE 'a__b'"), row));
    }

    private static String repeat(char c, int times) {
        StringBuilder builder = new StringBuilder(times);
        for (int i = 0; i < times; i++) {
            builder.append(c);
        }
        return builder.toString();
    }
}
