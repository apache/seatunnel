/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SQLToIcebergExprConverterTest {

    @Test
    public void testSimpleConditions() throws Exception {
        // test integer comparison
        String whereClause1 = "age = 30";
        Expression expr1 =
                SQLToIcebergExprConverter.parseWhereClauseToIcebergExpression(whereClause1);
        assertEquals(Expressions.equal("age", 30).toString(), expr1.toString());

        // test string comparison
        String whereClause2 = "name = 'John'";
        Expression expr2 =
                SQLToIcebergExprConverter.parseWhereClauseToIcebergExpression(whereClause2);
        assertEquals(Expressions.equal("name", "John").toString(), expr2.toString());

        // test float comparison
        String whereClause3 = "salary > 50000.5";
        Expression expr3 =
                SQLToIcebergExprConverter.parseWhereClauseToIcebergExpression(whereClause3);
        assertEquals(Expressions.greaterThan("salary", 50000.5).toString(), expr3.toString());

        // test boolean comparison
        String whereClause4 = "is_active = true";
        Expression expr4 =
                SQLToIcebergExprConverter.parseWhereClauseToIcebergExpression(whereClause4);
        assertEquals(Expressions.equal("is_active", true).toString(), expr4.toString());
    }

    @Test
    public void testLogicalCombinations() throws Exception {
        // test AND
        String whereClause1 = "age > 30 AND name = 'John'";
        Expression expr1 =
                SQLToIcebergExprConverter.parseWhereClauseToIcebergExpression(whereClause1);
        assertEquals(
                Expressions.and(
                                Expressions.greaterThan("age", 30),
                                Expressions.equal("name", "John"))
                        .toString(),
                expr1.toString());

        // OR
        String whereClause2 = "salary < 50000 OR is_active = true";
        Expression expr2 =
                SQLToIcebergExprConverter.parseWhereClauseToIcebergExpression(whereClause2);
        assertEquals(
                Expressions.or(
                                Expressions.lessThan("salary", 50000),
                                Expressions.equal("is_active", true))
                        .toString(),
                expr2.toString());

        // test combination of AND and OR
        String whereClause3 = "(age > 30 AND name = 'John') OR salary < 50000";
        Expression expr3 =
                SQLToIcebergExprConverter.parseWhereClauseToIcebergExpression(whereClause3);
        assertEquals(
                Expressions.or(
                                Expressions.and(
                                        Expressions.greaterThan("age", 30),
                                        Expressions.equal("name", "John")),
                                Expressions.lessThan("salary", 50000))
                        .toString(),
                expr3.toString());
    }

    @Test
    public void testComplexNestedExpressions() throws Exception {
        // test nested AND and OR
        String whereClause1 =
                "((age > 30 AND name = 'John') OR salary < 50000) AND is_active = true";
        Expression expr1 =
                SQLToIcebergExprConverter.parseWhereClauseToIcebergExpression(whereClause1);
        assertEquals(
                Expressions.and(
                                Expressions.or(
                                        Expressions.and(
                                                Expressions.greaterThan("age", 30),
                                                Expressions.equal("name", "John")),
                                        Expressions.lessThan("salary", 50000)),
                                Expressions.equal("is_active", true))
                        .toString(),
                expr1.toString());

        // test nested AND and OR with multiple levels
        String whereClause2 =
                "age > 30 AND (name = 'John' OR (salary < 50000 AND is_active = true))";
        Expression expr2 =
                SQLToIcebergExprConverter.parseWhereClauseToIcebergExpression(whereClause2);
        assertEquals(
                Expressions.and(
                                Expressions.greaterThan("age", 30),
                                Expressions.or(
                                        Expressions.equal("name", "John"),
                                        Expressions.and(
                                                Expressions.lessThan("salary", 50000),
                                                Expressions.equal("is_active", true))))
                        .toString(),
                expr2.toString());
    }

    @Test
    public void testSpecialScenarios() throws Exception {
        // IS NULL
        String whereClause1 = "name IS NULL";
        Expression expr1 =
                SQLToIcebergExprConverter.parseWhereClauseToIcebergExpression(whereClause1);
        assertEquals(Expressions.isNull("name").toString(), expr1.toString());

        // IS NOT NULL
        String whereClause2 = "name IS NOT NULL";
        Expression expr2 =
                SQLToIcebergExprConverter.parseWhereClauseToIcebergExpression(whereClause2);
        assertEquals(Expressions.notNull("name").toString(), expr2.toString());

        // NOT
        String whereClause3 = "NOT (age > 30)";
        Expression expr3 =
                SQLToIcebergExprConverter.parseWhereClauseToIcebergExpression(whereClause3);
        assertEquals(
                Expressions.not(Expressions.greaterThan("age", 30)).toString(), expr3.toString());

        // IN
        String whereClause4 = "age IN (30, 40, 50)";
        Expression expr4 =
                SQLToIcebergExprConverter.parseWhereClauseToIcebergExpression(whereClause4);
        assertEquals(Expressions.in("age", new Object[] {30, 40, 50}).toString(), expr4.toString());

        // start with
        String whereClause5 = "name LIKE 'John%'";
        Expression expr5 =
                SQLToIcebergExprConverter.parseWhereClauseToIcebergExpression(whereClause5);
        assertEquals(Expressions.startsWith("name", "John%").toString(), expr5.toString());
    }
}
