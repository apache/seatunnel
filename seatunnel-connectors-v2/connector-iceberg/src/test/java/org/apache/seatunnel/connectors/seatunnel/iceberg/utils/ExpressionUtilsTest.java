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

package org.apache.seatunnel.connectors.seatunnel.iceberg.utils;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExpressionUtilsTest {

    @Test
    public void testSqlToExpression() throws JSQLParserException {
        String sql = "delete from test.a where id = 1";

        Expression expression = ExpressionUtils.convertDeleteSQL(sql);
        Assertions.assertEquals(Expressions.equal("id", 1).toString(), expression.toString());

        sql = "delete from test.a where id != 1";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.notEqual("id", 1).toString(), expression.toString());

        sql = "delete from test.a where id > 1";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.greaterThan("id", 1).toString(), expression.toString());

        sql = "delete from test.a where id >= 1";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(
                Expressions.greaterThanOrEqual("id", 1).toString(), expression.toString());

        sql = "delete from test.a where id < 1";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.lessThan("id", 1).toString(), expression.toString());

        sql = "delete from test.a where id <= 1";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(
                Expressions.lessThanOrEqual("id", 1).toString(), expression.toString());

        sql = "delete from test.a where id is null";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.isNull("id").toString(), expression.toString());

        sql = "delete from test.a where id is not null";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.notNull("id").toString(), expression.toString());

        sql = "delete from test.a where id in (1,2,3)";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.in("id", 1, 2, 3).toString(), expression.toString());

        sql = "delete from test.a where id not in (1,2,3)";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.notIn("id", 1, 2, 3).toString(), expression.toString());

        sql = "delete from test.a where id is true";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.equal("id", true).toString(), expression.toString());

        sql = "delete from test.a where id = 1 and name = a or (age >=1 and age < 1)";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(
                Expressions.or(
                                Expressions.and(
                                        Expressions.equal("id", 1), Expressions.equal("name", "a")),
                                Expressions.and(
                                        Expressions.greaterThanOrEqual("age", 1),
                                        Expressions.lessThan("age", 1)))
                        .toString(),
                expression.toString());

        sql = "delete from test.a where id = 'a'";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.equal("id", "a").toString(), expression.toString());

        sql =
                "delete from test.a where f1 = '2024-01-01' and f2 = '12:00:00.001' and f3 = '2024-01-01 12:00:00.001'";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Delete delete = (Delete) statement;
        Schema schema =
                new Schema(
                        Types.NestedField.optional(1, "f1", Types.DateType.get()),
                        Types.NestedField.optional(2, "f2", Types.TimeType.get()),
                        Types.NestedField.optional(3, "f3", Types.TimestampType.withoutZone()));
        expression = ExpressionUtils.convert(delete.getWhere(), schema);

        Assertions.assertEquals(
                Expressions.and(
                                Expressions.equal("f1", 19723),
                                Expressions.equal("f2", 43200001000L),
                                Expressions.equal("f3", 1704110400001000L))
                        .toString(),
                expression.toString());
    }

    @Test
    public void testSimpleConditions() throws Exception {
        // test integer comparison
        String whereClause1 = "SELECT * FROM t WHERE  age = 30";
        Expression expr1 = ExpressionUtils.parseWhereClauseToIcebergExpression(whereClause1);
        assertEquals(Expressions.equal("age", 30).toString(), expr1.toString());

        // test string comparison
        String whereClause2 = "SELECT * FROM t WHERE name = 'John'";
        Expression expr2 = ExpressionUtils.parseWhereClauseToIcebergExpression(whereClause2);
        assertEquals(Expressions.equal("name", "John").toString(), expr2.toString());

        // test float comparison
        String whereClause3 = "SELECT * FROM t WHERE salary > 50000.5";
        Expression expr3 = ExpressionUtils.parseWhereClauseToIcebergExpression(whereClause3);
        assertEquals(Expressions.greaterThan("salary", 50000.5).toString(), expr3.toString());

        // test boolean comparison
        String whereClause4 = "SELECT * FROM t WHERE is_active is true";
        Expression expr4 = ExpressionUtils.parseWhereClauseToIcebergExpression(whereClause4);
        assertEquals(Expressions.equal("is_active", true).toString(), expr4.toString());
    }

    @Test
    public void testLogicalCombinations() throws Exception {
        // test AND
        String whereClause1 = "SELECT * FROM t WHERE age > 30 AND name = 'John'";
        Expression expr1 = ExpressionUtils.parseWhereClauseToIcebergExpression(whereClause1);
        assertEquals(
                Expressions.and(
                                Expressions.greaterThan("age", 30),
                                Expressions.equal("name", "John"))
                        .toString(),
                expr1.toString());

        // OR
        String whereClause2 = "SELECT * FROM t WHERE salary < 50000 OR is_active is true";
        Expression expr2 = ExpressionUtils.parseWhereClauseToIcebergExpression(whereClause2);
        assertEquals(
                Expressions.or(
                                Expressions.lessThan("salary", 50000),
                                Expressions.equal("is_active", true))
                        .toString(),
                expr2.toString());

        // test combination of AND and OR
        String whereClause3 =
                "SELECT * FROM t WHERE (age > 30 AND name = 'John') OR salary < 50000";
        Expression expr3 = ExpressionUtils.parseWhereClauseToIcebergExpression(whereClause3);
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
                "SELECT * FROM t WHERE ((age > 30 AND name = 'John') OR salary < 50000) AND is_active is true";
        Expression expr1 = ExpressionUtils.parseWhereClauseToIcebergExpression(whereClause1);
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
                "SELECT * FROM t WHERE age > 30 AND (name = 'John' OR (salary < 50000 AND is_active is true))";
        Expression expr2 = ExpressionUtils.parseWhereClauseToIcebergExpression(whereClause2);
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
        String whereClause1 = "SELECT * FROM t WHERE name IS NULL";
        Expression expr1 = ExpressionUtils.parseWhereClauseToIcebergExpression(whereClause1);
        assertEquals(Expressions.isNull("name").toString(), expr1.toString());

        // IS NOT NULL
        String whereClause2 = "SELECT * FROM t WHERE name IS NOT NULL";
        Expression expr2 = ExpressionUtils.parseWhereClauseToIcebergExpression(whereClause2);
        assertEquals(Expressions.notNull("name").toString(), expr2.toString());

        // NOT
        String whereClause3 = "SELECT * FROM t WHERE NOT (age > 30)";
        Expression expr3 = ExpressionUtils.parseWhereClauseToIcebergExpression(whereClause3);
        assertEquals(
                Expressions.not(Expressions.greaterThan("age", 30)).toString(), expr3.toString());

        // IN
        String whereClause4 = "SELECT * FROM t WHERE age IN (30, 40, 50)";
        Expression expr4 = ExpressionUtils.parseWhereClauseToIcebergExpression(whereClause4);
        assertEquals(Expressions.in("age", new Object[] {30, 40, 50}).toString(), expr4.toString());

        // start with
        String whereClause5 = "SELECT * FROM t WHERE name LIKE 'John%'";
        Expression expr5 = ExpressionUtils.parseWhereClauseToIcebergExpression(whereClause5);
        assertEquals(Expressions.startsWith("name", "John%").toString(), expr5.toString());
    }

    @Test
    void parseSelectColumns() {
        String sql = "SELECT id, name, age FROM test.a";
        List<String> columns = ExpressionUtils.parseSelectColumns(sql);
        assertEquals(3, columns.size());
        assertEquals("id", columns.get(0));
        assertEquals("name", columns.get(1));
        assertEquals("age", columns.get(2));

        sql = "SELECT * FROM test.a";
        columns = ExpressionUtils.parseSelectColumns(sql);
        assertEquals(1, columns.size());
        assertEquals("*", columns.get(0));
    }
}
