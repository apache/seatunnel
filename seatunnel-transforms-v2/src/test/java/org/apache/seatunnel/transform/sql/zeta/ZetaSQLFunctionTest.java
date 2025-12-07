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
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ZetaSQLFunctionTest {

    private SeaTunnelRowType rowType() {
        return new SeaTunnelRowType(
                new String[] {"id", "name", "age"},
                new SeaTunnelDataType[] {
                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                });
    }

    private ZetaSQLFunction createFunction() {
        SeaTunnelRowType rt = rowType();
        ZetaSQLType type = new ZetaSQLType(rt, Collections.emptyList());
        return new ZetaSQLFunction(rt, type, Collections.emptyList());
    }

    @Test
    public void testComputeForValueLiteralsAndColumns() {
        ZetaSQLFunction function = createFunction();
        Object[] input = new Object[] {1, "Alice", 20};

        Assertions.assertNull(function.computeForValue(new NullValue(), input));

        DateTimeLiteralExpression tsLiteral =
                new DateTimeLiteralExpression(
                        DateTimeLiteralExpression.DateTime.TYPE_TIMESTAMP, "2024-06-15T12:00:00");
        Object ts = function.computeForValue(tsLiteral, input);
        Assertions.assertTrue(ts instanceof LocalDateTime);

        Expression colExpr = new Column("name");
        Assertions.assertEquals("Alice", function.computeForValue(colExpr, input));

        Expression escapedColExpr = new Column("`name`");
        Assertions.assertEquals("Alice", function.computeForValue(escapedColExpr, input));

        Expression boolCol = new Column("true");
        Assertions.assertEquals(true, function.computeForValue(boolCol, input));
    }

    @Test
    public void testExecuteTimeKeyExpr() {
        ZetaSQLFunction function = createFunction();

        Object d = function.executeTimeKeyExpr(ZetaSQLFunction.CURRENT_DATE);
        Object t = function.executeTimeKeyExpr(ZetaSQLFunction.CURRENT_TIME);
        Object ts = function.executeTimeKeyExpr(ZetaSQLFunction.CURRENT_TIMESTAMP);

        Assertions.assertTrue(d instanceof LocalDate);
        Assertions.assertTrue(t instanceof LocalTime);
        Assertions.assertTrue(ts instanceof LocalDateTime);

        Assertions.assertThrows(
                TransformException.class, () -> function.executeTimeKeyExpr("UNSUPPORTED_KEY"));
    }

    @Test
    public void testExecuteCastExpr() {
        ZetaSQLFunction function = createFunction();

        CastExpression castExpression = new CastExpression();
        castExpression.setLeftExpression(new net.sf.jsqlparser.expression.LongValue(1));
        net.sf.jsqlparser.statement.create.table.ColDataType colDataType =
                new net.sf.jsqlparser.statement.create.table.ColDataType();
        colDataType.setDataType("INT");
        castExpression.setColDataType(colDataType);

        Object castResult = function.executeCastExpr(castExpression, 1L);
        Assertions.assertEquals(1, castResult);
    }

    @Test
    public void testExecuteCaseExprWithSwitchValue() {
        ZetaSQLFunction function = createFunction();
        Object[] input = new Object[] {1, "Alice", 20};

        CaseExpression caseExpression = new CaseExpression();
        caseExpression.setSwitchExpression(new Column("age"));

        WhenClause whenClause1 = new WhenClause();
        whenClause1.setWhenExpression(new net.sf.jsqlparser.expression.LongValue(18));
        whenClause1.setThenExpression(new StringValue("young"));

        WhenClause whenClause2 = new WhenClause();
        whenClause2.setWhenExpression(new net.sf.jsqlparser.expression.LongValue(20));
        whenClause2.setThenExpression(new StringValue("adult"));

        caseExpression.setWhenClauses(Arrays.asList(whenClause1, whenClause2));
        caseExpression.setElseExpression(new StringValue("other"));

        Object result = function.executeCaseExpr(caseExpression, input);
        Assertions.assertEquals("adult", ((StringValue) result).getValue());
    }

    @Test
    public void testExecuteCaseExprWithoutSwitchValue() {
        ZetaSQLFunction function = createFunction();
        Object[] input = new Object[] {1, "Alice", 20};

        CaseExpression caseExpression = new CaseExpression();

        WhenClause whenClause = new WhenClause();
        whenClause.setWhenExpression(new net.sf.jsqlparser.expression.LongValue(1));
        whenClause.setThenExpression(new StringValue("match"));

        caseExpression.setWhenClauses(Collections.singletonList(whenClause));
        caseExpression.setElseExpression(new StringValue("other"));

        Object result = function.executeCaseExpr(caseExpression, input);
        Assertions.assertEquals("match", ((StringValue) result).getValue());
    }

    @Test
    public void testMultiIfFunction() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"age"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        ZetaSQLType type = new ZetaSQLType(rt, Collections.emptyList());
        ZetaSQLFunction function = new ZetaSQLFunction(rt, type, Collections.emptyList());
        Object[] input = new Object[] {25};

        net.sf.jsqlparser.expression.Function multiIf = new net.sf.jsqlparser.expression.Function();
        multiIf.setName(ZetaSQLFunction.MULTI_IF);
        List<Expression> args =
                Arrays.asList(
                        new Column("age"),
                        new net.sf.jsqlparser.expression.LongValue(18),
                        new StringValue("adult"),
                        new net.sf.jsqlparser.expression.LongValue(0),
                        new StringValue("other"));
        multiIf.setParameters(new ExpressionList<>(args));

        Object result = function.computeForValue(multiIf, input);
        Assertions.assertEquals("adult", result);
    }

    @Test
    public void testTimezoneExpression() {
        ZetaSQLFunction function = createFunction();
        Object[] input = new Object[] {1, "foo", 20};

        net.sf.jsqlparser.expression.TimezoneExpression tzExpr =
                new net.sf.jsqlparser.expression.TimezoneExpression();
        tzExpr.setLeftExpression(
                new net.sf.jsqlparser.expression.TimestampValue("2024-01-01 00:00:00"));

        ExpressionList<Expression> tzArgs =
                new ExpressionList<>(Collections.singletonList(new StringValue("+08:00")));
        tzExpr.setTimezoneExpressions(tzArgs.getExpressions());

        Object result = function.computeForValue(tzExpr, input);
        Assertions.assertNotNull(result);
    }
}
