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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

import lombok.SneakyThrows;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

public class ExpressionUtils {
    private static final DateTimeFormatter LOCAL_DATE_TIME_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .append(ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(ISO_LOCAL_TIME)
                    .toFormatter();

    public static List<String> parseSelectColumns(String selectQuery) {
        if (StringUtils.isNotBlank(selectQuery)) {
            try {
                Statement statement = CCJSqlParserUtil.parse(selectQuery);
                PlainSelect select = (PlainSelect) statement;
                if (CollectionUtils.isNotEmpty(select.getSelectItems())) {
                    return select.getSelectItems().stream()
                            .map(selectItem -> selectItem.toString())
                            .collect(Collectors.toList());
                }
            } catch (JSQLParserException e) {
                throw new RuntimeException("Failed to parse select columns: " + e.getMessage());
            }
        }
        return new ArrayList<>();
    }

    public static Expression parseWhereClauseToIcebergExpression(String selectQuery)
            throws JSQLParserException {
        // use the JsqlParser to parse the where clause
        Statement statement = CCJSqlParserUtil.parse(selectQuery);
        PlainSelect select = (PlainSelect) statement;
        return convert(select.getWhere(), null);
    }

    public static Expression convertDeleteSQL(String sql) throws JSQLParserException {
        Statement statement = CCJSqlParserUtil.parse(sql);
        Delete delete = (Delete) statement;
        return convert(delete.getWhere(), null);
    }

    public static Expression convert(net.sf.jsqlparser.expression.Expression condition) {
        return convert(condition, null);
    }

    public static Expression convert(
            net.sf.jsqlparser.expression.Expression condition, org.apache.iceberg.Schema schema) {
        if (condition == null) {
            return Expressions.alwaysTrue();
        }

        if (condition instanceof AndExpression) {
            return Expressions.and(
                    convert(((AndExpression) condition).getLeftExpression(), schema),
                    convert(((AndExpression) condition).getRightExpression(), schema));
        }
        if (condition instanceof OrExpression) {
            return Expressions.or(
                    convert(((OrExpression) condition).getLeftExpression(), schema),
                    convert(((OrExpression) condition).getRightExpression(), schema));
        }
        if (condition instanceof Parenthesis) {
            return convert(((Parenthesis) condition).getExpression(), schema);
        }

        if (condition instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) condition;
            Column column = (Column) equalsTo.getLeftExpression();
            Object value =
                    schema == null
                            ? convertValueExpression(equalsTo.getRightExpression())
                            : convertValueExpression(
                                    equalsTo.getRightExpression(),
                                    schema.findField(column.getColumnName()));
            return Expressions.equal(column.getColumnName(), value);
        }
        if (condition instanceof NotEqualsTo) {
            NotEqualsTo notEqualsTo = (NotEqualsTo) condition;
            Column column = (Column) notEqualsTo.getLeftExpression();
            Object value =
                    schema == null
                            ? convertValueExpression(notEqualsTo.getRightExpression())
                            : convertValueExpression(
                                    notEqualsTo.getRightExpression(),
                                    schema.findField(column.getColumnName()));
            return Expressions.notEqual(column.getColumnName(), value);
        }
        if (condition instanceof NotExpression) {
            NotExpression expr = (NotExpression) condition;
            return Expressions.not(convert(expr.getExpression(), null));
        }
        if (condition instanceof GreaterThan) {
            GreaterThan greaterThan = (GreaterThan) condition;
            Column column = (Column) greaterThan.getLeftExpression();
            Object value =
                    schema == null
                            ? convertValueExpression(greaterThan.getRightExpression())
                            : convertValueExpression(
                                    greaterThan.getRightExpression(),
                                    schema.findField(column.getColumnName()));
            return Expressions.greaterThan(column.getColumnName(), value);
        }
        if (condition instanceof GreaterThanEquals) {
            GreaterThanEquals greaterThanEquals = (GreaterThanEquals) condition;
            Column column = (Column) greaterThanEquals.getLeftExpression();
            Object value =
                    schema == null
                            ? convertValueExpression(greaterThanEquals.getRightExpression())
                            : convertValueExpression(
                                    greaterThanEquals.getRightExpression(),
                                    schema.findField(column.getColumnName()));
            return Expressions.greaterThanOrEqual(column.getColumnName(), value);
        }
        if (condition instanceof MinorThan) {
            MinorThan minorThan = (MinorThan) condition;
            Column column = (Column) minorThan.getLeftExpression();
            Object value =
                    schema == null
                            ? convertValueExpression(minorThan.getRightExpression())
                            : convertValueExpression(
                                    minorThan.getRightExpression(),
                                    schema.findField(column.getColumnName()));
            return Expressions.lessThan(column.getColumnName(), value);
        }
        if (condition instanceof MinorThanEquals) {
            MinorThanEquals minorThanEquals = (MinorThanEquals) condition;
            Column column = (Column) minorThanEquals.getLeftExpression();
            Object value =
                    schema == null
                            ? convertValueExpression(minorThanEquals.getRightExpression())
                            : convertValueExpression(
                                    minorThanEquals.getRightExpression(),
                                    schema.findField(column.getColumnName()));
            return Expressions.lessThanOrEqual(column.getColumnName(), value);
        }
        if (condition instanceof IsNullExpression) {
            IsNullExpression isNullExpression = (IsNullExpression) condition;
            Column column = (Column) isNullExpression.getLeftExpression();
            if (isNullExpression.isNot()) {
                return Expressions.notNull(column.getColumnName());
            }
            return Expressions.isNull(column.getColumnName());
        }
        if (condition instanceof InExpression) {
            InExpression inExpression = (InExpression) condition;
            Column column = (Column) inExpression.getLeftExpression();
            ExpressionList<net.sf.jsqlparser.expression.Expression> itemsList =
                    (ExpressionList) inExpression.getRightExpression();
            List<Object> values =
                    itemsList.getExpressions().stream()
                            .map(
                                    e ->
                                            schema == null
                                                    ? convertValueExpression(e)
                                                    : convertValueExpression(
                                                            e,
                                                            schema.findField(
                                                                    column.getColumnName())))
                            .collect(Collectors.toList());
            if (inExpression.isNot()) {
                return Expressions.notIn(column.getColumnName(), values);
            }
            return Expressions.in(column.getColumnName(), values);
        }
        if (condition instanceof IsBooleanExpression) {
            IsBooleanExpression booleanExpression = (IsBooleanExpression) condition;
            Column column = (Column) booleanExpression.getLeftExpression();
            if (booleanExpression.isNot()) {
                return Expressions.notEqual(column.getColumnName(), booleanExpression.isTrue());
            }
            return Expressions.equal(column.getColumnName(), booleanExpression.isTrue());
        }
        if (condition instanceof LikeExpression) {
            LikeExpression expr = (LikeExpression) condition;
            String columnName = ((Column) expr.getLeftExpression()).getColumnName();
            String value = ((StringValue) expr.getRightExpression()).getValue();
            LikeExpression.KeyWord keyWord = expr.getLikeKeyWord();
            if (keyWord == LikeExpression.KeyWord.LIKE) {
                return Expressions.startsWith(columnName, value);
            } else {
                throw new UnsupportedOperationException("Unsupported like keyword: " + keyWord);
            }
        }

        throw new UnsupportedOperationException(
                "Unsupported condition: " + condition.getClass().getName());
    }

    @SneakyThrows
    private static Object convertValueExpression(
            net.sf.jsqlparser.expression.Expression valueExpression,
            Types.NestedField icebergColumn) {
        switch (icebergColumn.type().typeId()) {
            case DECIMAL:
                return new BigDecimal(valueExpression.toString());
            case DATE:
                if (valueExpression instanceof StringValue) {
                    LocalDate date =
                            LocalDate.parse(
                                    ((StringValue) valueExpression).getValue(), ISO_LOCAL_DATE);
                    return DateTimeUtil.daysFromDate(date);
                }
            case TIME:
                if (valueExpression instanceof StringValue) {
                    LocalTime time =
                            LocalTime.parse(
                                    ((StringValue) valueExpression).getValue(), ISO_LOCAL_TIME);
                    return DateTimeUtil.microsFromTime(time);
                }
            case TIMESTAMP:
                if (valueExpression instanceof StringValue) {
                    LocalDateTime dateTime =
                            LocalDateTime.parse(
                                    ((StringValue) valueExpression).getValue(),
                                    LOCAL_DATE_TIME_FORMATTER);
                    return DateTimeUtil.microsFromTimestamp(dateTime);
                }
            default:
                return convertValueExpression(valueExpression);
        }
    }

    private static Object convertValueExpression(
            net.sf.jsqlparser.expression.Expression valueExpression) {
        if (valueExpression instanceof LongValue) {
            return ((LongValue) valueExpression).getValue();
        }
        if (valueExpression instanceof DoubleValue) {
            return ((DoubleValue) valueExpression).getValue();
        }
        if (valueExpression instanceof StringValue) {
            return ((StringValue) valueExpression).getValue();
        }
        return valueExpression.toString();
    }
}
