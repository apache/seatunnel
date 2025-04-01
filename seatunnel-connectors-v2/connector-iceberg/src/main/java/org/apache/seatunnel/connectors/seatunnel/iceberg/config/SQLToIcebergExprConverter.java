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

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.expressions.Expressions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.util.ArrayList;
import java.util.List;

class SQLToIcebergExprConverter {
    private static final Logger log = LoggerFactory.getLogger(SQLToIcebergExprConverter.class);

    public static org.apache.iceberg.expressions.Expression parseWhereClauseToIcebergExpression(
            String whereClauseStr) {
        if (StringUtils.isNotBlank(whereClauseStr)) {
            try {
                // use the JsqlParser to parse the where clause
                Select select =
                        (Select) CCJSqlParserUtil.parse("SELECT * FROM t WHERE " + whereClauseStr);

                PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
                net.sf.jsqlparser.expression.Expression whereClause = plainSelect.getWhere();

                org.apache.iceberg.expressions.Expression icebergExpr =
                        convertToIcebergExpression(whereClause);
                return icebergExpr;
            } catch (JSQLParserException e) {
                log.error("Failed to parse where clause: {}", whereClauseStr, e);
            }
        }
        return Expressions.alwaysTrue();
    }

    private static org.apache.iceberg.expressions.Expression convertToIcebergExpression(
            net.sf.jsqlparser.expression.Expression expr) {
        if (expr instanceof Parenthesis) {
            return convertToIcebergExpression(((Parenthesis) expr).getExpression());
        }
        if (expr instanceof AndExpression) {
            return handleAndExpression((AndExpression) expr);
        }
        if (expr instanceof OrExpression) {
            return handleOrExpression((OrExpression) expr);
        }
        if (expr instanceof NotExpression) {
            return handleNotExpression((NotExpression) expr);
        }
        if (expr instanceof IsNullExpression) {
            return handleIsNullExpression((IsNullExpression) expr);
        }
        if (expr instanceof InExpression) {
            return handleInExpression((InExpression) expr);
        }
        if (expr instanceof LikeExpression) {
            return handleLikeExpression((LikeExpression) expr);
        }
        if (expr instanceof ComparisonOperator) {
            return handleComparisonOperator((ComparisonOperator) expr);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported expression type: " + expr.getClass());
        }
    }

    private static org.apache.iceberg.expressions.Expression handleLikeExpression(
            LikeExpression expr) {
        String columnName = ((Column) expr.getLeftExpression()).getColumnName();
        String value = ((StringValue) expr.getRightExpression()).getValue();
        LikeExpression.KeyWord keyWord = expr.getLikeKeyWord();
        if (keyWord == LikeExpression.KeyWord.LIKE) {
            return Expressions.startsWith(columnName, value);
        } else {
            throw new UnsupportedOperationException("Unsupported like keyword: " + keyWord);
        }
    }

    private static org.apache.iceberg.expressions.Expression handleInExpression(InExpression expr) {
        String columnName = ((Column) expr.getLeftExpression()).getColumnName();
        ParenthesedExpressionList<Expression> list =
                (ParenthesedExpressionList) expr.getRightExpression();
        List<Object> values = extractValuesFromItemsList(list);

        if (expr.isNot()) {
            // handle NOT IN
            return Expressions.notIn(columnName, values.toArray());
        } else {
            // handle IN
            return Expressions.in(columnName, values.toArray());
        }
    }

    private static List<Object> extractValuesFromItemsList(
            ParenthesedExpressionList<Expression> list) {
        List<Object> res = new ArrayList<>();
        for (Expression expression : list) {
            res.add(getValueFromExpression(expression));
        }
        return res;
    }

    private static org.apache.iceberg.expressions.Expression handleAndExpression(
            AndExpression expr) {
        return Expressions.and(
                convertToIcebergExpression(expr.getLeftExpression()),
                convertToIcebergExpression(expr.getRightExpression()));
    }

    private static org.apache.iceberg.expressions.Expression handleOrExpression(OrExpression expr) {
        return Expressions.or(
                convertToIcebergExpression(expr.getLeftExpression()),
                convertToIcebergExpression(expr.getRightExpression()));
    }

    private static org.apache.iceberg.expressions.Expression handleNotExpression(
            NotExpression expr) {
        return Expressions.not(convertToIcebergExpression(expr.getExpression()));
    }

    private static org.apache.iceberg.expressions.Expression handleComparisonOperator(
            ComparisonOperator expr) {
        String columnName = ((Column) expr.getLeftExpression()).getColumnName();
        Object value = getValueFromExpression(expr.getRightExpression());

        if (expr instanceof EqualsTo) {
            return Expressions.equal(columnName, value);
        } else if (expr instanceof NotEqualsTo) {
            return Expressions.notEqual(columnName, value);
        } else if (expr instanceof GreaterThan) {
            return Expressions.greaterThan(columnName, value);
        } else if (expr instanceof GreaterThanEquals) {
            return Expressions.greaterThanOrEqual(columnName, value);
        } else if (expr instanceof MinorThan) {
            return Expressions.lessThan(columnName, value);
        } else if (expr instanceof MinorThanEquals) {
            return Expressions.lessThanOrEqual(columnName, value);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported comparison operator: " + expr.getClass());
        }
    }

    private static org.apache.iceberg.expressions.Expression handleIsNullExpression(
            IsNullExpression expr) {
        String columnName = ((Column) expr.getLeftExpression()).getColumnName();
        if (expr.isNot()) {
            // handle IS NOT NULL
            return Expressions.notNull(columnName);
        } else {
            // handle IS NULL
            return Expressions.isNull(columnName);
        }
    }

    private static Object getValueFromExpression(net.sf.jsqlparser.expression.Expression expr) {
        if (expr instanceof LongValue) {
            return ((LongValue) expr).getValue();
        } else if (expr instanceof NullValue) {
            return ((LongValue) expr).getValue();
        } else if (expr instanceof StringValue) {
            return ((StringValue) expr).getValue();
        } else if (expr instanceof DoubleValue) {
            return ((DoubleValue) expr).getValue();
        } else if (expr instanceof Column) {
            String columnName = ((Column) expr).getColumnName();
            if (Boolean.TRUE.equals(Boolean.parseBoolean(columnName))) {
                return Boolean.TRUE;
            } else if (Boolean.FALSE.equals(Boolean.parseBoolean(columnName))) {
                return Boolean.FALSE;
            }
            return ((Column) expr).getColumnName();
        } else {
            throw new UnsupportedOperationException("Unsupported value type: " + expr.getClass());
        }
    }
}
