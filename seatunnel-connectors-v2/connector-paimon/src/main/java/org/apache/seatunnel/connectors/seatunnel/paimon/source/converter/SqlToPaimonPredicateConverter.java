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

package org.apache.seatunnel.connectors.seatunnel.paimon.source.converter;

import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

public class SqlToPaimonPredicateConverter {

    public static PlainSelect convertToPlainSelect(String query) {
        if (StringUtils.isBlank(query)) {
            return null;
        }
        Statement statement = null;
        try {
            statement = CCJSqlParserUtil.parse(query);
        } catch (JSQLParserException e) {
            throw new IllegalArgumentException("Error parsing SQL.", e);
        }
        // Confirm that the SQL statement is a Select statement
        if (!(statement instanceof Select)) {
            throw new IllegalArgumentException("Only SELECT statements are supported.");
        }
        Select select = (Select) statement;
        SelectBody selectBody = select.getSelectBody();
        if (!(selectBody instanceof PlainSelect)) {
            throw new IllegalArgumentException("Only simple SELECT statements are supported.");
        }
        PlainSelect plainSelect = (PlainSelect) selectBody;
        if (plainSelect.getHaving() != null
                || plainSelect.getGroupBy() != null
                || plainSelect.getOrderByElements() != null
                || plainSelect.getLimit() != null) {
            throw new IllegalArgumentException(
                    "Only SELECT statements with WHERE clause are supported. The Having, Group By, Order By, Limit clauses are currently unsupported.");
        }
        return plainSelect;
    }

    public static int[] convertSqlSelectToPaimonProjectionIndex(
            String[] fieldNames, PlainSelect plainSelect) {
        int[] projectionIndex = null;
        List<SelectItem> selectItems = plainSelect.getSelectItems();

        List<String> columnNames = new ArrayList<>();
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof AllColumns) {
                return null;
            } else if (selectItem instanceof SelectExpressionItem) {
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                String columnName = selectExpressionItem.getExpression().toString();
                columnNames.add(columnName);
            } else {
                throw new IllegalArgumentException("Error encountered parsing query fields.");
            }
        }

        String[] columnNamesArray = columnNames.toArray(new String[0]);
        projectionIndex =
                IntStream.range(0, columnNamesArray.length)
                        .map(
                                i -> {
                                    String fieldName = columnNamesArray[i];
                                    int index = Arrays.asList(fieldNames).indexOf(fieldName);
                                    if (index == -1) {
                                        throw new IllegalArgumentException(
                                                "column " + fieldName + " does not exist.");
                                    }
                                    return index;
                                })
                        .toArray();

        return projectionIndex;
    }

    public static Predicate convertSqlWhereToPaimonPredicate(
            RowType rowType, PlainSelect plainSelect) {
        Expression whereExpression = plainSelect.getWhere();
        if (Objects.isNull(whereExpression)) {
            return null;
        }
        PredicateBuilder builder = new PredicateBuilder(rowType);
        return parseExpressionToPredicate(builder, rowType, whereExpression);
    }

    private static Predicate parseExpressionToPredicate(
            PredicateBuilder builder, RowType rowType, Expression expression) {
        if (expression instanceof IsNullExpression) {
            IsNullExpression isNullExpression = (IsNullExpression) expression;
            Column column = (Column) isNullExpression.getLeftExpression();
            int columnIndex = getColumnIndex(builder, column);
            if (isNullExpression.isNot()) {
                return builder.isNotNull(columnIndex);
            }
            return builder.isNull(columnIndex);
        } else if (expression instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) expression;
            Column column = (Column) equalsTo.getLeftExpression();
            int columnIndex = getColumnIndex(builder, column);
            Object jsqlParserDataTypeValue =
                    getJSQLParserDataTypeValue(equalsTo.getRightExpression());
            Object paimonDataValue =
                    convertValueByPaimonDataType(
                            rowType, column.getColumnName(), jsqlParserDataTypeValue);
            return builder.equal(columnIndex, paimonDataValue);
        } else if (expression instanceof GreaterThan) {
            GreaterThan greaterThan = (GreaterThan) expression;
            Column column = (Column) greaterThan.getLeftExpression();
            int columnIndex = getColumnIndex(builder, column);
            Object jsqlParserDataTypeValue =
                    getJSQLParserDataTypeValue(greaterThan.getRightExpression());
            Object paimonDataValue =
                    convertValueByPaimonDataType(
                            rowType, column.getColumnName(), jsqlParserDataTypeValue);
            return builder.greaterThan(columnIndex, paimonDataValue);
        } else if (expression instanceof GreaterThanEquals) {
            GreaterThanEquals greaterThanEquals = (GreaterThanEquals) expression;
            Column column = (Column) greaterThanEquals.getLeftExpression();
            int columnIndex = getColumnIndex(builder, column);
            Object jsqlParserDataTypeValue =
                    getJSQLParserDataTypeValue(greaterThanEquals.getRightExpression());
            Object paimonDataValue =
                    convertValueByPaimonDataType(
                            rowType, column.getColumnName(), jsqlParserDataTypeValue);
            return builder.greaterOrEqual(columnIndex, paimonDataValue);
        } else if (expression instanceof MinorThan) {
            MinorThan minorThan = (MinorThan) expression;
            Column column = (Column) minorThan.getLeftExpression();
            int columnIndex = getColumnIndex(builder, column);
            Object jsqlParserDataTypeValue =
                    getJSQLParserDataTypeValue(minorThan.getRightExpression());
            Object paimonDataValue =
                    convertValueByPaimonDataType(
                            rowType, column.getColumnName(), jsqlParserDataTypeValue);
            return builder.lessThan(columnIndex, paimonDataValue);
        } else if (expression instanceof MinorThanEquals) {
            MinorThanEquals minorThanEquals = (MinorThanEquals) expression;
            Column column = (Column) minorThanEquals.getLeftExpression();
            int columnIndex = getColumnIndex(builder, column);
            Object jsqlParserDataTypeValue =
                    getJSQLParserDataTypeValue(minorThanEquals.getRightExpression());
            Object paimonDataValue =
                    convertValueByPaimonDataType(
                            rowType, column.getColumnName(), jsqlParserDataTypeValue);
            return builder.lessOrEqual(columnIndex, paimonDataValue);
        } else if (expression instanceof NotEqualsTo) {
            NotEqualsTo notEqualsTo = (NotEqualsTo) expression;
            Column column = (Column) notEqualsTo.getLeftExpression();
            int columnIndex = getColumnIndex(builder, column);
            Object jsqlParserDataTypeValue =
                    getJSQLParserDataTypeValue(notEqualsTo.getRightExpression());
            Object paimonDataValue =
                    convertValueByPaimonDataType(
                            rowType, column.getColumnName(), jsqlParserDataTypeValue);
            return builder.notEqual(columnIndex, paimonDataValue);
        } else if (expression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) expression;
            Predicate leftPredicate =
                    parseExpressionToPredicate(builder, rowType, andExpression.getLeftExpression());
            Predicate rightPredicate =
                    parseExpressionToPredicate(
                            builder, rowType, andExpression.getRightExpression());
            return PredicateBuilder.and(leftPredicate, rightPredicate);
        } else if (expression instanceof OrExpression) {
            OrExpression orExpression = (OrExpression) expression;
            Predicate leftPredicate =
                    parseExpressionToPredicate(builder, rowType, orExpression.getLeftExpression());
            Predicate rightPredicate =
                    parseExpressionToPredicate(builder, rowType, orExpression.getRightExpression());
            return PredicateBuilder.or(leftPredicate, rightPredicate);
        } else if (expression instanceof Parenthesis) {
            Parenthesis parenthesis = (Parenthesis) expression;
            return parseExpressionToPredicate(builder, rowType, parenthesis.getExpression());
        }
        throw new IllegalArgumentException(
                "Unsupported expression type: " + expression.getClass().getSimpleName());
    }

    private static Object convertValueByPaimonDataType(
            RowType rowType, String columnName, Object jsqlParserDataTypeValue) {
        Optional<DataField> theFiled =
                rowType.getFields().stream()
                        .filter(field -> field.name().equalsIgnoreCase(columnName))
                        .findFirst();
        String strValue = jsqlParserDataTypeValue.toString();
        if (theFiled.isPresent()) {
            DataType dataType = theFiled.get().type();
            switch (dataType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return jsqlParserDataTypeValue;
                case BOOLEAN:
                    return Boolean.parseBoolean(strValue);
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) dataType;
                    return Decimal.fromBigDecimal(
                            new BigDecimal(strValue),
                            decimalType.getPrecision(),
                            decimalType.getScale());
                case TINYINT:
                    return Byte.parseByte(strValue);
                case SMALLINT:
                    return Short.parseShort(strValue);
                case INTEGER:
                    return Integer.parseInt(strValue);
                case BIGINT:
                    return Long.parseLong(strValue);
                case FLOAT:
                    return Float.parseFloat(strValue);
                case DOUBLE:
                    return Double.parseDouble(strValue);
                case DATE:
                    return DateTimeUtils.toInternal(
                            org.apache.seatunnel.common.utils.DateUtils.parse(strValue));
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return Timestamp.fromLocalDateTime(
                            org.apache.seatunnel.common.utils.DateTimeUtils.parse(strValue));
                default:
                    throw new IllegalArgumentException(
                            "Unsupported Paimon data type :" + dataType.getTypeRoot());
            }
        }
        throw new IllegalArgumentException(
                String.format("The column named [%s] is not exists", columnName));
    }

    private static Object getJSQLParserDataTypeValue(Expression expression) {
        if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
        } else if (expression instanceof StringValue || expression instanceof HexValue) {
            return BinaryString.fromString(((StringValue) expression).getValue());
        } else if (expression instanceof DoubleValue) {
            return ((DoubleValue) expression).getValue();
        } else if (expression instanceof DateValue) {
            return ((DateValue) expression).getValue();
        } else if (expression instanceof TimeValue) {
            return ((TimeValue) expression).getValue();
        } else if (expression instanceof TimestampValue) {
            return ((TimestampValue) expression).getValue();
        }
        throw new IllegalArgumentException("Unsupported expression value type: " + expression);
    }

    private static int getColumnIndex(PredicateBuilder builder, Column column) {
        int index = builder.indexOf(column.getColumnName());
        if (index == -1) {
            throw new IllegalArgumentException(
                    String.format("The column named [%s] is not exists", column.getColumnName()));
        }
        return index;
    }
}
