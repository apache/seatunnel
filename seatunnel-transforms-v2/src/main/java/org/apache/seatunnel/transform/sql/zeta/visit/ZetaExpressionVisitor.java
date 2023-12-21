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

package org.apache.seatunnel.transform.sql.zeta.visit;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;
import org.apache.seatunnel.transform.sql.zeta.functions.DateTimeFunctionEnum;
import org.apache.seatunnel.transform.sql.zeta.functions.NumberFunctionEnum;
import org.apache.seatunnel.transform.sql.zeta.functions.StringFunctionEnum;
import org.apache.seatunnel.transform.sql.zeta.functions.SystemFunctionEnum;

import org.apache.commons.lang3.tuple.Pair;

import lombok.Getter;
import net.sf.jsqlparser.expression.AllValue;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.ArrayExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.ConnectByRootOperator;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonAggregateFunction;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.JsonFunction;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NextValExpression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.OracleNamedFunctionParameter;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.RowGetExpression;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.TimezoneExpression;
import net.sf.jsqlparser.expression.TryCastExpression;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.ValueListExpression;
import net.sf.jsqlparser.expression.VariableAssignment;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.XMLSerializeExpr;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.IntegerDivision;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.FullTextSearch;
import net.sf.jsqlparser.expression.operators.relational.GeometryDistance;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Pivot;
import net.sf.jsqlparser.statement.select.PivotXml;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.UnPivot;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZetaExpressionVisitor extends ExpressionVisitorAdapter {
    private final List<SeaTunnelDataType<?>> outputDataTypes;
    private final List<Object> outputFields;
    private final SeaTunnelRowType inputRowType;
    private final Object[] inputFields;
    private final List<ZetaUDF> udfList;
    private final ZetaStatementVisitor zetaStatementVisitor;

    private @Getter boolean isKeep = true;

    public ZetaExpressionVisitor(ZetaStatementVisitor zetaStatementVisitor) {
        outputDataTypes = zetaStatementVisitor.getOutputDataTypes();
        outputFields = zetaStatementVisitor.getOutputFields();
        inputRowType = zetaStatementVisitor.getInputRowType();
        inputFields = zetaStatementVisitor.getInputFields();
        udfList = zetaStatementVisitor.getUdfList();
        this.zetaStatementVisitor = zetaStatementVisitor;
    }

    private void setDataAndType(SeaTunnelDataType<?> type, Object value) {
        int index = zetaStatementVisitor.getIndex();
        if (outputDataTypes.size() == index + 1 && index >= 0) {
            outputDataTypes.set(index, type);
            outputFields.set(index, value);
        } else {
            outputDataTypes.add(type);
            outputFields.add(value);
        }
    }

    private Pair<SeaTunnelDataType<?>, Object> getLasTypeAndData() {
        int index = zetaStatementVisitor.getIndex();
        if (zetaStatementVisitor.isNeedWhere()) {
            index = outputDataTypes.size() - 1;
        }
        SeaTunnelDataType<?> type = outputDataTypes.get(index);
        Object value = outputFields.get(index);
        return Pair.of(type, value);
    }

    private void executeCalculate(Expression left, Expression right, String name) {
        left.accept(this);
        Pair<SeaTunnelDataType<?>, Object> leftPair = getLasTypeAndData();
        right.accept(this);
        Pair<SeaTunnelDataType<?>, Object> rightPair = getLasTypeAndData();

        Pair<SeaTunnelDataType<?>, Object> pair =
                CalculateEnum.evaluate(
                        name,
                        leftPair.getKey(),
                        rightPair.getKey(),
                        leftPair.getValue(),
                        rightPair.getValue());
        setDataAndType(pair.getKey(), pair.getValue());
    }

    private void executeFunction(
            String functionName, List<Object> args, List<SeaTunnelDataType<?>> types) {

        Pair<SeaTunnelDataType<?>, Object> pair =
                StringFunctionEnum.execute(functionName, types, args);
        if (pair != null) {
            setDataAndType(pair.getKey(), pair.getValue());
            return;
        }

        pair = NumberFunctionEnum.execute(functionName, types, args);
        if (pair != null) {
            setDataAndType(pair.getKey(), pair.getValue());
            return;
        }

        pair = DateTimeFunctionEnum.execute(functionName, types, args);
        if (pair != null) {
            setDataAndType(pair.getKey(), pair.getValue());
            return;
        }

        pair = SystemFunctionEnum.execute(functionName, types, args);
        if (pair != null) {
            setDataAndType(pair.getKey(), pair.getValue());
            return;
        }

        for (ZetaUDF udf : udfList) {
            if (udf.functionName().equalsIgnoreCase(functionName)) {
                SeaTunnelDataType<?> type = udf.resultType(types);
                Object val = udf.evaluate(args);
                setDataAndType(type, val);
                return;
            }
        }
        throw new IllegalArgumentException(
                String.format("Unsupported function: %s ", functionName));
    }

    @Override
    public void visit(StringValue value) {
        BasicType<String> type = BasicType.STRING_TYPE;
        String val = value.getValue();
        setDataAndType(type, val);
    }

    @Override
    public void visit(DoubleValue value) {
        BasicType<Double> type = BasicType.DOUBLE_TYPE;
        double val = value.getValue();
        setDataAndType(type, val);
    }

    @Override
    public void visit(LongValue value) {
        long temp = value.getValue();
        SeaTunnelDataType<?> type = BasicType.LONG_TYPE;
        Object val = temp;
        if (temp <= Integer.MAX_VALUE && temp >= Integer.MIN_VALUE) {
            type = BasicType.INT_TYPE;
            val = (int) temp;
        }
        setDataAndType(type, val);
    }

    @Override
    public void visit(DateValue value) {
        LocalTimeType<LocalDate> type = LocalTimeType.LOCAL_DATE_TYPE;
        Date val = value.getValue();
        setDataAndType(type, val);
    }

    @Override
    public void visit(TimeValue value) {
        LocalTimeType<LocalTime> type = LocalTimeType.LOCAL_TIME_TYPE;
        Time val = value.getValue();
        setDataAndType(type, val);
    }

    @Override
    public void visit(TimestampValue value) {
        LocalTimeType<LocalDateTime> type = LocalTimeType.LOCAL_DATE_TIME_TYPE;
        Timestamp val = value.getValue();
        setDataAndType(type, val);
    }

    @Override
    public void visit(NullValue value) {
        BasicType<Void> type = BasicType.VOID_TYPE;
        setDataAndType(type, null);
    }

    @Override
    public void visit(Column column) {
        String columnName = column.getColumnName();
        int index = inputRowType.indexOf(columnName);
        SeaTunnelDataType<?> type = inputRowType.getFieldType(index);
        Object val = inputFields[index];
        setDataAndType(type, val);
    }

    @Override
    public void visit(ArrayExpression array) {
        Expression indexExpression = array.getIndexExpression();
        Expression objExpression = array.getObjExpression();
        String objectName = objExpression.toString();

        int fieldIndex = inputRowType.indexOf(objectName);
        SeaTunnelDataType<?> fieldType = inputRowType.getFieldType(fieldIndex);
        Object fieldData = inputFields[fieldIndex];

        indexExpression.accept(this);
        Object key = getLasTypeAndData().getValue();

        Object val = null;
        if (fieldType instanceof MapType && null != key) {
            Map<?, ?> map = (Map<?, ?>) fieldData;

            if (null != fieldData) {
                val = map.get(key);
                // json serialization key will be parsed to string
                val = val == null ? map.get(key.toString()) : val;
            }
            SeaTunnelDataType<?> type = ((MapType<?, ?>) fieldType).getValueType();
            setDataAndType(type, val);
        } else if (fieldType instanceof ArrayType && key instanceof Number) {
            int index = Integer.parseInt(key.toString());
            try {
                if (null != fieldData) {
                    val = ((Object[]) fieldData)[index];
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                // do nothing
            }
            BasicType<?> type = ((ArrayType<?, ?>) fieldType).getElementType();
            setDataAndType(type, val);
        } else {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                    String.format(
                            "The column %s type is %s, wrong operation %s[%s]",
                            objectName, fieldType, objectName, indexExpression));
        }
    }

    @Override
    public void visit(Addition expr) {
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();
        executeCalculate(left, right, "add");
    }

    @Override
    public void visit(Subtraction expr) {
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();
        executeCalculate(left, right, "sub");
    }

    @Override
    public void visit(Multiplication expr) {
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();
        executeCalculate(left, right, "mul");
    }

    @Override
    public void visit(Division expr) {
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();
        executeCalculate(left, right, "div");
    }

    @Override
    public void visit(Modulo expr) {
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();
        executeCalculate(left, right, "mod");
    }

    @Override
    public void visit(Function function) {
        List<Object> functionArgs = new ArrayList<>();
        List<SeaTunnelDataType<?>> functionTypes = new ArrayList<>();
        ExpressionList parameters = function.getParameters();
        if (parameters != null) {
            for (Expression expression : parameters.getExpressions()) {
                expression.accept(this);
                Pair<SeaTunnelDataType<?>, Object> pair = getLasTypeAndData();
                functionTypes.add(pair.getKey());
                functionArgs.add(pair.getValue());
            }
        }
        executeFunction(function.getName(), functionArgs, functionTypes);
    }

    @Override
    public void visit(IsNullExpression expr) {
        expr.getLeftExpression().accept(this);
        Object value = getLasTypeAndData().getValue();
        isKeep = expr.isNot() != (value == null);
    }

    @Override
    public void visit(InExpression expr) {
        expr.getLeftExpression().accept(this);
        Object leftValue = getLasTypeAndData().getValue();
        ExpressionList rightItemsList = (ExpressionList) expr.getRightItemsList();
        boolean checkIn = false;
        for (Expression expression : rightItemsList.getExpressions()) {
            expression.accept(this);
            Object rightValue = getLasTypeAndData().getValue();
            if (leftValue == null || rightValue == null) {
                break;
            }
            if (leftValue instanceof Number && rightValue instanceof Number) {
                if (((Number) leftValue).doubleValue() == ((Number) rightValue).doubleValue()) {
                    checkIn = true;
                    break;
                }
            }
            if (leftValue.toString().equals(rightValue.toString())) {
                checkIn = true;
                break;
            }
        }
        isKeep = expr.isNot() != checkIn;
    }

    @Override
    public void visit(LikeExpression expr) {
        expr.getLeftExpression().accept(this);
        Object leftValue = getLasTypeAndData().getValue();
        expr.getRightExpression().accept(this);
        Object rightValue = getLasTypeAndData().getValue();
        if (leftValue == null || rightValue == null) {
            isKeep = false;
            return;
        }

        String regex = rightValue.toString().replace("%", ".*").replace("_", ".");
        if (regex.startsWith("'") && regex.endsWith("'")) {
            regex = regex.substring(0, regex.length() - 1).substring(1);
        }
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(leftValue.toString());

        isKeep = expr.isNot() != matcher.matches();
    }

    @Override
    public void visit(EqualsTo expr) {
        expr.getLeftExpression().accept(this);
        Object leftValue = getLasTypeAndData().getValue();
        expr.getRightExpression().accept(this);
        Object rightValue = getLasTypeAndData().getValue();

        if (leftValue == null || rightValue == null) {
            isKeep = false;
            return;
        }
        if (leftValue instanceof Number && rightValue instanceof Number) {
            isKeep = ((Number) leftValue).doubleValue() == ((Number) rightValue).doubleValue();
            return;
        }
        isKeep = leftValue.toString().equals(rightValue.toString());
    }

    @Override
    public void visit(NotEqualsTo expr) {
        expr.getLeftExpression().accept(this);
        Object leftValue = getLasTypeAndData().getValue();
        expr.getRightExpression().accept(this);
        Object rightValue = getLasTypeAndData().getValue();
        if (leftValue == null || rightValue == null) {
            isKeep = false;
            return;
        }
        if (leftValue instanceof Number && rightValue instanceof Number) {
            isKeep = ((Number) leftValue).doubleValue() != ((Number) rightValue).doubleValue();
            return;
        }
        isKeep = !leftValue.toString().equals(rightValue.toString());
    }

    @Override
    public void visit(GreaterThan expr) {
        expr.getLeftExpression().accept(this);
        Object leftValue = getLasTypeAndData().getValue();
        expr.getRightExpression().accept(this);
        Object rightValue = getLasTypeAndData().getValue();
        if (leftValue == null || rightValue == null) {
            isKeep = false;
            return;
        }
        if (leftValue instanceof Number && rightValue instanceof Number) {
            isKeep = ((Number) leftValue).doubleValue() > ((Number) rightValue).doubleValue();
            return;
        }
        if (leftValue instanceof String && rightValue instanceof String) {
            isKeep = ((String) leftValue).compareTo((String) rightValue) > 0;
            return;
        }
        if (leftValue instanceof LocalDateTime && rightValue instanceof LocalDateTime) {
            isKeep = ((LocalDateTime) leftValue).isAfter(((LocalDateTime) rightValue));
            return;
        }
        if (leftValue instanceof LocalDate && rightValue instanceof LocalDate) {
            isKeep = ((LocalDate) leftValue).isAfter(((LocalDate) rightValue));
            return;
        }
        if (leftValue instanceof LocalTime && rightValue instanceof LocalTime) {
            isKeep = ((LocalTime) leftValue).isAfter(((LocalTime) rightValue));
            return;
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format(
                        "Filed types not matched, leftVal is: %s, rightVal is: %s ",
                        leftValue.getClass().getSimpleName(),
                        rightValue.getClass().getSimpleName()));
    }

    @Override
    public void visit(GreaterThanEquals expr) {
        expr.getLeftExpression().accept(this);
        Object leftValue = getLasTypeAndData().getValue();
        expr.getRightExpression().accept(this);
        Object rightValue = getLasTypeAndData().getValue();
        if (leftValue == null || rightValue == null) {
            isKeep = false;
            return;
        }
        if (leftValue instanceof Number && rightValue instanceof Number) {
            isKeep = ((Number) leftValue).doubleValue() >= ((Number) rightValue).doubleValue();
            return;
        }
        if (leftValue instanceof String && rightValue instanceof String) {
            isKeep = ((String) leftValue).compareTo((String) rightValue) >= 0;
            return;
        }
        if (leftValue instanceof LocalDateTime && rightValue instanceof LocalDateTime) {
            LocalDateTime l = (LocalDateTime) leftValue;
            LocalDateTime r = (LocalDateTime) rightValue;
            isKeep = l.isAfter(r) || l.isEqual(r);
            return;
        }
        if (leftValue instanceof LocalDate && rightValue instanceof LocalDate) {
            LocalDate l = (LocalDate) leftValue;
            LocalDate r = (LocalDate) rightValue;
            isKeep = l.isAfter(r) || l.isEqual(r);
            return;
        }
        if (leftValue instanceof LocalTime && rightValue instanceof LocalTime) {
            LocalTime l = (LocalTime) leftValue;
            LocalTime r = (LocalTime) rightValue;
            isKeep = l.isAfter(r) || l.equals(r);
            return;
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format(
                        "Filed types not matched, leftVal is: %s, rightVal is: %s ",
                        leftValue.getClass().getSimpleName(),
                        rightValue.getClass().getSimpleName()));
    }

    @Override
    public void visit(MinorThan expr) {
        expr.getLeftExpression().accept(this);
        Object leftValue = getLasTypeAndData().getValue();
        expr.getRightExpression().accept(this);
        Object rightValue = getLasTypeAndData().getValue();
        if (leftValue == null || rightValue == null) {
            isKeep = false;
            return;
        }
        if (leftValue instanceof Number && rightValue instanceof Number) {
            isKeep = ((Number) leftValue).doubleValue() < ((Number) rightValue).doubleValue();
            return;
        }
        if (leftValue instanceof String && rightValue instanceof String) {
            isKeep = ((String) leftValue).compareTo((String) rightValue) < 0;
            return;
        }
        if (leftValue instanceof LocalDateTime && rightValue instanceof LocalDateTime) {
            isKeep = ((LocalDateTime) leftValue).isBefore((LocalDateTime) rightValue);
            return;
        }
        if (leftValue instanceof LocalDate && rightValue instanceof LocalDate) {
            isKeep = ((LocalDate) leftValue).isBefore((LocalDate) rightValue);
            return;
        }
        if (leftValue instanceof LocalTime && rightValue instanceof LocalTime) {
            isKeep = ((LocalTime) leftValue).isBefore((LocalTime) rightValue);
            return;
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format(
                        "Filed types not matched, leftVal is: %s, rightVal is: %s ",
                        leftValue.getClass().getSimpleName(),
                        rightValue.getClass().getSimpleName()));
    }

    @Override
    public void visit(MinorThanEquals expr) {
        expr.getLeftExpression().accept(this);
        Object leftValue = getLasTypeAndData().getValue();
        expr.getRightExpression().accept(this);
        Object rightValue = getLasTypeAndData().getValue();
        if (leftValue == null || rightValue == null) {
            isKeep = false;
            return;
        }
        if (leftValue instanceof Number && rightValue instanceof Number) {
            isKeep = ((Number) leftValue).doubleValue() <= ((Number) rightValue).doubleValue();
            return;
        }
        if (leftValue instanceof String && rightValue instanceof String) {
            isKeep = ((String) leftValue).compareTo((String) rightValue) <= 0;
            return;
        }
        if (leftValue instanceof LocalDateTime && rightValue instanceof LocalDateTime) {
            LocalDateTime l = (LocalDateTime) leftValue;
            LocalDateTime r = (LocalDateTime) rightValue;
            isKeep = l.isBefore(r) || l.isEqual(r);
            return;
        }
        if (leftValue instanceof LocalDate && rightValue instanceof LocalDate) {
            LocalDate l = (LocalDate) leftValue;
            LocalDate r = (LocalDate) rightValue;
            isKeep = l.isBefore(r) || l.isEqual(r);
            return;
        }
        if (leftValue instanceof LocalTime && rightValue instanceof LocalTime) {
            LocalTime l = (LocalTime) leftValue;
            LocalTime r = (LocalTime) rightValue;
            isKeep = l.isBefore(r) || l.equals(r);
            return;
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format(
                        "Filed types not matched, leftVal is: %s, rightVal is: %s ",
                        leftValue.getClass().getSimpleName(),
                        rightValue.getClass().getSimpleName()));
    }

    @Override
    public void visit(AndExpression expr) {
        expr.getLeftExpression().accept(this);
        boolean leftValue = isKeep;
        expr.getRightExpression().accept(this);
        boolean rightValue = isKeep;
        isKeep = leftValue && rightValue;
    }

    @Override
    public void visit(OrExpression expr) {
        expr.getLeftExpression().accept(this);
        boolean leftValue = isKeep;
        expr.getRightExpression().accept(this);
        boolean rightValue = isKeep;
        isKeep = leftValue || rightValue;
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
        SeaTunnelDataType<?> type;
        Object val;
        String key = timeKeyExpression.getStringValue().toUpperCase();
        switch (key) {
            case "CURRENT_DATE":
            case "CURRENT_DATE()":
                type = LocalTimeType.LOCAL_DATE_TYPE;
                val = LocalDate.now();
                break;
            case "CURRENT_TIME":
            case "CURRENT_TIME()":
                type = LocalTimeType.LOCAL_TIME_TYPE;
                val = LocalTime.now();
                break;
            case "CURRENT_TIMESTAMP":
            case "CURRENT_TIMESTAMP()":
                type = LocalTimeType.LOCAL_DATE_TIME_TYPE;
                val = LocalDateTime.now();
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported TimeKey expression: %s ", key));
        }
        setDataAndType(type, val);
    }

    @Override
    public void visit(ExtractExpression expr) {
        List<Object> functionArgs = new ArrayList<>();
        expr.getExpression().accept(this);
        functionArgs.add(getLasTypeAndData().getValue());
        functionArgs.add(expr.getName());
        executeFunction("EXTRACT", functionArgs, null);
    }

    @Override
    public void visit(CastExpression expr) {
        expr.getLeftExpression().accept(this);
        ColDataType colDataType = expr.getType();
        Pair<SeaTunnelDataType<?>, Object> pair = getLasTypeAndData();

        ArrayList<SeaTunnelDataType<?>> types = new ArrayList<>();
        ArrayList<Object> objects = new ArrayList<>();

        SeaTunnelDataType<?> t1 = pair.getKey();
        SeaTunnelDataType<?> t2 = BasicType.STRING_TYPE;
        types.add(t1);
        types.add(t2);
        Object d1 = pair.getValue();
        String d2 = colDataType.getDataType();
        objects.add(d1);
        objects.add(d2);

        List<String> argumentsStringList = colDataType.getArgumentsStringList();
        if (argumentsStringList != null) {
            objects.addAll(argumentsStringList);
        }

        Pair<SeaTunnelDataType<?>, Object> resultPair =
                SystemFunctionEnum.execute("CAST", types, objects);
        if (resultPair != null) {
            setDataAndType(resultPair.getKey(), resultPair.getValue());
        } else {
            setDataAndType(t1, null);
        }
    }

    /** c1 || c2 */
    @Override
    public void visit(Concat expr) {
        expr.getLeftExpression().accept(this);
        Object left = getLasTypeAndData().getValue();
        left = Optional.ofNullable(left).orElse("");
        expr.getRightExpression().accept(this);
        Object right = getLasTypeAndData().getValue();
        right = Optional.ofNullable(right).orElse("");
        SeaTunnelDataType<?> type = BasicType.STRING_TYPE;
        Object val = left + right.toString();
        setDataAndType(type, val);
    }

    @Override
    protected void visitBinaryExpression(BinaryExpression expr) {
        throw new IllegalArgumentException("Unsupported visit BinaryExpression");
    }

    @Override
    public void visit(ExpressionList expressionList) {
        throw new IllegalArgumentException("Unsupported visit ExpressionList");
    }

    @Override
    public void visit(SignedExpression expr) {
        throw new IllegalArgumentException("Unsupported visit SignedExpression");
    }

    @Override
    public void visit(JdbcParameter parameter) {
        throw new IllegalArgumentException("Unsupported visit JdbcParameter");
    }

    @Override
    public void visit(JdbcNamedParameter parameter) {
        throw new IllegalArgumentException("Unsupported visit JdbcNamedParameter");
    }

    @Override
    public void visit(IntegerDivision expr) {
        throw new IllegalArgumentException("Unsupported visit IntegerDivision");
    }

    @Override
    public void visit(XorExpression expr) {
        throw new IllegalArgumentException("Unsupported visit XorExpression");
    }

    @Override
    public void visit(Between expr) {
        throw new IllegalArgumentException("Unsupported visit Between");
    }

    @Override
    public void visit(FullTextSearch expr) {
        throw new IllegalArgumentException("Unsupported visit FullTextSearch");
    }

    @Override
    public void visit(IsBooleanExpression expr) {
        throw new IllegalArgumentException("Unsupported visit IsBooleanExpression");
    }

    @Override
    public void visit(SubSelect subSelect) {
        throw new IllegalArgumentException("Unsupported visit SubSelect");
    }

    @Override
    public void visit(CaseExpression expr) {
        throw new IllegalArgumentException("Unsupported visit CaseExpression");
    }

    @Override
    public void visit(WhenClause expr) {
        throw new IllegalArgumentException("Unsupported visit WhenClause");
    }

    @Override
    public void visit(ExistsExpression expr) {
        throw new IllegalArgumentException("Unsupported visit ExistsExpression");
    }

    @Override
    public void visit(AnyComparisonExpression expr) {
        throw new IllegalArgumentException("Unsupported visit AnyComparisonExpression");
    }

    @Override
    public void visit(Matches expr) {
        throw new IllegalArgumentException("Unsupported visit Matches");
    }

    @Override
    public void visit(BitwiseAnd expr) {
        throw new IllegalArgumentException("Unsupported visit BitwiseAnd");
    }

    @Override
    public void visit(BitwiseOr expr) {
        throw new IllegalArgumentException("Unsupported visit BitwiseOr");
    }

    @Override
    public void visit(BitwiseXor expr) {
        throw new IllegalArgumentException("Unsupported visit BitwiseXor");
    }

    @Override
    public void visit(TryCastExpression expr) {
        throw new IllegalArgumentException("Unsupported visit TryCastExpression");
    }

    @Override
    public void visit(AnalyticExpression expr) {
        throw new IllegalArgumentException("Unsupported visit AnalyticExpression");
    }

    @Override
    public void visit(IntervalExpression expr) {
        throw new IllegalArgumentException("Unsupported visit IntervalExpression");
    }

    @Override
    public void visit(OracleHierarchicalExpression expr) {
        throw new IllegalArgumentException("Unsupported visit OracleHierarchicalExpression");
    }

    @Override
    public void visit(RegExpMatchOperator expr) {
        throw new IllegalArgumentException("Unsupported visit RegExpMatchOperator");
    }

    @Override
    public void visit(NamedExpressionList namedExpressionList) {
        throw new IllegalArgumentException("Unsupported visit NamedExpressionList");
    }

    @Override
    public void visit(MultiExpressionList multiExprList) {
        throw new IllegalArgumentException("Unsupported visit MultiExpressionList");
    }

    @Override
    public void visit(NotExpression notExpr) {
        throw new IllegalArgumentException("Unsupported visit NotExpression");
    }

    @Override
    public void visit(BitwiseRightShift expr) {
        throw new IllegalArgumentException("Unsupported visit BitwiseRightShift");
    }

    @Override
    public void visit(BitwiseLeftShift expr) {
        throw new IllegalArgumentException("Unsupported visit BitwiseLeftShift");
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        throw new IllegalArgumentException("Unsupported visit JsonExpression");
    }

    @Override
    public void visit(JsonOperator expr) {
        throw new IllegalArgumentException("Unsupported visit JsonOperator");
    }

    @Override
    public void visit(RegExpMySQLOperator expr) {
        throw new IllegalArgumentException("Unsupported visit RegExpMySQLOperator");
    }

    @Override
    public void visit(UserVariable var) {
        throw new IllegalArgumentException("Unsupported visit UserVariable");
    }

    @Override
    public void visit(NumericBind bind) {
        throw new IllegalArgumentException("Unsupported visit NumericBind");
    }

    @Override
    public void visit(KeepExpression expr) {
        throw new IllegalArgumentException("Unsupported visit KeepExpression");
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        throw new IllegalArgumentException("Unsupported visit MySQLGroupConcat");
    }

    @Override
    public void visit(ValueListExpression valueListExpression) {
        throw new IllegalArgumentException("Unsupported visit ValueListExpression");
    }

    @Override
    public void visit(Pivot pivot) {
        throw new IllegalArgumentException("Unsupported visit Pivot");
    }

    @Override
    public void visit(PivotXml pivot) {
        throw new IllegalArgumentException("Unsupported visit PivotXml");
    }

    @Override
    public void visit(UnPivot unpivot) {
        throw new IllegalArgumentException("Unsupported visit UnPivot");
    }

    @Override
    public void visit(AllColumns allColumns) {
        throw new IllegalArgumentException("Unsupported visit AllColumns");
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        throw new IllegalArgumentException("Unsupported visit AllTableColumns");
    }

    @Override
    public void visit(AllValue allValue) {
        throw new IllegalArgumentException("Unsupported visit AllValue");
    }

    @Override
    public void visit(IsDistinctExpression isDistinctExpression) {
        throw new IllegalArgumentException("Unsupported visit IsDistinctExpression");
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        throw new IllegalArgumentException("Unsupported visit SelectExpressionItem");
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        throw new IllegalArgumentException("Unsupported visit RowConstructor");
    }

    @Override
    public void visit(RowGetExpression rowGetExpression) {
        throw new IllegalArgumentException("Unsupported visit RowGetExpression");
    }

    @Override
    public void visit(HexValue hexValue) {
        throw new IllegalArgumentException("Unsupported visit HexValue");
    }

    @Override
    public void visit(OracleHint hint) {
        throw new IllegalArgumentException("Unsupported visit OracleHint");
    }

    @Override
    public void visit(DateTimeLiteralExpression literal) {
        throw new IllegalArgumentException("Unsupported visit DateTimeLiteralExpression");
    }

    @Override
    public void visit(NextValExpression nextVal) {
        throw new IllegalArgumentException("Unsupported visit NextValExpression");
    }

    @Override
    public void visit(CollateExpression col) {
        throw new IllegalArgumentException("Unsupported visit CollateExpression");
    }

    @Override
    public void visit(SimilarToExpression expr) {
        throw new IllegalArgumentException("Unsupported visit SimilarToExpression");
    }

    @Override
    public void visit(ArrayConstructor aThis) {
        throw new IllegalArgumentException("Unsupported visit ArrayConstructor");
    }

    @Override
    public void visit(VariableAssignment var) {
        throw new IllegalArgumentException("Unsupported visit VariableAssignment");
    }

    @Override
    public void visit(XMLSerializeExpr expr) {
        throw new IllegalArgumentException("Unsupported visit XMLSerializeExpr");
    }

    @Override
    public void visit(TimezoneExpression expr) {
        throw new IllegalArgumentException("Unsupported visit TimezoneExpression");
    }

    @Override
    public void visit(JsonAggregateFunction expression) {
        throw new IllegalArgumentException("Unsupported visit JsonAggregateFunction");
    }

    @Override
    public void visit(JsonFunction expression) {
        throw new IllegalArgumentException("Unsupported visit JsonFunction");
    }

    @Override
    public void visit(ConnectByRootOperator connectByRootOperator) {
        throw new IllegalArgumentException("Unsupported visit ConnectByRootOperator");
    }

    @Override
    public void visit(OracleNamedFunctionParameter oracleNamedFunctionParameter) {
        throw new IllegalArgumentException("Unsupported visit OracleNamedFunctionParameter");
    }

    @Override
    public void visit(GeometryDistance geometryDistance) {
        throw new IllegalArgumentException("Unsupported visit GeometryDistance");
    }

    @Override
    public void visit(ColumnDefinition columnDefinition) {
        throw new IllegalArgumentException("Unsupported visit ColumnDefinition");
    }
}
