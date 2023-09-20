package org.apache.seatunnel.transform.sql.zeta.visit;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.zeta.agg.OutputDataTypes;
import org.apache.seatunnel.transform.sql.zeta.agg.OutputFieldNames;
import org.apache.seatunnel.transform.sql.zeta.agg.OutputFields;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZetaExpressionVisitor extends ExpressionVisitorAdapter {
    private final OutputFieldNames outputFieldNames;
    private final OutputDataTypes outputDataTypes;
    private final OutputFields outputFields;
    private final SeaTunnelRowType inputRowType;
    private final Object[] inputFields;

    private @Getter boolean isKeep = true;

    public ZetaExpressionVisitor(ZetaStatementVisitor zetaStatementVisitor) {
        outputFieldNames = zetaStatementVisitor.getOutputFieldNames();
        outputDataTypes = zetaStatementVisitor.getOutputDataTypes();
        outputFields = zetaStatementVisitor.getOutputFields();
        inputRowType = zetaStatementVisitor.getInputRowType();
        inputFields = zetaStatementVisitor.getInputFields();
    }

    private void setDataAndType(SeaTunnelDataType<?> type, Object value) {
        int index = outputFieldNames.getIndex();
        outputDataTypes.addOrReplaceTail(index, type);
        outputFields.addOrReplaceTail(index, value);
    }

    // Unified Long
    public Long parseCalculateItem(Expression expression) {
        expression.accept(this);
        try {
            return Long.parseLong(outputFields.getTail().toString());
        } catch (NumberFormatException e) {
            throw new TransformException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "The calculate item must be long integer");
        } catch (NullPointerException e) {
            throw new TransformException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "The calculate currently does not support referencing columns");
        }
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
        Object key = outputFields.getTail();

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
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    String.format(
                            "The column %s type is %s, wrong operation %s[%s]",
                            objectName, fieldType, objectName, indexExpression));
        }
    }

    @Override
    public void visit(Addition expr) {
        Long leftValue = parseCalculateItem(expr.getLeftExpression());
        Long rightValue = parseCalculateItem(expr.getRightExpression());
        new LongValue(leftValue + rightValue).accept(this);
    }

    @Override
    public void visit(Subtraction expr) {
        Long leftValue = parseCalculateItem(expr.getLeftExpression());
        Long rightValue = parseCalculateItem(expr.getRightExpression());
        new LongValue(leftValue - rightValue).accept(this);
    }

    @Override
    public void visit(Multiplication expr) {
        Long leftValue = parseCalculateItem(expr.getLeftExpression());
        Long rightValue = parseCalculateItem(expr.getRightExpression());
        new LongValue(leftValue * rightValue).accept(this);
    }

    @Override
    public void visit(Division expr) {
        Long leftValue = parseCalculateItem(expr.getLeftExpression());
        Long rightValue = parseCalculateItem(expr.getRightExpression());
        new LongValue(leftValue / rightValue).accept(this);
    }

    @Override
    public void visit(Function function) {
        List<Object> functionArgs = new ArrayList<>();
        ExpressionList parameters = function.getParameters();
        for (Expression expression : parameters.getExpressions()) {
            expression.accept(this);
            functionArgs.add(outputFields.getTail());
        }
        System.out.printf(
                "select statement use function: %s \t function args: %s %n",
                function.getName(), functionArgs);
    }

    @Override
    public void visit(IsNullExpression expr) {
        expr.getLeftExpression().accept(this);
        Object value = outputFields.getTail();
        isKeep = expr.isNot() != (value == null);
    }

    @Override
    public void visit(InExpression expr) {
        expr.getLeftExpression().accept(this);
        Object leftValue = outputFields.getTail();
        ExpressionList rightItemsList = (ExpressionList) expr.getRightItemsList();
        boolean checkIn = false;
        for (Expression expression : rightItemsList.getExpressions()) {
            expression.accept(this);
            Object rightValue = outputFields.getTail();
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
        Object leftValue = outputFields.getTail();
        expr.getRightExpression().accept(this);
        Object rightValue = outputFields.getTail();
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
        Object leftValue = outputFields.getTail();
        expr.getRightExpression().accept(this);
        Object rightValue = outputFields.getTail();

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
        Object leftValue = outputFields.getTail();
        expr.getRightExpression().accept(this);
        Object rightValue = outputFields.getTail();
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
        Object leftValue = outputFields.getTail();
        expr.getRightExpression().accept(this);
        Object rightValue = outputFields.getTail();
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
                CommonErrorCode.UNSUPPORTED_OPERATION,
                String.format(
                        "Filed types not matched, left is: %s, right is: %s ",
                        leftValue.getClass().getSimpleName(),
                        rightValue.getClass().getSimpleName()));
    }

    @Override
    public void visit(GreaterThanEquals expr) {
        expr.getLeftExpression().accept(this);
        Object leftValue = outputFields.getTail();
        expr.getRightExpression().accept(this);
        Object rightValue = outputFields.getTail();
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
                CommonErrorCode.UNSUPPORTED_OPERATION,
                String.format(
                        "Filed types not matched, left is: %s, right is: %s ",
                        leftValue.getClass().getSimpleName(),
                        rightValue.getClass().getSimpleName()));
    }

    @Override
    public void visit(MinorThan expr) {
        expr.getLeftExpression().accept(this);
        Object leftValue = outputFields.getTail();
        expr.getRightExpression().accept(this);
        Object rightValue = outputFields.getTail();
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
                CommonErrorCode.UNSUPPORTED_OPERATION,
                String.format(
                        "Filed types not matched, left is: %s, right is: %s ",
                        leftValue.getClass().getSimpleName(),
                        rightValue.getClass().getSimpleName()));
    }

    @Override
    public void visit(MinorThanEquals expr) {
        expr.getLeftExpression().accept(this);
        Object leftValue = outputFields.getTail();
        expr.getRightExpression().accept(this);
        Object rightValue = outputFields.getTail();
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
                CommonErrorCode.UNSUPPORTED_OPERATION,
                String.format(
                        "Filed types not matched, left is: %s, right is: %s ",
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
    public void visit(Concat expr) {
        throw new IllegalArgumentException("Unsupported visit Concat");
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
    public void visit(CastExpression expr) {
        throw new IllegalArgumentException("Unsupported visit CastExpression");
    }

    @Override
    public void visit(TryCastExpression expr) {
        throw new IllegalArgumentException("Unsupported visit TryCastExpression");
    }

    @Override
    public void visit(Modulo expr) {
        throw new IllegalArgumentException("Unsupported visit Modulo");
    }

    @Override
    public void visit(AnalyticExpression expr) {
        throw new IllegalArgumentException("Unsupported visit AnalyticExpression");
    }

    @Override
    public void visit(ExtractExpression expr) {
        throw new IllegalArgumentException("Unsupported visit ExtractExpression");
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
    protected void visitBinaryExpression(BinaryExpression expr) {
        throw new IllegalArgumentException("Unsupported visit BinaryExpression");
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
