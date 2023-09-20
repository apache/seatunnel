package org.apache.seatunnel.transform.sql.zeta;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.transform.exception.TransformException;

import lombok.Getter;
import net.sf.jsqlparser.expression.ArrayExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class ZetaExpressionVisitor extends ExpressionVisitorAdapter {
    private final SeaTunnelRowType inputRowType;
    private final Object[] inputFields;

    private Object value;
    private SeaTunnelDataType<?> type;

    public ZetaExpressionVisitor(SeaTunnelRowType inputRowType, Object[] inputFields) {
        this.inputRowType = inputRowType;
        this.inputFields = inputFields;
    }

    @Override
    public void visit(StringValue value) {
        this.value = value.getValue();
    }

    @Override
    public void visit(LongValue value) {
        this.value = value.getValue();
    }

    @Override
    public void visit(DoubleValue value) {
        this.value = value.getValue();
    }

    @Override
    public void visit(Addition expr) {
        Expression leftExpression = expr.getLeftExpression();
        Expression rightExpression = expr.getRightExpression();

        Long left = this.parseCalculateItem(leftExpression);
        Long right = this.parseCalculateItem(rightExpression);

        value = left + right;
    }

    @Override
    public void visit(Subtraction expr) {
        Expression leftExpression = expr.getLeftExpression();
        Expression rightExpression = expr.getRightExpression();

        Long left = this.parseCalculateItem(leftExpression);
        Long right = this.parseCalculateItem(rightExpression);

        value = left - right;
    }

    @Override
    public void visit(Multiplication expr) {
        Expression leftExpression = expr.getLeftExpression();
        Expression rightExpression = expr.getRightExpression();

        Long left = this.parseCalculateItem(leftExpression);
        Long right = this.parseCalculateItem(rightExpression);

        value = left * right;
    }

    @Override
    public void visit(Division expr) {
        Expression leftExpression = expr.getLeftExpression();
        Expression rightExpression = expr.getRightExpression();

        Long left = this.parseCalculateItem(leftExpression);
        Long right = this.parseCalculateItem(rightExpression);

        value = left / right;
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(Column column) {
        String columnName = column.getColumnName();
        Table table = column.getTable();
        if (null == table) {
            int index = inputRowType.indexOf(columnName);
            type = inputRowType.getFieldType(index);
            if (null != inputFields) {
                value = inputFields[index];
            }
        } else {
            List<String> keys =
                    Arrays.stream(table.getFullyQualifiedName().split("\\."))
                            .collect(Collectors.toList());
            keys.add(columnName);
            int currentIndex = inputRowType.indexOf(keys.get(0));
            SeaTunnelDataType<?> currentDataType = inputRowType.getFieldType(currentIndex);
            Object currentFieldData = null;
            if (null != inputFields) {
                currentFieldData = inputFields[currentIndex];
            }
            SeaTunnelRowType currentRowType;

            String errorKey = null;
            for (int i = 1; i < keys.size(); i++) {
                if (currentDataType instanceof SeaTunnelRowType) {
                    currentRowType = (SeaTunnelRowType) currentDataType;
                    currentIndex = currentRowType.indexOf(keys.get(i));
                    SeaTunnelRow row = (SeaTunnelRow) currentFieldData;
                    if (i == keys.size() - 1) {
                        if (null != row) {
                            value = row.getField(currentIndex);
                        }
                        type = currentRowType.getFieldType(currentIndex);
                        break;
                    }
                    if (null != row) {
                        currentFieldData = row.getField(currentIndex);
                    }
                    currentDataType = currentRowType.getFieldType(currentIndex);
                } else {
                    errorKey = keys.get(i - 1);
                    break;
                }
            }

            if (null != errorKey) {
                throw new TransformException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format("The column is not row type: %s ", errorKey));
            }
        }
    }

    @Override
    public void visit(ArrayExpression array) {
        Expression indexExpression = array.getIndexExpression();
        Expression objExpression = array.getObjExpression();
        String objectName = objExpression.toString();

        int fieldIndex = inputRowType.indexOf(objectName);
        SeaTunnelDataType<?> fieldType = inputRowType.getFieldType(fieldIndex);
        Object fieldData = null;
        if (null != inputFields) {
            fieldData = inputFields[fieldIndex];
        }

        indexExpression.accept(this);
        Object key = value;

        if (fieldType instanceof MapType && null != key) {
            Map<?, ?> map = (Map<?, ?>) fieldData;
            if (null != fieldData) {
                Object result = map.get(key);
                // json serialization key will be parsed to string
                value = result == null ? map.get(key.toString()) : result;
            }
            type = ((MapType<?, ?>) fieldType).getValueType();
        } else if (fieldType instanceof ArrayType && key instanceof Long) {
            int index = Integer.parseInt(key.toString());
            Object result = null;
            try {
                if (null != fieldData) {
                    result = ((Object[]) fieldData)[index];
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                // do nothing
            }
            value = result;
            type = ((ArrayType<?, ?>) fieldType).getElementType();
        } else {
            throw new TransformException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    String.format(
                            "The column %s type is %s, wrong operation %s[%s]",
                            objectName, fieldType, objectName, indexExpression));
        }
    }

    public Long parseCalculateItem(Expression expression) {
        expression.accept(this);

        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            throw new TransformException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "The array index calculate item must be integer");
        } catch (NullPointerException e) {
            throw new TransformException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "The array index calculation currently does not support referencing columns");
        }
    }
}
