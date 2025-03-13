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

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.zeta.functions.ArrayFunction;

import org.apache.commons.collections4.CollectionUtils;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TrimFunction;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ZetaSQLType {
    public static final String DECIMAL = "DECIMAL";
    public static final String VARCHAR = "VARCHAR";
    public static final String STRING = "STRING";
    public static final String INT = "INT";
    public static final String INTEGER = "INTEGER";
    public static final String BIGINT = "BIGINT";
    public static final String LONG = "LONG";
    public static final String BYTE = "BYTE";
    public static final String BYTES = "BYTES";
    public static final String BINARY = "BINARY";
    public static final String DOUBLE = "DOUBLE";
    public static final String FLOAT = "FLOAT";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String DATETIME = "DATETIME";
    public static final String DATE = "DATE";
    public static final String TIME = "TIME";

    private final SeaTunnelRowType inputRowType;

    private final List<ZetaUDF> udfList;

    public ZetaSQLType(SeaTunnelRowType inputRowType, List<ZetaUDF> udfList) {
        this.inputRowType = inputRowType;
        this.udfList = udfList;
    }

    public SeaTunnelDataType<?> getExpressionType(Expression expression) {
        if (expression instanceof NullValue) {
            return BasicType.VOID_TYPE;
        }
        if (expression instanceof SignedExpression) {
            return getExpressionType(((SignedExpression) expression).getExpression());
        }
        if (expression instanceof DoubleValue) {
            return BasicType.DOUBLE_TYPE;
        }
        if (expression instanceof LongValue) {
            long longVal = ((LongValue) expression).getValue();
            if (longVal <= Integer.MAX_VALUE && longVal >= Integer.MIN_VALUE) {
                return BasicType.INT_TYPE;
            }
            return BasicType.LONG_TYPE;
        }
        if (expression instanceof StringValue) {
            return BasicType.STRING_TYPE;
        }
        if (expression instanceof Column) {
            Column columnExp = (Column) expression;
            String columnName = columnExp.getColumnName();
            int index = inputRowType.indexOf(columnName, false);
            if (index == -1
                    && columnName.startsWith(ZetaSQLEngine.ESCAPE_IDENTIFIER)
                    && columnName.endsWith(ZetaSQLEngine.ESCAPE_IDENTIFIER)) {
                columnName = columnName.substring(1, columnName.length() - 1);
                index = inputRowType.indexOf(columnName, false);
            }

            if (index != -1) {
                return inputRowType.getFieldType(index);
            } else {
                // fullback logical to handel struct query.
                String fullyQualifiedName = columnExp.getFullyQualifiedName();
                String[] columnNames = fullyQualifiedName.split("\\.");
                int deep = columnNames.length;
                SeaTunnelRowType parRowType = inputRowType;
                SeaTunnelDataType<?> filedTypeRes = null;
                for (int i = 0; i < deep; i++) {
                    String key = columnNames[i];
                    int idx = parRowType.indexOf(key, false);
                    if (idx == -1
                            && key.startsWith(ZetaSQLEngine.ESCAPE_IDENTIFIER)
                            && key.endsWith(ZetaSQLEngine.ESCAPE_IDENTIFIER)) {
                        key = key.substring(1, key.length() - 1);
                        idx = parRowType.indexOf(key, false);
                    }
                    if (idx == -1) {
                        throw new IllegalArgumentException(
                                String.format("can't find field [%s]", fullyQualifiedName));
                    }
                    filedTypeRes = parRowType.getFieldType(idx);
                    if (filedTypeRes instanceof SeaTunnelRowType) {
                        parRowType = (SeaTunnelRowType) filedTypeRes;
                    } else if (filedTypeRes instanceof MapType) {
                        if (i < deep - 2) {
                            throw new IllegalArgumentException(
                                    "For now, when you query map field with inner query, it must be latest field or latest struct field! Please modify your query!");
                        }
                        if (i == deep - 1) {
                            return filedTypeRes;
                        } else {
                            return ((MapType<?, ?>) filedTypeRes).getValueType();
                        }
                    }
                }
                return filedTypeRes;
            }
        }
        if (expression instanceof Function) {
            return getFunctionType((Function) expression);
        }
        if (expression instanceof TrimFunction) {
            return BasicType.STRING_TYPE;
        }
        if (expression instanceof TimeKeyExpression) {
            return getTimeKeyExprType((TimeKeyExpression) expression);
        }
        if (expression instanceof ExtractExpression) {
            return BasicType.INT_TYPE;
        }
        if (expression instanceof Parenthesis) {
            Parenthesis parenthesis = (Parenthesis) expression;
            return getExpressionType(parenthesis.getExpression());
        }
        if (expression instanceof Concat) {
            return BasicType.STRING_TYPE;
        }

        if (expression instanceof CaseExpression) {
            return getCaseType((CaseExpression) expression);
        }
        if (expression instanceof ComparisonOperator
                || expression instanceof IsNullExpression
                || expression instanceof InExpression
                || expression instanceof LikeExpression
                || expression instanceof AndExpression
                || expression instanceof OrExpression
                || expression instanceof NotEqualsTo) {
            return BasicType.BOOLEAN_TYPE;
        }

        if (expression instanceof CastExpression) {
            return getCastType((CastExpression) expression);
        }

        if (expression instanceof BinaryExpression) {
            BinaryExpression binaryExpression = (BinaryExpression) expression;
            SeaTunnelDataType<?> leftType = getExpressionType(binaryExpression.getLeftExpression());
            SeaTunnelDataType<?> rightType =
                    getExpressionType(binaryExpression.getRightExpression());
            if ((leftType.getSqlType() == SqlType.TINYINT
                            || leftType.getSqlType() == SqlType.SMALLINT
                            || leftType.getSqlType() == SqlType.INT)
                    && (rightType.getSqlType() == SqlType.TINYINT
                            || rightType.getSqlType() == SqlType.SMALLINT
                            || rightType.getSqlType() == SqlType.INT)) {
                return BasicType.INT_TYPE;
            }
            if ((leftType.getSqlType() == SqlType.TINYINT
                            || leftType.getSqlType() == SqlType.SMALLINT
                            || leftType.getSqlType() == SqlType.INT
                            || leftType.getSqlType() == SqlType.BIGINT)
                    && rightType.getSqlType() == SqlType.BIGINT) {
                return BasicType.LONG_TYPE;
            }
            if ((rightType.getSqlType() == SqlType.TINYINT
                            || rightType.getSqlType() == SqlType.SMALLINT
                            || rightType.getSqlType() == SqlType.INT
                            || rightType.getSqlType() == SqlType.BIGINT)
                    && leftType.getSqlType() == SqlType.BIGINT) {
                return BasicType.LONG_TYPE;
            }
            if (leftType.getSqlType() == SqlType.DECIMAL
                    || rightType.getSqlType() == SqlType.DECIMAL) {
                int precision = 0;
                int scale = 0;
                if (leftType.getSqlType() == SqlType.DECIMAL) {
                    DecimalType decimalType = (DecimalType) leftType;
                    precision = decimalType.getPrecision();
                    scale = decimalType.getScale();
                }
                if (rightType.getSqlType() == SqlType.DECIMAL) {
                    DecimalType decimalType = (DecimalType) rightType;
                    precision = Math.max(decimalType.getPrecision(), precision);
                    scale = Math.max(decimalType.getScale(), scale);
                }
                return new DecimalType(precision, scale);
            }
            if ((leftType.getSqlType() == SqlType.FLOAT || leftType.getSqlType() == SqlType.DOUBLE)
                    || (rightType.getSqlType() == SqlType.FLOAT
                            || rightType.getSqlType() == SqlType.DOUBLE)) {
                return BasicType.DOUBLE_TYPE;
            }
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported SQL Expression: %s ", expression.toString()));
    }

    public boolean isNumberType(SqlType type) {
        return type.compareTo(SqlType.TINYINT) >= 0 && type.compareTo(SqlType.DECIMAL) <= 0;
    }

    public SeaTunnelDataType<?> getMaxType(
            SeaTunnelDataType<?> leftType, SeaTunnelDataType<?> rightType) {
        if (leftType == null || BasicType.VOID_TYPE.equals(leftType)) {
            return rightType;
        }
        if (rightType == null || BasicType.VOID_TYPE.equals(rightType)) {
            return leftType;
        }
        if (leftType.equals(rightType)) {
            return leftType;
        }

        final boolean isAllNumber =
                isNumberType(leftType.getSqlType()) && isNumberType(rightType.getSqlType());
        if (!isAllNumber) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    leftType + " type not compatible " + rightType);
        }

        if (leftType.getSqlType() == SqlType.DECIMAL || rightType.getSqlType() == SqlType.DECIMAL) {
            int precision = 0;
            int scale = 0;
            if (leftType.getSqlType() == SqlType.DECIMAL) {
                DecimalType decimalType = (DecimalType) leftType;
                precision = decimalType.getPrecision();
                scale = decimalType.getScale();
            }
            if (rightType.getSqlType() == SqlType.DECIMAL) {
                DecimalType decimalType = (DecimalType) rightType;
                precision = Math.max(decimalType.getPrecision(), precision);
                scale = Math.max(decimalType.getScale(), scale);
            }
            return new DecimalType(precision, scale);
        }
        return leftType.getSqlType().compareTo(rightType.getSqlType()) <= 0 ? rightType : leftType;
    }

    public SeaTunnelDataType<?> getMaxType(Collection<SeaTunnelDataType<?>> types) {
        if (CollectionUtils.isEmpty(types)) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    "getMaxType parameter is null");
        }
        Iterator<SeaTunnelDataType<?>> iterator = types.iterator();
        SeaTunnelDataType<?> result = iterator.next();
        while (iterator.hasNext()) {
            result = getMaxType(result, iterator.next());
        }
        return result;
    }

    private SeaTunnelDataType<?> getCaseType(CaseExpression caseExpression) {
        final Collection<SeaTunnelDataType<?>> types =
                caseExpression.getWhenClauses().stream()
                        .map(WhenClause::getThenExpression)
                        .map(this::getExpressionType)
                        .collect(Collectors.toSet());
        if (caseExpression.getElseExpression() != null) {
            types.add(getExpressionType(caseExpression.getElseExpression()));
        }
        return getMaxType(types);
    }

    private SeaTunnelDataType<?> getCastType(CastExpression castExpression) {
        String dataType = castExpression.getColDataType().getDataType();
        switch (dataType.toUpperCase()) {
            case DECIMAL:
                List<String> ps = castExpression.getColDataType().getArgumentsStringList();
                return new DecimalType(Integer.parseInt(ps.get(0)), Integer.parseInt(ps.get(1)));
            case VARCHAR:
            case STRING:
                return BasicType.STRING_TYPE;
            case INT:
            case INTEGER:
                return BasicType.INT_TYPE;
            case BIGINT:
            case LONG:
                return BasicType.LONG_TYPE;
            case BYTE:
                return BasicType.BYTE_TYPE;
            case BYTES:
            case BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case TIMESTAMP:
            case DATETIME:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            default:
                throw new TransformException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        String.format("Unsupported CAST AS type: %s", dataType));
        }
    }

    private SeaTunnelDataType<?> getFunctionType(Function function) {
        switch (function.getName().toUpperCase()) {
            case ZetaSQLFunction.CHAR:
            case ZetaSQLFunction.CHR:
            case ZetaSQLFunction.CONCAT:
            case ZetaSQLFunction.CONCAT_WS:
            case ZetaSQLFunction.HEXTORAW:
            case ZetaSQLFunction.RAWTOHEX:
            case ZetaSQLFunction.INSERT:
            case ZetaSQLFunction.LOWER:
            case ZetaSQLFunction.LCASE:
            case ZetaSQLFunction.UPPER:
            case ZetaSQLFunction.UCASE:
            case ZetaSQLFunction.LEFT:
            case ZetaSQLFunction.RIGHT:
            case ZetaSQLFunction.LPAD:
            case ZetaSQLFunction.RPAD:
            case ZetaSQLFunction.LTRIM:
            case ZetaSQLFunction.RTRIM:
            case ZetaSQLFunction.TRIM:
            case ZetaSQLFunction.REGEXP_REPLACE:
            case ZetaSQLFunction.REGEXP_SUBSTR:
            case ZetaSQLFunction.REPEAT:
            case ZetaSQLFunction.REPLACE:
            case ZetaSQLFunction.SOUNDEX:
            case ZetaSQLFunction.SPACE:
            case ZetaSQLFunction.SUBSTRING:
            case ZetaSQLFunction.SUBSTR:
            case ZetaSQLFunction.TO_CHAR:
            case ZetaSQLFunction.TRANSLATE:
            case ZetaSQLFunction.DAYNAME:
            case ZetaSQLFunction.MONTHNAME:
            case ZetaSQLFunction.FORMATDATETIME:
            case ZetaSQLFunction.FROM_UNIXTIME:
            case ZetaSQLFunction.UUID:
                return BasicType.STRING_TYPE;
            case ZetaSQLFunction.ASCII:
            case ZetaSQLFunction.LOCATE:
            case ZetaSQLFunction.INSTR:
            case ZetaSQLFunction.POSITION:
            case ZetaSQLFunction.CEIL:
            case ZetaSQLFunction.CEILING:
            case ZetaSQLFunction.FLOOR:
            case ZetaSQLFunction.DAY_OF_MONTH:
            case ZetaSQLFunction.DAY_OF_WEEK:
            case ZetaSQLFunction.DAY_OF_YEAR:
            case ZetaSQLFunction.EXTRACT:
            case ZetaSQLFunction.HOUR:
            case ZetaSQLFunction.MINUTE:
            case ZetaSQLFunction.MONTH:
            case ZetaSQLFunction.QUARTER:
            case ZetaSQLFunction.SECOND:
            case ZetaSQLFunction.WEEK:
            case ZetaSQLFunction.YEAR:
            case ZetaSQLFunction.SIGN:
                return BasicType.INT_TYPE;
            case ZetaSQLFunction.BIT_LENGTH:
            case ZetaSQLFunction.CHAR_LENGTH:
            case ZetaSQLFunction.LENGTH:
            case ZetaSQLFunction.OCTET_LENGTH:
            case ZetaSQLFunction.DATEDIFF:
                return BasicType.LONG_TYPE;
            case ZetaSQLFunction.REGEXP_LIKE:
            case ZetaSQLFunction.IS_DATE:
                return BasicType.BOOLEAN_TYPE;
            case ZetaSQLFunction.ACOS:
            case ZetaSQLFunction.ASIN:
            case ZetaSQLFunction.ATAN:
            case ZetaSQLFunction.COS:
            case ZetaSQLFunction.COSH:
            case ZetaSQLFunction.COT:
            case ZetaSQLFunction.SIN:
            case ZetaSQLFunction.SINH:
            case ZetaSQLFunction.TAN:
            case ZetaSQLFunction.TANH:
            case ZetaSQLFunction.ATAN2:
            case ZetaSQLFunction.EXP:
            case ZetaSQLFunction.LN:
            case ZetaSQLFunction.LOG:
            case ZetaSQLFunction.LOG10:
            case ZetaSQLFunction.RADIANS:
            case ZetaSQLFunction.SQRT:
            case ZetaSQLFunction.PI:
            case ZetaSQLFunction.POWER:
            case ZetaSQLFunction.RAND:
            case ZetaSQLFunction.RANDOM:
            case ZetaSQLFunction.TRUNC:
            case ZetaSQLFunction.TRUNCATE:
                return BasicType.DOUBLE_TYPE;
            case ZetaSQLFunction.ARRAY:
                return ArrayFunction.castArrayTypeMapping(function, inputRowType);
            case ZetaSQLFunction.ARRAY_MAX:
            case ZetaSQLFunction.ARRAY_MIN:
                return ArrayFunction.getElementType(function, inputRowType);
            case ZetaSQLFunction.SPLIT:
                return ArrayType.STRING_ARRAY_TYPE;
            case ZetaSQLFunction.NOW:
            case ZetaSQLFunction.DATE_TRUNC:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case ZetaSQLFunction.PARSEDATETIME:
            case ZetaSQLFunction.TO_DATE:
                {
                    String format = function.getParameters().getExpressions().get(1).toString();
                    if (format.contains("yy") && format.contains("mm")) {
                        return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                    }
                    if (format.contains("yy")) {
                        return LocalTimeType.LOCAL_DATE_TYPE;
                    }
                    if (format.contains("mm")) {
                        return LocalTimeType.LOCAL_TIME_TYPE;
                    }
                    throw new TransformException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                            String.format(
                                    "Unknown pattern letter %s for function: %s",
                                    format, function.getName()));
                }
            case ZetaSQLFunction.ABS:
            case ZetaSQLFunction.DATEADD:
            case ZetaSQLFunction.TIMESTAMPADD:
            case ZetaSQLFunction.ROUND:
            case ZetaSQLFunction.NULLIF:
            case ZetaSQLFunction.COALESCE:
            case ZetaSQLFunction.IFNULL:
                // Result has the same type as first argument
                return getExpressionType(function.getParameters().getExpressions().get(0));
            case ZetaSQLFunction.MOD:
                // Result has the same type as second argument
                return getExpressionType(function.getParameters().getExpressions().get(1));
            default:
                for (ZetaUDF udf : udfList) {
                    if (udf.functionName().equalsIgnoreCase(function.getName())) {
                        List<SeaTunnelDataType<?>> argsType = new ArrayList<>();
                        ExpressionList expressionList = function.getParameters();
                        if (expressionList != null) {
                            List<Expression> expressions = expressionList.getExpressions();
                            if (expressions != null) {
                                for (Expression expression : expressions) {
                                    argsType.add(getExpressionType(expression));
                                }
                            }
                        }
                        return udf.resultType(argsType);
                    }
                }
                throw new TransformException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        String.format("Unsupported function: %s ", function.getName()));
        }
    }

    private SeaTunnelDataType<?> getTimeKeyExprType(TimeKeyExpression timeKeyExpression) {
        switch (timeKeyExpression.getStringValue().toUpperCase()) {
            case ZetaSQLFunction.CURRENT_DATE:
            case ZetaSQLFunction.CURRENT_DATE_P:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case ZetaSQLFunction.CURRENT_TIME:
            case ZetaSQLFunction.CURRENT_TIME_P:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case ZetaSQLFunction.CURRENT_TIMESTAMP:
            case ZetaSQLFunction.CURRENT_TIMESTAMP_P:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                throw new TransformException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        String.format(
                                "Unsupported TimeKey expression: %s ",
                                timeKeyExpression.getStringValue()));
        }
    }
}
