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

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.zeta.functions.ArrayFunction;
import org.apache.seatunnel.transform.sql.zeta.functions.DateTimeFunction;
import org.apache.seatunnel.transform.sql.zeta.functions.NumericFunction;
import org.apache.seatunnel.transform.sql.zeta.functions.StringFunction;
import org.apache.seatunnel.transform.sql.zeta.functions.SystemFunction;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

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
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.LateralView;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.UUID.randomUUID;
import static org.apache.seatunnel.transform.exception.TransformCommonErrorCode.INPUT_FIELDS_NOT_FOUND;

public class ZetaSQLFunction {
    // ============================internal functions=====================

    // -------------------------string functions----------------------------
    public static final String ASCII = "ASCII";
    public static final String BIT_LENGTH = "BIT_LENGTH";
    public static final String CHAR_LENGTH = "CHAR_LENGTH";
    public static final String LENGTH = "LENGTH";
    public static final String OCTET_LENGTH = "OCTET_LENGTH";
    public static final String CHAR = "CHAR";
    public static final String CHR = "CHR";
    public static final String CONCAT = "CONCAT";
    public static final String CONCAT_WS = "CONCAT_WS";
    public static final String HEXTORAW = "HEXTORAW";
    public static final String RAWTOHEX = "RAWTOHEX";
    public static final String INSERT = "INSERT";
    public static final String LOWER = "LOWER";
    public static final String LCASE = "LCASE";
    public static final String BINARY = "BINARY";
    public static final String BYTE = "BYTE";
    public static final String UPPER = "UPPER";
    public static final String UCASE = "UCASE";
    public static final String LEFT = "LEFT";
    public static final String RIGHT = "RIGHT";
    public static final String LOCATE = "LOCATE";
    public static final String INSTR = "INSTR";
    public static final String POSITION = "POSITION";
    public static final String LPAD = "LPAD";
    public static final String RPAD = "RPAD";
    public static final String LTRIM = "LTRIM";
    public static final String RTRIM = "RTRIM";
    public static final String TRIM = "TRIM";
    public static final String REGEXP_REPLACE = "REGEXP_REPLACE";
    public static final String REGEXP_LIKE = "REGEXP_LIKE";
    public static final String REGEXP_SUBSTR = "REGEXP_SUBSTR";
    public static final String REPEAT = "REPEAT";
    public static final String REPLACE = "REPLACE";
    public static final String SOUNDEX = "SOUNDEX";
    public static final String SPACE = "SPACE";
    public static final String SUBSTRING = "SUBSTRING";
    public static final String SUBSTR = "SUBSTR";
    public static final String TO_CHAR = "TO_CHAR";
    public static final String TRANSLATE = "TRANSLATE";
    public static final String SPLIT = "SPLIT";

    // -------------------------numeric functions----------------------------
    public static final String ABS = "ABS";
    public static final String ACOS = "ACOS";
    public static final String ASIN = "ASIN";
    public static final String ATAN = "ATAN";
    public static final String COS = "COS";
    public static final String COSH = "COSH";
    public static final String COT = "COT";
    public static final String SIN = "SIN";
    public static final String SINH = "SINH";
    public static final String TAN = "TAN";
    public static final String TANH = "TANH";
    public static final String ATAN2 = "ATAN2";
    public static final String MOD = "MOD";
    public static final String CEIL = "CEIL";
    public static final String CEILING = "CEILING";
    public static final String EXP = "EXP";
    public static final String FLOOR = "FLOOR";
    public static final String LN = "LN";
    public static final String LOG = "LOG";
    public static final String LOG10 = "LOG10";
    public static final String RADIANS = "RADIANS";
    public static final String SQRT = "SQRT";
    public static final String PI = "PI";
    public static final String POWER = "POWER";
    public static final String RAND = "RAND";
    public static final String RANDOM = "RANDOM";
    public static final String ROUND = "ROUND";
    public static final String SIGN = "SIGN";
    public static final String TRUNC = "TRUNC";
    public static final String TRUNCATE = "TRUNCATE";
    public static final String ARRAY_MAX = "ARRAY_MAX";
    public static final String ARRAY_MIN = "ARRAY_MIN";

    // -------------------------time and date functions----------------------------
    public static final String CURRENT_DATE = "CURRENT_DATE";
    public static final String CURRENT_DATE_P = "CURRENT_DATE()";
    public static final String CURRENT_TIME = "CURRENT_TIME";
    public static final String CURRENT_TIME_P = "CURRENT_TIME()";
    public static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
    public static final String CURRENT_TIMESTAMP_P = "CURRENT_TIMESTAMP()";
    public static final String NOW = "NOW";
    public static final String DATEADD = "DATEADD";
    public static final String TIMESTAMPADD = "TIMESTAMPADD";
    public static final String DATEDIFF = "DATEDIFF";
    public static final String DATE_TRUNC = "DATE_TRUNC";
    public static final String DAYNAME = "DAYNAME";
    public static final String DAY_OF_MONTH = "DAY_OF_MONTH";
    public static final String DAY_OF_WEEK = "DAY_OF_WEEK";
    public static final String DAY_OF_YEAR = "DAY_OF_YEAR";
    public static final String EXTRACT = "EXTRACT";
    public static final String FORMATDATETIME = "FORMATDATETIME";
    public static final String HOUR = "HOUR";
    public static final String MINUTE = "MINUTE";
    public static final String MONTH = "MONTH";
    public static final String MONTHNAME = "MONTHNAME";
    public static final String PARSEDATETIME = "PARSEDATETIME";
    public static final String TO_DATE = "TO_DATE";
    public static final String IS_DATE = "IS_DATE";
    public static final String QUARTER = "QUARTER";
    public static final String SECOND = "SECOND";
    public static final String WEEK = "WEEK";
    public static final String YEAR = "YEAR";
    public static final String FROM_UNIXTIME = "FROM_UNIXTIME";

    // -------------------------lateralView functions----------------------------
    public static final String EXPLODE = "EXPLODE";
    public static final String ARRAY = "ARRAY";

    // -------------------------system functions----------------------------
    public static final String COALESCE = "COALESCE";
    public static final String IFNULL = "IFNULL";
    public static final String NULLIF = "NULLIF";

    public static final String UUID = "UUID";

    private final SeaTunnelRowType inputRowType;

    private final ZetaSQLType zetaSQLType;
    private final ZetaSQLFilter zetaSQLFilter;

    private final List<ZetaUDF> udfList;

    public ZetaSQLFunction(
            SeaTunnelRowType inputRowType, ZetaSQLType zetaSQLType, List<ZetaUDF> udfList) {
        this.inputRowType = inputRowType;
        this.zetaSQLType = zetaSQLType;
        this.zetaSQLFilter = new ZetaSQLFilter(this, zetaSQLType);
        this.udfList = udfList;
    }

    public Object computeForValue(Expression expression, Object[] inputFields) {
        if (expression instanceof NullValue) {
            return null;
        }
        if (expression instanceof TrimFunction) {
            TrimFunction function = (TrimFunction) expression;
            Column column = (Column) function.getExpression();
            List<Object> functionArgs = new ArrayList<>();
            if (column != null) {
                functionArgs.add(computeForValue(column, inputFields));
                if (function.getFromExpression() != null) {
                    functionArgs.add(((StringValue) function.getFromExpression()).getValue());
                }
            }
            return executeFunctionExpr(TRIM, functionArgs);
        }
        if (expression instanceof SignedExpression) {
            SignedExpression signedExpression = (SignedExpression) expression;
            if (signedExpression.getSign() == '-') {
                Object value = computeForValue(signedExpression.getExpression(), inputFields);
                if (value instanceof Integer) {
                    return -((Integer) value);
                }
                if (value instanceof Long) {
                    return -((Long) value);
                }
                if (value instanceof Double) {
                    return -((Double) value);
                }
                if (value instanceof Number) {
                    return -((Number) value).doubleValue();
                }
            } else {
                return computeForValue(signedExpression, inputFields);
            }
        }
        if (expression instanceof DoubleValue) {
            return ((DoubleValue) expression).getValue();
        }
        if (expression instanceof LongValue) {
            long longVal = ((LongValue) expression).getValue();
            if (longVal <= Integer.MAX_VALUE && longVal >= Integer.MIN_VALUE) {
                return (int) longVal;
            } else {
                return longVal;
            }
        }
        if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
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
                return inputFields[index];
            } else {
                String fullyQualifiedName = columnExp.getFullyQualifiedName();
                String[] columnNames = fullyQualifiedName.split("\\.");
                int deep = columnNames.length;
                SeaTunnelDataType parDataType = inputRowType;
                SeaTunnelRow parRowValues = new SeaTunnelRow(inputFields);
                Object res = parRowValues;
                for (int i = 0; i < deep; i++) {
                    String key = columnNames[i];
                    if (parDataType instanceof MapType) {
                        Map<String, Object> mapValue = ((Map) res);
                        if (mapValue.containsKey(key)) {
                            return mapValue.get(key);
                        } else if (key.startsWith(ZetaSQLEngine.ESCAPE_IDENTIFIER)
                                && key.endsWith(ZetaSQLEngine.ESCAPE_IDENTIFIER)) {
                            key = key.substring(1, key.length() - 1);
                            return mapValue.get(key);
                        }
                        return null;
                    }
                    parRowValues = (SeaTunnelRow) res;
                    int idx = ((SeaTunnelRowType) parDataType).indexOf(key, false);
                    if (idx == -1
                            && key.startsWith(ZetaSQLEngine.ESCAPE_IDENTIFIER)
                            && key.endsWith(ZetaSQLEngine.ESCAPE_IDENTIFIER)) {
                        key = key.substring(1, key.length() - 1);
                        idx = ((SeaTunnelRowType) parDataType).indexOf(key, false);
                    }
                    if (idx == -1) {
                        throw new IllegalArgumentException(
                                String.format("can't find field [%s]", fullyQualifiedName));
                    }
                    parDataType = ((SeaTunnelRowType) parDataType).getFieldType(idx);
                    res = parRowValues.getFields()[idx];
                }
                return res;
            }
        }
        if (expression instanceof Function) {
            Function function = (Function) expression;
            ExpressionList<Expression> expressionList =
                    (ExpressionList<Expression>) function.getParameters();
            List<Object> functionArgs = new ArrayList<>();
            if (expressionList != null) {
                for (Expression funcArgExpression : expressionList.getExpressions()) {
                    functionArgs.add(computeForValue(funcArgExpression, inputFields));
                }
            }
            return executeFunctionExpr(function.getName(), functionArgs);
        }
        if (expression instanceof TimeKeyExpression) {
            return executeTimeKeyExpr(((TimeKeyExpression) expression).getStringValue());
        }
        if (expression instanceof ExtractExpression) {
            ExtractExpression extract = (ExtractExpression) expression;
            List<Object> functionArgs = new ArrayList<>();
            functionArgs.add(computeForValue(extract.getExpression(), inputFields));
            functionArgs.add(extract.getName());
            return executeFunctionExpr(ZetaSQLFunction.EXTRACT, functionArgs);
        }
        if (expression instanceof Parenthesis) {
            Parenthesis parenthesis = (Parenthesis) expression;
            return computeForValue(parenthesis.getExpression(), inputFields);
        }
        // bytes not supported at the moment,use BINARY instead.
        if (expression instanceof CaseExpression) {
            CaseExpression caseExpression = (CaseExpression) expression;
            final Object value = executeCaseExpr(caseExpression, inputFields);
            SeaTunnelDataType<?> type = zetaSQLType.getExpressionType(expression);
            return SystemFunction.castAs(value, type);
        }
        if (expression instanceof BinaryExpression) {
            return executeBinaryExpr((BinaryExpression) expression, inputFields);
        }
        if (expression instanceof CastExpression) {
            CastExpression castExpression = (CastExpression) expression;
            Expression leftExpr = castExpression.getLeftExpression();
            Object leftValue = computeForValue(leftExpr, inputFields);
            return executeCastExpr(castExpression, leftValue);
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported SQL Expression: %s ", expression.toString()));
    }

    public Object executeCaseExpr(CaseExpression caseExpression, Object[] inputFields) {
        Expression switchExpr = caseExpression.getSwitchExpression();
        Object switchValue = switchExpr == null ? null : computeForValue(switchExpr, inputFields);
        for (WhenClause whenClause : caseExpression.getWhenClauses()) {
            Expression whenExpression = whenClause.getWhenExpression();
            final Object when =
                    zetaSQLFilter.isConditionExpr(whenExpression)
                            ? zetaSQLFilter.executeFilter(whenExpression, inputFields)
                            : computeForValue(whenExpression, inputFields);
            // match: case [column] when column1 compare other, add by javalover123
            if (when instanceof Boolean && (boolean) when) {
                return computeForValue(whenClause.getThenExpression(), inputFields);
            } else if (zetaSQLFilter.equalsToExpr(Pair.of(switchValue, when))) {
                return computeForValue(whenClause.getThenExpression(), inputFields);
            }
        }
        final Expression elseExpression = caseExpression.getElseExpression();
        return elseExpression == null ? null : computeForValue(elseExpression, inputFields);
    }

    public Object executeFunctionExpr(String functionName, List<Object> args) {
        switch (functionName.toUpperCase()) {
            case ASCII:
                return StringFunction.ascii(args);
            case BIT_LENGTH:
                return StringFunction.bitLength(args);
            case CHAR_LENGTH:
            case LENGTH:
                return StringFunction.charLength(args);
            case OCTET_LENGTH:
                return StringFunction.octetLength(args);
            case CHAR:
            case CHR:
                return StringFunction.chr(args);
            case CONCAT:
                return StringFunction.concat(args);
            case CONCAT_WS:
                return StringFunction.concatWs(args);
            case HEXTORAW:
                return StringFunction.hextoraw(args);
            case RAWTOHEX:
                return StringFunction.rawtohex(args);
            case INSERT:
                return StringFunction.insert(args);
            case LOWER:
            case LCASE:
                return StringFunction.lower(args);
            case UPPER:
            case UCASE:
                return StringFunction.upper(args);
            case LEFT:
                return StringFunction.left(args);
            case RIGHT:
                return StringFunction.right(args);
            case LOCATE:
            case POSITION:
                return StringFunction.location(functionName, args);
            case INSTR:
                return StringFunction.instr(args);
            case LPAD:
            case RPAD:
                return StringFunction.pad(functionName, args);
            case LTRIM:
                return StringFunction.ltrim(args);
            case RTRIM:
                return StringFunction.rtrim(args);
            case TRIM:
                return StringFunction.trim(args);
            case REGEXP_REPLACE:
                return StringFunction.regexpReplace(args);
            case REGEXP_LIKE:
                return StringFunction.regexpLike(args);
            case REGEXP_SUBSTR:
                return StringFunction.regexpSubstr(args);
            case REPEAT:
                return StringFunction.repeat(args);
            case REPLACE:
                return StringFunction.replace(args);
            case SOUNDEX:
                return StringFunction.soundex(args);
            case SPACE:
                return StringFunction.space(args);
            case SUBSTRING:
            case SUBSTR:
                return StringFunction.substring(args);
            case TO_CHAR:
                return StringFunction.toChar(args);
            case TRANSLATE:
                return StringFunction.translate(args);
            case SPLIT:
                return StringFunction.split(args);
            case ABS:
                return NumericFunction.abs(args);
            case ACOS:
                return NumericFunction.acos(args);
            case ASIN:
                return NumericFunction.asin(args);
            case ATAN:
                return NumericFunction.atan(args);
            case COS:
                return NumericFunction.cos(args);
            case COSH:
                return NumericFunction.cosh(args);
            case COT:
                return NumericFunction.cot(args);
            case SIN:
                return NumericFunction.sin(args);
            case SINH:
                return NumericFunction.sinh(args);
            case TAN:
                return NumericFunction.tan(args);
            case TANH:
                return NumericFunction.tanh(args);
            case ATAN2:
                return NumericFunction.atan2(args);
            case MOD:
                return NumericFunction.mod(args);
            case CEIL:
            case CEILING:
                return NumericFunction.ceil(args);
            case EXP:
                return NumericFunction.exp(args);
            case FLOOR:
                return NumericFunction.floor(args);
            case LN:
                return NumericFunction.ln(args);
            case LOG:
                return NumericFunction.log(args);
            case LOG10:
                return NumericFunction.log10(args);
            case RADIANS:
                return NumericFunction.radians(args);
            case SQRT:
                return NumericFunction.sqrt(args);
            case PI:
                return NumericFunction.pi(args);
            case POWER:
                return NumericFunction.power(args);
            case RAND:
            case RANDOM:
                return NumericFunction.random(args);
            case ROUND:
                return NumericFunction.round(args);
            case SIGN:
                return NumericFunction.sign(args);
            case TRUNC:
            case TRUNCATE:
                return NumericFunction.trunc(args);
            case NOW:
                return DateTimeFunction.currentTimestamp();
            case DATEADD:
            case TIMESTAMPADD:
                return DateTimeFunction.dateadd(args);
            case DATEDIFF:
                return DateTimeFunction.datediff(args);
            case DATE_TRUNC:
                return DateTimeFunction.dateTrunc(args);
            case DAYNAME:
                return DateTimeFunction.dayname(args);
            case DAY_OF_MONTH:
                return DateTimeFunction.dayOfMonth(args);
            case DAY_OF_WEEK:
                return DateTimeFunction.dayOfWeek(args);
            case DAY_OF_YEAR:
                return DateTimeFunction.dayOfYear(args);
            case FROM_UNIXTIME:
                return DateTimeFunction.fromUnixTime(args);
            case EXTRACT:
                return DateTimeFunction.extract(args);
            case FORMATDATETIME:
                return DateTimeFunction.formatdatetime(args);
            case HOUR:
                return DateTimeFunction.hour(args);
            case MINUTE:
                return DateTimeFunction.minute(args);
            case MONTH:
                return DateTimeFunction.month(args);
            case MONTHNAME:
                return DateTimeFunction.monthname(args);
            case PARSEDATETIME:
            case TO_DATE:
                return DateTimeFunction.parsedatetime(args);
            case IS_DATE:
                return DateTimeFunction.isDate(args);
            case QUARTER:
                return DateTimeFunction.quarter(args);
            case SECOND:
                return DateTimeFunction.second(args);
            case WEEK:
                return DateTimeFunction.week(args);
            case YEAR:
                return DateTimeFunction.year(args);
            case COALESCE:
                return SystemFunction.coalesce(args);
            case IFNULL:
                return SystemFunction.ifnull(args);
            case NULLIF:
                return SystemFunction.nullif(args);
            case ARRAY:
                return ArrayFunction.array(args);
            case ARRAY_MAX:
                return ArrayFunction.arrayMax(args);
            case ARRAY_MIN:
                return ArrayFunction.arrayMin(args);
            case UUID:
                return randomUUID().toString();
            default:
                for (ZetaUDF udf : udfList) {
                    if (udf.functionName().equalsIgnoreCase(functionName)) {
                        return udf.evaluate(args);
                    }
                }
                throw new TransformException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        String.format("Unsupported function: %s", functionName));
        }
    }

    public Object executeTimeKeyExpr(String timeKeyExpr) {
        switch (timeKeyExpr.toUpperCase()) {
            case CURRENT_DATE:
            case CURRENT_DATE_P:
                return DateTimeFunction.currentDate();
            case CURRENT_TIME:
            case CURRENT_TIME_P:
                return DateTimeFunction.currentTime();
            case CURRENT_TIMESTAMP:
            case CURRENT_TIMESTAMP_P:
                return DateTimeFunction.currentTimestamp();
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported TimeKey expression: %s", timeKeyExpr));
    }

    public Object executeCastExpr(CastExpression castExpression, Object arg) {
        String dataType = castExpression.getColDataType().getDataType();
        List<Object> args = new ArrayList<>(2);
        args.add(arg);
        args.add(dataType.toUpperCase());
        if (dataType.equalsIgnoreCase("DECIMAL")) {
            List<String> ps = castExpression.getColDataType().getArgumentsStringList();
            args.add(Integer.parseInt(ps.get(0)));
            args.add(Integer.parseInt(ps.get(1)));
        }
        return SystemFunction.castAs(args);
    }

    private Object executeBinaryExpr(BinaryExpression binaryExpression, Object[] inputFields) {
        if (binaryExpression instanceof Concat) {
            Concat concat = (Concat) binaryExpression;
            Expression leftExpr = concat.getLeftExpression();
            Expression rightExpr = concat.getRightExpression();
            Function function = new Function();
            function.setName(ZetaSQLFunction.CONCAT);
            ExpressionList expressionList = new ExpressionList();
            expressionList.setExpressions(new ArrayList<>());
            expressionList.getExpressions().add(leftExpr);
            expressionList.getExpressions().add(rightExpr);
            function.setParameters(expressionList);
            return computeForValue(function, inputFields);
        }
        Number leftValue =
                (Number) computeForValue(binaryExpression.getLeftExpression(), inputFields);
        Number rightValue =
                (Number) computeForValue(binaryExpression.getRightExpression(), inputFields);
        if (leftValue == null || rightValue == null) {
            return null;
        }
        SeaTunnelDataType<?> resultType = zetaSQLType.getExpressionType(binaryExpression);
        if (resultType.getSqlType() == SqlType.INT) {
            if (binaryExpression instanceof Addition) {
                return leftValue.intValue() + rightValue.intValue();
            }
            if (binaryExpression instanceof Subtraction) {
                return leftValue.intValue() - rightValue.intValue();
            }
            if (binaryExpression instanceof Multiplication) {
                return leftValue.intValue() * rightValue.intValue();
            }
            if (binaryExpression instanceof Division) {
                return leftValue.intValue() / rightValue.intValue();
            }
            if (binaryExpression instanceof Modulo) {
                return leftValue.intValue() % rightValue.intValue();
            }
        }
        if (resultType.getSqlType() == SqlType.DECIMAL) {
            BigDecimal bigDecimal = BigDecimal.valueOf(leftValue.doubleValue());
            if (binaryExpression instanceof Addition) {
                return bigDecimal.add(BigDecimal.valueOf(rightValue.doubleValue()));
            }
            if (binaryExpression instanceof Subtraction) {
                return bigDecimal.subtract(BigDecimal.valueOf(rightValue.doubleValue()));
            }
            if (binaryExpression instanceof Multiplication) {
                return bigDecimal.multiply(BigDecimal.valueOf(rightValue.doubleValue()));
            }
            if (binaryExpression instanceof Division) {
                DecimalType decimalType = (DecimalType) resultType;
                return bigDecimal.divide(
                        BigDecimal.valueOf(rightValue.doubleValue()),
                        decimalType.getScale(),
                        RoundingMode.UP);
            }
            if (binaryExpression instanceof Modulo) {
                List<Object> args = new ArrayList<>();
                args.add(leftValue);
                args.add(rightValue);
                return NumericFunction.mod(args);
            }
        }
        if (resultType.getSqlType() == SqlType.DOUBLE) {
            if (binaryExpression instanceof Addition) {
                return leftValue.doubleValue() + rightValue.doubleValue();
            }
            if (binaryExpression instanceof Subtraction) {
                return leftValue.doubleValue() - rightValue.doubleValue();
            }
            if (binaryExpression instanceof Multiplication) {
                return leftValue.doubleValue() * rightValue.doubleValue();
            }
            if (binaryExpression instanceof Division) {
                return leftValue.doubleValue() / rightValue.doubleValue();
            }
            if (binaryExpression instanceof Modulo) {
                return leftValue.doubleValue() % rightValue.doubleValue();
            }
        }
        if (resultType.getSqlType() == SqlType.BIGINT) {
            if (binaryExpression instanceof Addition) {
                return leftValue.longValue() + rightValue.longValue();
            }
            if (binaryExpression instanceof Subtraction) {
                return leftValue.longValue() - rightValue.longValue();
            }
            if (binaryExpression instanceof Multiplication) {
                return leftValue.longValue() * rightValue.longValue();
            }
            if (binaryExpression instanceof Division) {
                return leftValue.longValue() / rightValue.longValue();
            }
            if (binaryExpression instanceof Modulo) {
                return leftValue.longValue() % rightValue.longValue();
            }
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported SQL Expression: %s ", binaryExpression));
    }

    public List<SeaTunnelRow> lateralView(
            List<SeaTunnelRow> seaTunnelRows,
            List<LateralView> lateralViews,
            SeaTunnelRowType outRowType) {
        for (LateralView lateralView : lateralViews) {
            Function function = lateralView.getGeneratorFunction();
            boolean isUsingOuter = lateralView.isUsingOuter();
            String functionName = function.getName();
            String alias = lateralView.getColumnAlias().getName();
            if (EXPLODE.equalsIgnoreCase(functionName)) {
                seaTunnelRows = explode(seaTunnelRows, function, outRowType, isUsingOuter, alias);
            } else {
                throw new SeaTunnelRuntimeException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        "Transform config error! UnSupport function:" + functionName);
            }
        }

        return seaTunnelRows;
    }

    private List<SeaTunnelRow> explode(
            List<SeaTunnelRow> seaTunnelRows,
            Function lateralViewFunction,
            SeaTunnelRowType outRowType,
            boolean isUsingOuter,
            String alias) {
        ExpressionList<?> expressions = lateralViewFunction.getParameters();
        int aliasFieldIndex = outRowType.indexOf(alias);
        for (Expression expression : expressions) {
            if (expression instanceof Column) {
                String column = ((Column) expression).getColumnName();
                List<SeaTunnelRow> next = new ArrayList<>();
                for (SeaTunnelRow row : seaTunnelRows) {
                    int fieldIndex = outRowType.indexOf(column);
                    Object splitFieldValue = row.getField(fieldIndex);
                    transformExplodeValue(
                            splitFieldValue,
                            outRowType,
                            isUsingOuter,
                            next,
                            aliasFieldIndex,
                            row,
                            expression);
                }
                seaTunnelRows = next;
            } else if (expression instanceof Function) {
                List<SeaTunnelRow> next = new ArrayList<>();
                for (SeaTunnelRow row : seaTunnelRows) {
                    Object splitFieldValue = computeForValue(expression, row.getFields());
                    transformExplodeValue(
                            splitFieldValue,
                            outRowType,
                            isUsingOuter,
                            next,
                            aliasFieldIndex,
                            row,
                            expression);
                }
                seaTunnelRows = next;
            }
        }
        return seaTunnelRows;
    }

    private void transformExplodeValue(
            Object splitFieldValue,
            SeaTunnelRowType outRowType,
            boolean isUsingOuter,
            List<SeaTunnelRow> next,
            int aliasFieldIndex,
            SeaTunnelRow row,
            Expression expression) {
        if (splitFieldValue == null) {
            if (isUsingOuter) {
                next.add(
                        copySeaTunnelRowWithNewValue(
                                outRowType.getTotalFields(), row, aliasFieldIndex, null));
            }
            return;
        }
        if (splitFieldValue.getClass().isArray()) {
            if (ArrayUtils.isEmpty((Object[]) splitFieldValue)) {
                if (isUsingOuter) {
                    next.add(
                            copySeaTunnelRowWithNewValue(
                                    outRowType.getTotalFields(), row, aliasFieldIndex, null));
                }
                return;
            }
            for (Object fieldValue : (Object[]) splitFieldValue) {

                if (!isUsingOuter && fieldValue == null) {
                    continue;
                }
                next.add(
                        copySeaTunnelRowWithNewValue(
                                outRowType.getTotalFields(), row, aliasFieldIndex, fieldValue));
            }
        } else {
            throw new SeaTunnelRuntimeException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    "Transform config error! UnSupport explode function:"
                            + ((Function) expression).getName());
        }
    }

    private SeaTunnelRow copySeaTunnelRowWithNewValue(
            int length, SeaTunnelRow row, int fieldIndex, Object fieldValue) {
        Object[] fields = new Object[length];
        System.arraycopy(row.getFields(), 0, fields, 0, row.getFields().length);
        SeaTunnelRow outputRow = new SeaTunnelRow(fields);
        outputRow.setRowKind(row.getRowKind());
        outputRow.setTableId(row.getTableId());
        outputRow.setField(fieldIndex, fieldValue);
        return outputRow;
    }

    public SeaTunnelRowType lateralViewMapping(
            String[] fieldNames,
            SeaTunnelDataType<?>[] seaTunnelDataTypes,
            List<LateralView> lateralViews,
            List<String> inputColumnsMapping) {
        for (LateralView lateralView : lateralViews) {
            Function function = lateralView.getGeneratorFunction();
            String functionName = function.getName();
            String alias = lateralView.getColumnAlias().getName();
            if (EXPLODE.equalsIgnoreCase(functionName)) {
                ExpressionList<?> expressions = function.getParameters();
                int aliasIndex = Arrays.asList(fieldNames).indexOf(alias);
                for (Expression expression : expressions) {
                    if (expression instanceof Column) {
                        String column = ((Column) expression).getColumnName();
                        int columnIndex = Arrays.asList(fieldNames).indexOf(column);
                        if (columnIndex == -1) {
                            throw new TransformException(
                                    INPUT_FIELDS_NOT_FOUND,
                                    "Lateral view field must be in select item:" + fieldNames);
                        }
                        ArrayType arrayType = (ArrayType) seaTunnelDataTypes[columnIndex];
                        SeaTunnelDataType seaTunnelDataType =
                                PhysicalColumn.of(
                                                column,
                                                arrayType.getElementType(),
                                                200,
                                                true,
                                                "",
                                                "")
                                        .getDataType();
                        if (aliasIndex == -1) {
                            fieldNames = ArrayUtils.add(fieldNames, alias);
                            seaTunnelDataTypes =
                                    ArrayUtils.add(seaTunnelDataTypes, seaTunnelDataType);
                            inputColumnsMapping.add(alias);
                        } else {
                            seaTunnelDataTypes[columnIndex] = seaTunnelDataType;
                        }
                    } else {

                        ArrayType arrayType = (ArrayType) zetaSQLType.getExpressionType(expression);

                        if (aliasIndex == -1) {
                            fieldNames = ArrayUtils.add(fieldNames, alias);
                            seaTunnelDataTypes =
                                    ArrayUtils.add(seaTunnelDataTypes, arrayType.getElementType());
                            inputColumnsMapping.add(alias);
                        }
                    }
                }
            } else {
                throw new SeaTunnelRuntimeException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        "Transform config error! UnSupport function:" + functionName);
            }
        }
        return new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
    }
}
