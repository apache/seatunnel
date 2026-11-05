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

import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.zeta.functions.NumericFunction;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
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
import net.sf.jsqlparser.schema.Column;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.regex.Pattern;

public class ZetaSQLFilter {
    private final ZetaSQLFunction zetaSQLFunction;
    private final ZetaSQLType zetaSQLType;

    public ZetaSQLFilter(ZetaSQLFunction zetaSQLFunction, ZetaSQLType zetaSQLType) {
        this.zetaSQLFunction = zetaSQLFunction;
        this.zetaSQLType = zetaSQLType;
    }

    public boolean isConditionExpr(Expression expression) {
        return BasicType.BOOLEAN_TYPE.equals(zetaSQLType.getExpressionType(expression));
    }

    public boolean executeFilter(Expression whereExpr, Object[] inputFields) {
        if (whereExpr == null) {
            return true;
        }
        if (whereExpr instanceof Function) {
            return functionExpr((Function) whereExpr, inputFields);
        }
        if (whereExpr instanceof IsNullExpression) {
            return isNullExpr((IsNullExpression) whereExpr, inputFields);
        }
        if (whereExpr instanceof InExpression) {
            return inExpr((InExpression) whereExpr, inputFields);
        }
        if (whereExpr instanceof LikeExpression) {
            boolean isNotLike = ((LikeExpression) whereExpr).isNot();
            // not like SQL parsing
            if (isNotLike) {
                return notLikeExpr((LikeExpression) whereExpr, inputFields);
            }
            // like SQL parsing
            if (!isNotLike) {
                return likeExpr((LikeExpression) whereExpr, inputFields);
            }
        }
        // ILIKE, RLIKE and REGEXP are all parsed into LikeExpression as well
        if (whereExpr instanceof ComparisonOperator) {
            Pair<Object, Object> pair =
                    executeComparisonOperator((ComparisonOperator) whereExpr, inputFields);
            if (whereExpr instanceof EqualsTo) {
                return equalsToExpr(pair);
            }
            if (whereExpr instanceof NotEqualsTo) {
                return notEqualsToExpr(pair);
            }
            if (whereExpr instanceof GreaterThan) {
                return greaterThanExpr(pair);
            }
            if (whereExpr instanceof GreaterThanEquals) {
                return greaterThanEqualsExpr(pair);
            }
            if (whereExpr instanceof MinorThan) {
                return minorThanExpr(pair);
            }
            if (whereExpr instanceof MinorThanEquals) {
                return minorThanEqualsExpr(pair);
            }
        }
        if (whereExpr instanceof AndExpression) {
            return andExpr((AndExpression) whereExpr, inputFields);
        }
        if (whereExpr instanceof OrExpression) {
            return orExpr((OrExpression) whereExpr, inputFields);
        }
        if (whereExpr instanceof Parenthesis) {
            return parenthesisExpr((Parenthesis) whereExpr, inputFields);
        }
        if (whereExpr instanceof Column) {
            return (boolean) zetaSQLFunction.computeForValue(whereExpr, inputFields);
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported SQL Expression: %s ", whereExpr));
    }

    private boolean functionExpr(Function function, Object[] inputFields) {
        Boolean result = (Boolean) zetaSQLFunction.computeForValue(function, inputFields);
        if (result == null) {
            return false;
        }
        return result;
    }

    private boolean isNullExpr(IsNullExpression isNullExpression, Object[] inputFields) {
        Expression leftExpr = isNullExpression.getLeftExpression();
        Object leftVal = zetaSQLFunction.computeForValue(leftExpr, inputFields);
        if (isNullExpression.isNot()) {
            return leftVal != null;
        } else {
            return leftVal == null;
        }
    }

    private boolean inExpr(InExpression inExpression, Object[] inputFields) {
        Expression leftExpr = inExpression.getLeftExpression();
        ParenthesedExpressionList<Expression> itemsList =
                (ParenthesedExpressionList) inExpression.getRightExpression();
        Object leftValue = zetaSQLFunction.computeForValue(leftExpr, inputFields);
        for (Expression exprItem : itemsList.getExpressions()) {
            Object rightValue = zetaSQLFunction.computeForValue(exprItem, inputFields);
            if (leftValue == null && rightValue == null) {
                return true;
            }
            if (leftValue != null) {
                if (leftValue instanceof Number && rightValue instanceof Number) {
                    boolean equal =
                            areExactNumbers(leftValue, rightValue)
                                    ? compareExactNumbers((Number) leftValue, (Number) rightValue)
                                            == 0
                                    : ((Number) leftValue).doubleValue()
                                            == ((Number) rightValue).doubleValue();
                    if (equal) {
                        return !inExpression.isNot();
                    }
                } else if (leftValue instanceof OffsetDateTime
                        && rightValue instanceof OffsetDateTime) {
                    if (((OffsetDateTime) leftValue).isEqual((OffsetDateTime) rightValue)) {
                        return !inExpression.isNot();
                    }
                } else if (leftValue.equals(rightValue)) {
                    return !inExpression.isNot();
                }

            } else {
                return false;
            }
        }
        return inExpression.isNot(); // if all not in return true
    }

    /**
     * Like expression filter
     *
     * @param likeExpression like expression
     * @param inputFields input fields
     * @return filter result
     */
    private boolean likeExpr(LikeExpression likeExpression, Object[] inputFields) {
        Object leftVal =
                zetaSQLFunction.computeForValue(likeExpression.getLeftExpression(), inputFields);
        if (leftVal == null) {
            return false;
        }
        Object rightVal =
                zetaSQLFunction.computeForValue(likeExpression.getRightExpression(), inputFields);
        // SQL: a comparison with NULL is UNKNOWN, so the row is not kept
        if (rightVal == null) {
            return false;
        }
        return matchLikeExpression(likeExpression, rightVal.toString(), leftVal.toString());
    }

    /**
     * Applies the operator that was actually written. JSqlParser parses {@code LIKE}, {@code
     * ILIKE}, {@code RLIKE} and {@code REGEXP} all into a {@link LikeExpression}, so the keyword
     * decides the semantics: {@code RLIKE} / {@code REGEXP} are compiled as regular expressions,
     * and everything else is matched as a SQL LIKE pattern.
     *
     * <p>Note that {@code RLIKE} / {@code REGEXP} used to run through the LIKE rewriting as well,
     * so a pattern with {@code %} or {@code _} at its edge was altered before being compiled; such
     * a pattern is now used verbatim, which is what a regular expression is expected to mean.
     */
    private static boolean matchLikeExpression(
            LikeExpression likeExpression, String pattern, String value) {
        LikeExpression.KeyWord keyWord = likeExpression.getLikeKeyWord();
        if (keyWord == LikeExpression.KeyWord.RLIKE || keyWord == LikeExpression.KeyWord.REGEXP) {
            return Pattern.compile(pattern).matcher(value).matches();
        }
        return likeMatches(pattern, value);
    }

    /**
     * Matches a value against a SQL LIKE pattern, where {@code %} matches any sequence of
     * characters, {@code _} matches exactly one, and every other character matches itself.
     *
     * <p>Iterative with a single backtrack point per {@code %}, so it is O(pattern * value) and
     * cannot hit the catastrophic backtracking a regex built from the pattern would. Matching
     * characters directly also keeps regex metacharacters literal without any escaping.
     */
    private static boolean likeMatches(String pattern, String value) {
        int p = 0;
        int v = 0;
        int resumePattern = -1;
        int resumeValue = 0;
        while (v < value.length()) {
            if (p < pattern.length() && pattern.charAt(p) == '%') {
                resumePattern = ++p;
                resumeValue = v;
            } else if (p < pattern.length() && isSingleCharacterMatch(pattern, p, value, v)) {
                p += Character.charCount(pattern.codePointAt(p));
                v += Character.charCount(value.codePointAt(v));
            } else if (resumePattern >= 0) {
                // let the last % swallow one more character and retry from there
                resumeValue += Character.charCount(value.codePointAt(resumeValue));
                p = resumePattern;
                v = resumeValue;
            } else {
                return false;
            }
        }
        while (p < pattern.length() && pattern.charAt(p) == '%') {
            p++;
        }
        // shortcut: LIKE ... ESCAPE is unsupported, add it when a literal % or _ is needed
        return p == pattern.length();
    }

    private static boolean isSingleCharacterMatch(String pattern, int p, String value, int v) {
        return pattern.charAt(p) == '_' || pattern.codePointAt(p) == value.codePointAt(v);
    }

    /**
     * Not Like expression filter
     *
     * @param likeExpression not like expression
     * @param inputFields input fields
     * @return filter result
     */
    private boolean notLikeExpr(LikeExpression likeExpression, Object[] inputFields) {
        Object leftVal =
                zetaSQLFunction.computeForValue(likeExpression.getLeftExpression(), inputFields);
        if (leftVal == null) {
            return false;
        }
        Object rightVal =
                zetaSQLFunction.computeForValue(likeExpression.getRightExpression(), inputFields);
        // SQL: a comparison with NULL is UNKNOWN, so the row is not kept
        if (rightVal == null) {
            return false;
        }
        return !matchLikeExpression(likeExpression, rightVal.toString(), leftVal.toString());
    }

    private Pair<Object, Object> executeComparisonOperator(
            ComparisonOperator comparisonOperator, Object[] inputFields) {
        Expression leftExpr = comparisonOperator.getLeftExpression();
        Expression rightExpr = comparisonOperator.getRightExpression();
        Object leftVal = zetaSQLFunction.computeForValue(leftExpr, inputFields);
        Object rightVal = zetaSQLFunction.computeForValue(rightExpr, inputFields);
        return Pair.of(leftVal, rightVal);
    }

    boolean equalsToExpr(Pair<Object, Object> pair) {
        Object leftVal = pair.getLeft();
        Object rightVal = pair.getRight();
        if (leftVal == null || rightVal == null) {
            return false;
        }
        if (leftVal instanceof Number && rightVal instanceof Number) {
            return areExactNumbers(leftVal, rightVal)
                    ? compareExactNumbers((Number) leftVal, (Number) rightVal) == 0
                    : ((Number) leftVal).doubleValue() == ((Number) rightVal).doubleValue();
        }
        if (leftVal instanceof OffsetDateTime && rightVal instanceof OffsetDateTime) {
            return ((OffsetDateTime) leftVal).isEqual((OffsetDateTime) rightVal);
        }
        return leftVal.equals(rightVal);
    }

    private boolean notEqualsToExpr(Pair<Object, Object> pair) {
        Object leftVal = pair.getLeft();
        Object rightVal = pair.getRight();
        if (leftVal == null) {
            return rightVal != null;
        }
        if (leftVal instanceof Number && rightVal instanceof Number) {
            return areExactNumbers(leftVal, rightVal)
                    ? compareExactNumbers((Number) leftVal, (Number) rightVal) != 0
                    : ((Number) leftVal).doubleValue() != ((Number) rightVal).doubleValue();
        }
        if (leftVal instanceof OffsetDateTime && rightVal instanceof OffsetDateTime) {
            return !((OffsetDateTime) leftVal).isEqual((OffsetDateTime) rightVal);
        }
        return !leftVal.equals(rightVal);
    }

    private boolean greaterThanExpr(Pair<Object, Object> pair) {
        Object leftVal = pair.getLeft();
        Object rightVal = pair.getRight();
        if (leftVal == null || rightVal == null) {
            return false;
        }
        if (leftVal instanceof Number && rightVal instanceof Number) {
            return areExactNumbers(leftVal, rightVal)
                    ? compareExactNumbers((Number) leftVal, (Number) rightVal) > 0
                    : ((Number) leftVal).doubleValue() > ((Number) rightVal).doubleValue();
        }
        if (leftVal instanceof String && rightVal instanceof String) {
            return ((String) leftVal).compareTo((String) rightVal) > 0;
        }
        if (leftVal instanceof OffsetDateTime && rightVal instanceof OffsetDateTime) {
            return ((OffsetDateTime) leftVal).isAfter((OffsetDateTime) rightVal);
        }
        if (leftVal instanceof LocalDateTime && rightVal instanceof LocalDateTime) {
            return ((LocalDateTime) leftVal).isAfter((LocalDateTime) rightVal);
        }
        if (leftVal instanceof LocalDate && rightVal instanceof LocalDate) {
            return ((LocalDate) leftVal).isAfter((LocalDate) rightVal);
        }
        if (leftVal instanceof LocalTime && rightVal instanceof LocalTime) {
            return ((LocalTime) leftVal).isAfter((LocalTime) rightVal);
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format(
                        "Filed types not matched, left is: %s, right is: %s ",
                        leftVal.getClass().getSimpleName(), rightVal.getClass().getSimpleName()));
    }

    private boolean greaterThanEqualsExpr(Pair<Object, Object> pair) {
        Object leftVal = pair.getLeft();
        Object rightVal = pair.getRight();
        if (leftVal == null || rightVal == null) {
            return false;
        }
        if (leftVal instanceof Number && rightVal instanceof Number) {
            return areExactNumbers(leftVal, rightVal)
                    ? compareExactNumbers((Number) leftVal, (Number) rightVal) >= 0
                    : ((Number) leftVal).doubleValue() >= ((Number) rightVal).doubleValue();
        }
        if (leftVal instanceof String && rightVal instanceof String) {
            return ((String) leftVal).compareTo((String) rightVal) >= 0;
        }
        if (leftVal instanceof OffsetDateTime && rightVal instanceof OffsetDateTime) {
            return ((OffsetDateTime) leftVal).isAfter((OffsetDateTime) rightVal)
                    || ((OffsetDateTime) leftVal).isEqual((OffsetDateTime) rightVal);
        }
        if (leftVal instanceof LocalDateTime && rightVal instanceof LocalDateTime) {
            return ((LocalDateTime) leftVal).isAfter((LocalDateTime) rightVal)
                    || ((LocalDateTime) leftVal).isEqual((LocalDateTime) rightVal);
        }
        if (leftVal instanceof LocalDate && rightVal instanceof LocalDate) {
            return ((LocalDate) leftVal).isAfter((LocalDate) rightVal)
                    || ((LocalDate) leftVal).isEqual((LocalDate) rightVal);
        }
        if (leftVal instanceof LocalTime && rightVal instanceof LocalTime) {
            return ((LocalTime) leftVal).isAfter((LocalTime) rightVal) || leftVal.equals(rightVal);
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format(
                        "Filed types not matched, left is: %s, right is: %s ",
                        leftVal.getClass().getSimpleName(), rightVal.getClass().getSimpleName()));
    }

    private boolean minorThanExpr(Pair<Object, Object> pair) {
        Object leftVal = pair.getLeft();
        Object rightVal = pair.getRight();
        if (leftVal == null || rightVal == null) {
            return false;
        }
        if (leftVal instanceof OffsetDateTime && rightVal instanceof OffsetDateTime) {
            return ((OffsetDateTime) leftVal).isBefore((OffsetDateTime) rightVal);
        }
        if (leftVal instanceof LocalDateTime && rightVal instanceof LocalDateTime) {
            return ((LocalDateTime) leftVal).isBefore((LocalDateTime) rightVal);
        }
        if (leftVal instanceof LocalDate && rightVal instanceof LocalDate) {
            return ((LocalDate) leftVal).isBefore((LocalDate) rightVal);
        }
        if (leftVal instanceof LocalTime && rightVal instanceof LocalTime) {
            return ((LocalTime) leftVal).isBefore((LocalTime) rightVal);
        }
        if (leftVal instanceof Number && rightVal instanceof Number) {
            return areExactNumbers(leftVal, rightVal)
                    ? compareExactNumbers((Number) leftVal, (Number) rightVal) < 0
                    : ((Number) leftVal).doubleValue() < ((Number) rightVal).doubleValue();
        }
        if (leftVal instanceof String && rightVal instanceof String) {
            return ((String) leftVal).compareTo((String) rightVal) < 0;
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format(
                        "Filed types not matched, left is: %s, right is: %s ",
                        leftVal.getClass().getSimpleName(), rightVal.getClass().getSimpleName()));
    }

    private boolean minorThanEqualsExpr(Pair<Object, Object> pair) {
        Object leftVal = pair.getLeft();
        Object rightVal = pair.getRight();
        if (leftVal == null || rightVal == null) {
            return false;
        }
        if (leftVal instanceof OffsetDateTime && rightVal instanceof OffsetDateTime) {
            return ((OffsetDateTime) leftVal).isBefore((OffsetDateTime) rightVal)
                    || ((OffsetDateTime) leftVal).isEqual((OffsetDateTime) rightVal);
        }
        if (leftVal instanceof LocalDateTime && rightVal instanceof LocalDateTime) {
            return ((LocalDateTime) leftVal).isBefore((LocalDateTime) rightVal)
                    || ((LocalDateTime) leftVal).isEqual((LocalDateTime) rightVal);
        }
        if (leftVal instanceof LocalDate && rightVal instanceof LocalDate) {
            return ((LocalDate) leftVal).isBefore((LocalDate) rightVal)
                    || ((LocalDate) leftVal).isEqual((LocalDate) rightVal);
        }
        if (leftVal instanceof LocalTime && rightVal instanceof LocalTime) {
            return ((LocalTime) leftVal).isBefore((LocalTime) rightVal) || leftVal.equals(rightVal);
        }
        if (leftVal instanceof Number && rightVal instanceof Number) {
            return areExactNumbers(leftVal, rightVal)
                    ? compareExactNumbers((Number) leftVal, (Number) rightVal) <= 0
                    : ((Number) leftVal).doubleValue() <= ((Number) rightVal).doubleValue();
        }
        if (leftVal instanceof String && rightVal instanceof String) {
            return ((String) leftVal).compareTo((String) rightVal) <= 0;
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format(
                        "Filed types not matched, left is: %s, right is: %s ",
                        leftVal.getClass().getSimpleName(), rightVal.getClass().getSimpleName()));
    }

    private static boolean areExactNumbers(Object left, Object right) {
        return isExactNumber(left) && isExactNumber(right);
    }

    /**
     * Recognizes integral wrappers and BigDecimal. Float/Double and other Number subtypes
     * (including BigInteger) retain the legacy double path. Callers use native floating operators,
     * not Double.compare, to preserve NaN, infinity and signed-zero behavior.
     */
    private static boolean isExactNumber(Object value) {
        return value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof BigDecimal;
    }

    /** Compares exact operands; callers must first establish {@link #areExactNumbers}. */
    private static int compareExactNumbers(Number left, Number right) {
        // Integral pairs need no allocation. Mixed decimal/integral pairs may allocate a BigDecimal
        // per comparison (including each IN item) to preserve precision instead of using double.
        if (left instanceof BigDecimal || right instanceof BigDecimal) {
            return NumericFunction.toBigDecimal(left)
                    .compareTo(NumericFunction.toBigDecimal(right));
        }
        return Long.compare(left.longValue(), right.longValue());
    }

    private boolean andExpr(AndExpression andExpression, Object[] inputFields) {
        Expression leftExpr = andExpression.getLeftExpression();
        boolean leftRes = executeFilter(leftExpr, inputFields);
        Expression rightExpr = andExpression.getRightExpression();
        boolean rightRes = executeFilter(rightExpr, inputFields);
        return leftRes && rightRes;
    }

    private boolean orExpr(OrExpression orExpression, Object[] inputFields) {
        Expression leftExpr = orExpression.getLeftExpression();
        boolean leftRes = executeFilter(leftExpr, inputFields);
        Expression rightExpr = orExpression.getRightExpression();
        boolean rightRes = executeFilter(rightExpr, inputFields);
        return leftRes || rightRes;
    }

    private boolean parenthesisExpr(Parenthesis parenthesis, Object[] inputFields) {
        Expression expression = parenthesis.getExpression();
        return executeFilter(expression, inputFields);
    }
}
