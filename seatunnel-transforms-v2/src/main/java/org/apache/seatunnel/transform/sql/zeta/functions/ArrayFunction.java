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
package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.transform.exception.TransformException;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ArrayFunction {

    public static Object arrayMax(List<Object> args) {
        if (args == null || args.isEmpty()) {
            return null;
        }
        Object[] dataList = (Object[]) args.get(0);
        if (dataList == null || dataList.length == 0) {
            return null;
        }
        if (dataList[0] instanceof String) {
            return Arrays.stream(dataList)
                    .map(String.class::cast)
                    .max(String::compareTo)
                    .orElse(null);
        } else if (dataList[0] instanceof Number) {
            return Arrays.stream(dataList)
                    .map(Number.class::cast)
                    .max(Comparator.comparingDouble(Number::doubleValue))
                    .orElse(null);
        }
        throw new TransformException(
                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                String.format("Unsupported function max() arguments: %s", args));
    }

    public static Object arrayMin(List<Object> args) {
        if (args == null || args.isEmpty()) {
            return null;
        }
        Object[] dataList = (Object[]) args.get(0);
        if (dataList == null || dataList.length == 0) {
            return null;
        }
        if (dataList[0] instanceof String) {
            return Arrays.stream(dataList)
                    .map(String.class::cast)
                    .min(String::compareTo)
                    .orElse(null);
        } else if (dataList[0] instanceof Number) {
            return Arrays.stream(dataList)
                    .map(Number.class::cast)
                    .min(Comparator.comparingDouble(Number::doubleValue))
                    .orElse(null);
        }
        throw new TransformException(
                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                String.format("Unsupported function max() arguments: %s", args));
    }

    public static Object[] array(List<Object> args) {
        if (args == null || args.isEmpty()) {
            return new Object[0];
        }
        Class<?> arrayType = getDataClassType(args);
        Object[] result = (Object[]) java.lang.reflect.Array.newInstance(arrayType, args.size());
        for (int i = 0; i < args.size(); i++) {
            result[i] = convertToType(args.get(i), arrayType);
        }

        return result;
    }

    public static ArrayType castArrayTypeMapping(Function function, SeaTunnelRowType inputRowType) {
        return castArrayTypeMapping(getFunctionArgs(function, inputRowType));
    }

    public static ArrayType castArrayTypeMapping(List<Class<?>> args) {
        if (args == null || args.isEmpty()) {
            return ArrayType.STRING_ARRAY_TYPE;
        }

        Class<?> arrayType = getClassType(args);
        return getSeaTunnelDataType(arrayType);
    }

    private static ArrayType getSeaTunnelDataType(Class<?> clazz) {
        String className = clazz.getSimpleName();
        switch (className) {
            case "Integer":
                return ArrayType.INT_ARRAY_TYPE;
            case "Double":
                return ArrayType.DOUBLE_ARRAY_TYPE;
            case "Boolean":
                return ArrayType.BOOLEAN_ARRAY_TYPE;
            case "Long":
                return ArrayType.LONG_ARRAY_TYPE;
            case "float":
                return ArrayType.FLOAT_ARRAY_TYPE;
            case "short":
                return ArrayType.SHORT_ARRAY_TYPE;
            default:
                return ArrayType.STRING_ARRAY_TYPE;
        }
    }

    private static Class<?> getArrayType(Class<?> type1, Class<?> type2) {
        if (type1.isAssignableFrom(type2)) {
            return type1;
        }
        if (type2.isAssignableFrom(type1)) {
            return type2;
        }
        if (isNumericType(type1) && isNumericType(type2)) {
            return getNumericCommonType(type1, type2);
        }
        return String.class;
    }

    private static boolean isNumericType(Class<?> type) {
        return type == Short.class
                || type == Integer.class
                || type == Long.class
                || type == Float.class
                || type == Double.class;
    }

    private static Class<?> getNumericCommonType(Class<?> type1, Class<?> type2) {
        if (type1 == Double.class || type2 == Double.class) {
            return Double.class;
        }
        if (type1 == Float.class || type2 == Float.class) {
            return Float.class;
        }
        if (type1 == Long.class || type2 == Long.class) {
            return Long.class;
        }
        if (type1 == Integer.class || type2 == Integer.class) {
            return Integer.class;
        }
        if (type1 == Short.class || type2 == Short.class) {
            return Short.class;
        }
        return String.class;
    }

    private static Class<?> getClassType(List<Class<?>> args) {
        Class<?> arrayType = null;
        for (Class<?> obj : args) {
            if (obj == null) {
                continue;
            }
            if (arrayType == null) {
                arrayType = obj;
            } else {
                arrayType = getArrayType(arrayType, obj);
            }
        }
        return arrayType == null ? String.class : arrayType;
    }

    private static Class<?> getDataClassType(List<Object> args) {
        Class<?> arrayType = null;
        for (Object obj : args) {
            if (obj == null) {
                continue;
            }
            if (arrayType == null) {
                arrayType = obj.getClass();
            } else {
                arrayType = getArrayType(arrayType, obj.getClass());
            }
        }
        return arrayType == null ? String.class : arrayType;
    }

    public static SeaTunnelDataType<?> getElementType(
            Function function, SeaTunnelRowType inputRowType) {
        String columnName = function.getParameters().getExpressions().get(0).toString();
        int columnIndex = inputRowType.indexOf(columnName);
        ArrayType arrayType = (ArrayType) inputRowType.getFieldType(columnIndex);
        return arrayType.getElementType();
    }

    private static List<Class<?>> getFunctionArgs(
            Function function, SeaTunnelRowType inputRowType) {
        ExpressionList<Expression> expressionList =
                (ExpressionList<Expression>) function.getParameters();
        List<Class<?>> functionArgs = new ArrayList<>();
        if (expressionList != null) {
            for (Expression expression : expressionList.getExpressions()) {
                if (expression instanceof NullValue) {
                    functionArgs.add(null);
                    continue;
                }
                if (expression instanceof DoubleValue) {
                    functionArgs.add(Double.class);
                    continue;
                }
                if (expression instanceof Column) {
                    int columnIndex = inputRowType.indexOf(((Column) expression).getColumnName());
                    functionArgs.add(inputRowType.getFieldType(columnIndex).getTypeClass());
                    continue;
                }

                if (expression instanceof LongValue) {
                    long longVal = ((LongValue) expression).getValue();
                    if (longVal <= Integer.MAX_VALUE && longVal >= Integer.MIN_VALUE) {
                        functionArgs.add(Integer.class);
                    } else {
                        functionArgs.add(Long.class);
                    }
                    continue;
                }
                if (expression instanceof StringValue) {
                    functionArgs.add(String.class);
                    continue;
                }
                throw new SeaTunnelException("unSupport expression： " + expression.toString());
            }
        }
        return functionArgs;
    }

    private static Object convertToType(Object obj, Class<?> targetType) {
        if (obj == null || targetType.isInstance(obj)) {
            return obj;
        }

        if (targetType == Double.class) {
            return ((Number) obj).doubleValue();
        }
        if (targetType == Float.class) {
            return ((Number) obj).floatValue();
        }
        if (targetType == Long.class) {
            return ((Number) obj).longValue();
        }
        if (targetType == Integer.class) {
            return ((Number) obj).intValue();
        }
        if (targetType == Short.class) {
            return ((Number) obj).shortValue();
        }
        if (targetType == Byte.class) {
            return ((Number) obj).byteValue();
        }
        if (targetType == String.class) {
            return obj.toString();
        }

        throw new SeaTunnelException("Cannot convert " + obj.getClass() + " to " + targetType);
    }
}
