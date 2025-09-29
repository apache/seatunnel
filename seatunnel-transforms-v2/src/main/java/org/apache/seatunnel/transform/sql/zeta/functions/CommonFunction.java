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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

public class CommonFunction {
    private CommonFunction() {}

    public static SeaTunnelDataType resolveExpressionType(
            Expression expression, SeaTunnelRowType rowType) {
        if (expression instanceof NullValue) {
            return null;
        }
        if (expression instanceof DoubleValue) {
            return BasicType.DOUBLE_TYPE;
        }
        if (expression instanceof LongValue) {
            long v = ((LongValue) expression).getValue();
            if (v <= Integer.MAX_VALUE && v >= Integer.MIN_VALUE) {
                return BasicType.INT_TYPE;
            }
            return BasicType.LONG_TYPE;
        }
        if (expression instanceof StringValue) {
            return BasicType.STRING_TYPE;
        }
        if (expression instanceof Column) {
            Column c = (Column) expression;
            int idx = rowType.indexOf(c.getColumnName());
            if (idx < 0) {
                throw CommonError.illegalArgument(
                        "column not found: " + c.getColumnName(), "derive expression type");
            }
            return rowType.getFieldType(idx);
        }
        if (expression instanceof Function) {
            Function function = (Function) expression;
            String name = function.getName();
            if (name != null && "ARRAY".equalsIgnoreCase(name)) {
                return ArrayFunction.castArrayTypeMapping(function, rowType);
            }
            if (name != null && "MAP".equalsIgnoreCase(name)) {
                return MapFunction.castMapTypeMapping(function, rowType);
            }
        }
        throw CommonError.unsupportedDataType(
                "SeaTunnel", expression.getClass().getTypeName(), expression.toString());
    }

    public static SeaTunnelDataType unifyCollectionType(
            SeaTunnelDataType type1, SeaTunnelDataType type2) {
        if (type1 == null || BasicType.VOID_TYPE.equals(type1)) return type2;
        if (type2 == null || BasicType.VOID_TYPE.equals(type2)) return type1;

        if (type1.equals(type2)) return type1;

        if (isNumeric(type1) && isNumeric(type2)) {
            return widenNumeric(type1, type2);
        }

        if (type1 instanceof ArrayType && type2 instanceof ArrayType) {
            ArrayType at = (ArrayType) type1;
            ArrayType bt = (ArrayType) type2;
            SeaTunnelDataType ae = at.getElementType();
            SeaTunnelDataType be = bt.getElementType();
            SeaTunnelDataType ue = unifyCollectionType(ae, be);
            return ArrayFunction.createArrayType(ue);
        }

        if (type1 instanceof MapType && type2 instanceof MapType) {
            MapType map1 = (MapType) type1;
            MapType map2 = (MapType) type2;
            SeaTunnelDataType uk = unifyCollectionType(map1.getKeyType(), map2.getKeyType());
            SeaTunnelDataType uv = unifyCollectionType(map1.getValueType(), map2.getValueType());
            return new MapType<>(uk, uv);
        }

        return BasicType.STRING_TYPE;
    }

    public static boolean isNumeric(SeaTunnelDataType<?> type) {
        return type == BasicType.BYTE_TYPE
                || type == BasicType.SHORT_TYPE
                || type == BasicType.INT_TYPE
                || type == BasicType.LONG_TYPE
                || type == BasicType.FLOAT_TYPE
                || type == BasicType.DOUBLE_TYPE;
    }

    public static SeaTunnelDataType widenNumeric(SeaTunnelDataType type1, SeaTunnelDataType type2) {
        int rank1 = numericRank(type1);
        int rank2 = numericRank(type2);
        int max = Math.max(rank1, rank2);
        switch (max) {
            case 5:
                return BasicType.DOUBLE_TYPE;
            case 4:
                return BasicType.FLOAT_TYPE;
            case 3:
                return BasicType.LONG_TYPE;
            case 2:
                return BasicType.INT_TYPE;
            case 1:
                return BasicType.SHORT_TYPE;
            default:
                return BasicType.BYTE_TYPE;
        }
    }

    private static int numericRank(SeaTunnelDataType<?> type) {
        if (type == BasicType.DOUBLE_TYPE) return 5;
        if (type == BasicType.FLOAT_TYPE) return 4;
        if (type == BasicType.LONG_TYPE) return 3;
        if (type == BasicType.INT_TYPE) return 2;
        if (type == BasicType.SHORT_TYPE) return 1;
        return 0; // BYTE
    }

    public static List<Expression> getExpressions(Function function) {
        ExpressionList<Expression> params = (ExpressionList<Expression>) function.getParameters();
        List<Expression> expressions = new ArrayList<>();
        if (params != null) {
            for (Expression expression : params) {
                expressions.add(expression);
            }
        }
        return expressions;
    }
}
