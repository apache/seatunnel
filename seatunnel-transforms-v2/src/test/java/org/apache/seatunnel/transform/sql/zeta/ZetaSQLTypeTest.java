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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.zeta.functions.udf.DesEncrypt;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimezoneExpression;
import net.sf.jsqlparser.expression.TrimFunction;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class ZetaSQLTypeTest {

    private ZetaSQLType simpleType(SeaTunnelRowType rowType) {
        return new ZetaSQLType(rowType, Collections.emptyList());
    }

    @Test
    public void testLiteralAndColumnTypes() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        ZetaSQLType type = simpleType(rowType);

        Assertions.assertEquals(BasicType.VOID_TYPE, type.getExpressionType(new NullValue()));

        SignedExpression signed = new SignedExpression();
        signed.setExpression(new DoubleValue("1.5"));
        signed.setSign('-');
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, type.getExpressionType(signed));

        Assertions.assertEquals(
                BasicType.DOUBLE_TYPE, type.getExpressionType(new DoubleValue("1.0")));

        Assertions.assertEquals(BasicType.INT_TYPE, type.getExpressionType(new LongValue(100)));

        long biggerThanInt = (long) Integer.MAX_VALUE + 1;
        Assertions.assertEquals(
                BasicType.LONG_TYPE,
                type.getExpressionType(new LongValue(Long.toString(biggerThanInt))));

        Assertions.assertEquals(
                BasicType.STRING_TYPE, type.getExpressionType(new StringValue("abc")));

        Assertions.assertEquals(BasicType.INT_TYPE, type.getExpressionType(new Column("id")));

        Assertions.assertEquals(
                BasicType.STRING_TYPE, type.getExpressionType(new Column("`name`")));

        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, type.getExpressionType(new Column("true")));
        Assertions.assertEquals(
                BasicType.BOOLEAN_TYPE, type.getExpressionType(new Column("FALSE")));

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> type.getExpressionType(new Column("unknown")));
    }

    @Test
    public void testNestedRowAndMapColumnResolution() {
        SeaTunnelRowType addressType =
                new SeaTunnelRowType(
                        new String[] {"street", "zipcode"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

        SeaTunnelRowType userType =
                new SeaTunnelRowType(
                        new String[] {"name", "address"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, addressType});

        SeaTunnelRowType topRowType =
                new SeaTunnelRowType(new String[] {"user"}, new SeaTunnelDataType[] {userType});

        ZetaSQLType rowZetaType = simpleType(topRowType);

        Assertions.assertEquals(
                addressType, rowZetaType.getExpressionType(new Column("user.address")));

        Assertions.assertEquals(
                BasicType.STRING_TYPE,
                rowZetaType.getExpressionType(new Column("user.address.street")));

        MapType<String, Integer> mapType = new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE);
        SeaTunnelRowType mapRowType =
                new SeaTunnelRowType(new String[] {"metrics"}, new SeaTunnelDataType[] {mapType});
        ZetaSQLType mapZetaType = simpleType(mapRowType);

        Assertions.assertEquals(mapType, mapZetaType.getExpressionType(new Column("metrics")));

        Assertions.assertEquals(
                BasicType.INT_TYPE, mapZetaType.getExpressionType(new Column("metrics.cpu")));

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> mapZetaType.getExpressionType(new Column("metrics.cpu.extra")));
    }

    @Test
    public void testTrimExtractParenthesisConcatAndComparisonTypes() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        ZetaSQLType type = simpleType(rowType);

        TrimFunction trim = new TrimFunction();
        trim.setExpression(new StringValue(" abc "));
        Assertions.assertEquals(BasicType.STRING_TYPE, type.getExpressionType(trim));

        Assertions.assertEquals(
                BasicType.INT_TYPE, type.getExpressionType(new ExtractExpression()));

        Parenthesis parenthesis = new Parenthesis(new LongValue(1));
        Assertions.assertEquals(BasicType.INT_TYPE, type.getExpressionType(parenthesis));

        EqualsTo equalsTo = new EqualsTo();
        equalsTo.setLeftExpression(new Column("id"));
        equalsTo.setRightExpression(new LongValue(1));
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, type.getExpressionType(equalsTo));
    }

    @Test
    public void testFunctionTypeStringNumericBooleanAndVector() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        ZetaSQLType type = simpleType(rowType);

        Function substring = new Function();
        substring.setName(ZetaSQLFunction.SUBSTRING);
        substring.setParameters(
                new ExpressionList<>(Arrays.asList(new StringValue("abc"), new LongValue(1))));
        Assertions.assertEquals(BasicType.STRING_TYPE, type.getExpressionType(substring));

        Function charLength = new Function();
        charLength.setName(ZetaSQLFunction.CHAR_LENGTH);
        charLength.setParameters(
                new ExpressionList<>(Collections.singletonList(new StringValue("abc"))));
        Assertions.assertEquals(BasicType.LONG_TYPE, type.getExpressionType(charLength));

        Function regexpLike = new Function();
        regexpLike.setName(ZetaSQLFunction.REGEXP_LIKE);
        regexpLike.setParameters(
                new ExpressionList<>(
                        Arrays.asList(new StringValue("abc"), new StringValue("a.*"))));
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, type.getExpressionType(regexpLike));

        Function cosFunc = new Function();
        cosFunc.setName(ZetaSQLFunction.COS);
        cosFunc.setParameters(
                new ExpressionList<>(Collections.singletonList(new DoubleValue("0.0"))));
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, type.getExpressionType(cosFunc));

        Function arrayFunc = new Function();
        arrayFunc.setName(ZetaSQLFunction.ARRAY);
        arrayFunc.setParameters(
                new ExpressionList<>(Arrays.asList(new LongValue(1), new LongValue(2))));
        SeaTunnelDataType<?> arrayType = type.getExpressionType(arrayFunc);
        Assertions.assertTrue(arrayType instanceof ArrayType);
        Assertions.assertEquals(BasicType.INT_TYPE, ((ArrayType) arrayType).getElementType());

        Function mapFunc = new Function();
        mapFunc.setName(ZetaSQLFunction.MAP);
        mapFunc.setParameters(
                new ExpressionList<>(
                        Arrays.asList(
                                new StringValue("k1"), new LongValue(1),
                                new StringValue("k2"), new LongValue(2))));
        SeaTunnelDataType<?> mapType = type.getExpressionType(mapFunc);
        Assertions.assertTrue(mapType instanceof MapType);
        MapType<?, ?> mt = (MapType<?, ?>) mapType;
        Assertions.assertEquals(BasicType.STRING_TYPE, mt.getKeyType());
        Assertions.assertEquals(BasicType.INT_TYPE, mt.getValueType());

        Function dimsFunc = new Function();
        dimsFunc.setName(ZetaSQLFunction.VECTOR_DIMS);
        dimsFunc.setParameters(
                new ExpressionList<>(Collections.singletonList(new StringValue("ignored"))));
        Assertions.assertEquals(BasicType.INT_TYPE, type.getExpressionType(dimsFunc));

        Function reduceFunc = new Function();
        reduceFunc.setName(ZetaSQLFunction.VECTOR_REDUCE);
        reduceFunc.setParameters(
                new ExpressionList<>(
                        Arrays.asList(
                                new StringValue("v"),
                                new LongValue(2),
                                new StringValue("TRUNCATE"))));
        Assertions.assertEquals(VectorType.VECTOR_FLOAT_TYPE, type.getExpressionType(reduceFunc));
    }

    @Test
    public void testParsedatetimeAndTimeKeyExpressionTypes() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        ZetaSQLType type = simpleType(rowType);

        Function parseDateTime = new Function();
        parseDateTime.setName(ZetaSQLFunction.PARSEDATETIME);
        parseDateTime.setParameters(
                new ExpressionList<>(
                        Arrays.asList(
                                new StringValue("2025-05-21 12:00:00"),
                                new StringValue("yyyy-MM-dd HH:mm:ss"))));
        Assertions.assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE, type.getExpressionType(parseDateTime));

        Function parseDate = new Function();
        parseDate.setName(ZetaSQLFunction.PARSEDATETIME);
        parseDate.setParameters(
                new ExpressionList<>(
                        Arrays.asList(
                                new StringValue("2025-05-21"), new StringValue("yyyy-MM-dd"))));
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, type.getExpressionType(parseDate));

        Function parseTime = new Function();
        parseTime.setName(ZetaSQLFunction.PARSEDATETIME);
        parseTime.setParameters(
                new ExpressionList<>(
                        Arrays.asList(new StringValue("12:00:00"), new StringValue("HH:mm:ss"))));
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, type.getExpressionType(parseTime));

        Function badPattern = new Function();
        badPattern.setName(ZetaSQLFunction.PARSEDATETIME);
        badPattern.setParameters(
                new ExpressionList<>(
                        Arrays.asList(new StringValue("data"), new StringValue("invalid"))));
        Assertions.assertThrows(TransformException.class, () -> type.getExpressionType(badPattern));

        Assertions.assertEquals(
                LocalTimeType.LOCAL_DATE_TYPE,
                type.getExpressionType(new TimeKeyExpression(ZetaSQLFunction.CURRENT_DATE)));
        Assertions.assertEquals(
                LocalTimeType.LOCAL_TIME_TYPE,
                type.getExpressionType(new TimeKeyExpression(ZetaSQLFunction.CURRENT_TIME)));
        Assertions.assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                type.getExpressionType(new TimeKeyExpression(ZetaSQLFunction.CURRENT_TIMESTAMP)));
    }

    @Test
    public void testCastBinaryAndTimezoneTypes() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        ZetaSQLType type = simpleType(rowType);

        CastExpression castExpression = new CastExpression();
        castExpression.setLeftExpression(new LongValue(1));
        net.sf.jsqlparser.statement.create.table.ColDataType colDataType =
                new net.sf.jsqlparser.statement.create.table.ColDataType();
        colDataType.setDataType("INT");
        castExpression.setColDataType(colDataType);
        Assertions.assertEquals(BasicType.INT_TYPE, type.getExpressionType(castExpression));

        BinaryExpression add = new Addition();
        add.setLeftExpression(new LongValue(1));
        add.setRightExpression(new LongValue(2));
        Assertions.assertEquals(BasicType.INT_TYPE, type.getExpressionType(add));

        BinaryExpression addBigint = new Addition();
        addBigint.setLeftExpression(new LongValue(Long.toString(Integer.MAX_VALUE + 1L)));
        addBigint.setRightExpression(new LongValue(1));
        // both BIGINT -> result should be LONG_TYPE
        SeaTunnelDataType<?> bigintResult = type.getExpressionType(addBigint);
        Assertions.assertEquals(BasicType.LONG_TYPE, bigintResult);

        TimezoneExpression timezoneExpression = new TimezoneExpression();
        Assertions.assertEquals(
                LocalTimeType.OFFSET_DATE_TIME_TYPE, type.getExpressionType(timezoneExpression));
    }

    @Test
    public void testCoalesceMultiIfModAndUdfTypes() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        ZetaSQLType type = simpleType(rowType);

        Function coalesce = new Function();
        coalesce.setName(ZetaSQLFunction.COALESCE);
        coalesce.setParameters(
                new ExpressionList<>(Arrays.asList(new NullValue(), new LongValue(10))));
        Assertions.assertEquals(BasicType.INT_TYPE, type.getExpressionType(coalesce));

        Function allNull = new Function();
        allNull.setName(ZetaSQLFunction.COALESCE);
        allNull.setParameters(
                new ExpressionList<>(Arrays.asList(new NullValue(), new NullValue())));
        Assertions.assertEquals(BasicType.VOID_TYPE, type.getExpressionType(allNull));

        Function badCoalesce = new Function();
        badCoalesce.setName(ZetaSQLFunction.COALESCE);
        Assertions.assertThrows(
                TransformException.class, () -> type.getExpressionType(badCoalesce));

        Function multiIf = new Function();
        multiIf.setName(ZetaSQLFunction.MULTI_IF);
        multiIf.setParameters(
                new ExpressionList<>(
                        Arrays.asList(
                                new LongValue(1),
                                new LongValue(1),
                                new LongValue(0),
                                new LongValue(2),
                                new LongValue(3))));
        Assertions.assertEquals(BasicType.INT_TYPE, type.getExpressionType(multiIf));

        Function multiIfNoParams = new Function();
        multiIfNoParams.setName(ZetaSQLFunction.MULTI_IF);
        Assertions.assertThrows(
                TransformException.class, () -> type.getExpressionType(multiIfNoParams));

        Function multiIfEvenArgs = new Function();
        multiIfEvenArgs.setName(ZetaSQLFunction.MULTI_IF);
        multiIfEvenArgs.setParameters(
                new ExpressionList<>(Arrays.asList(new LongValue(1), new LongValue(1))));
        Assertions.assertThrows(
                TransformException.class, () -> type.getExpressionType(multiIfEvenArgs));

        Function modFunc = new Function();
        modFunc.setName(ZetaSQLFunction.MOD);
        modFunc.setParameters(
                new ExpressionList<>(Arrays.asList(new LongValue(5), new LongValue(2))));
        Assertions.assertEquals(BasicType.INT_TYPE, type.getExpressionType(modFunc));

        SeaTunnelRowType udfRowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        ZetaSQLType udfType =
                new ZetaSQLType(udfRowType, Collections.singletonList(new DesEncrypt()));

        Function udfFunction = new Function();
        udfFunction.setName("DES_ENCRYPT");
        udfFunction.setParameters(
                new ExpressionList<>(
                        Arrays.asList(new StringValue("password"), new StringValue("data"))));
        Assertions.assertEquals(BasicType.STRING_TYPE, udfType.getExpressionType(udfFunction));

        Function unknownFunc = new Function();
        unknownFunc.setName("UNKNOWN_FUNC");
        unknownFunc.setParameters(
                new ExpressionList<>(Collections.singletonList(new LongValue(1))));
        Assertions.assertThrows(
                TransformException.class, () -> udfType.getExpressionType(unknownFunc));
    }

    @Test
    public void testIsNumberTypeAndGetMaxType() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        ZetaSQLType type = simpleType(rowType);

        Assertions.assertTrue(type.isNumberType(SqlType.TINYINT));
        Assertions.assertTrue(type.isNumberType(SqlType.DECIMAL));
        Assertions.assertFalse(type.isNumberType(SqlType.BOOLEAN));

        SeaTunnelDataType<?> intType = BasicType.INT_TYPE;
        SeaTunnelDataType<?> longType = BasicType.LONG_TYPE;

        Assertions.assertEquals(longType, type.getMaxType(intType, longType));
        Assertions.assertEquals(longType, type.getMaxType(longType, intType));

        DecimalType d1 = new DecimalType(10, 2);
        DecimalType d2 = new DecimalType(12, 3);
        SeaTunnelDataType<?> maxDecimal = type.getMaxType(d1, d2);
        Assertions.assertTrue(maxDecimal instanceof DecimalType);
        DecimalType md = (DecimalType) maxDecimal;
        Assertions.assertEquals(12, md.getPrecision());
        Assertions.assertEquals(3, md.getScale());

        Assertions.assertEquals(longType, type.getMaxType(null, longType));
        Assertions.assertEquals(intType, type.getMaxType(intType, null));

        Assertions.assertThrows(
                TransformException.class,
                () -> type.getMaxType(BasicType.STRING_TYPE, BasicType.INT_TYPE));
    }

    @Test
    public void testGetMaxTypeCollection() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        ZetaSQLType type = simpleType(rowType);

        Collection<SeaTunnelDataType<?>> types =
                Arrays.asList(BasicType.INT_TYPE, BasicType.LONG_TYPE, BasicType.DOUBLE_TYPE);
        SeaTunnelDataType<?> result = type.getMaxType(types);
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, result);

        Assertions.assertThrows(
                TransformException.class, () -> type.getMaxType(Collections.emptyList()));
    }
}
