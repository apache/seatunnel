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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.DateTimeUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.statement.select.PlainSelect;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;

import static org.apache.seatunnel.connectors.seatunnel.paimon.source.converter.SqlToPaimonPredicateConverter.convertToPlainSelect;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SqlToPaimonConverterTest {

    private RowType rowType;

    private String[] fieldNames;

    @BeforeEach
    public void setUp() {
        rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "char_col", new CharType()),
                                new DataField(1, "varchar_col", new VarCharType()),
                                new DataField(2, "boolean_col", new BooleanType()),
                                new DataField(3, "binary_col", new VarBinaryType()),
                                new DataField(4, "decimal_col", new DecimalType(10, 2)),
                                new DataField(5, "tinyint_col", new TinyIntType()),
                                new DataField(6, "smallint_col", new SmallIntType()),
                                new DataField(7, "int_col", new IntType()),
                                new DataField(8, "bigint_col", new BigIntType()),
                                new DataField(9, "float_col", new FloatType()),
                                new DataField(10, "double_col", new DoubleType()),
                                new DataField(11, "date_col", new DateType()),
                                new DataField(12, "timestamp_col", new TimestampType()),
                                new DataField(13, "time_col", new TimeType())));

        fieldNames = rowType.getFieldNames().toArray(new String[0]);
    }

    @Test
    public void testConvertSqlWhereToPaimonPredicate() {
        String query =
                "SELECT * FROM table WHERE "
                        + "char_col = 'a' AND "
                        + "varchar_col = 'test' AND "
                        + "boolean_col = 'true' AND "
                        + "decimal_col = 123.45 AND "
                        + "tinyint_col = 1 AND "
                        + "smallint_col = 2 AND "
                        + "int_col = 3 AND "
                        + "bigint_col = 4 AND "
                        + "float_col = 5.5 AND "
                        + "double_col = 6.6 AND "
                        + "date_col = '2022-01-01' AND "
                        + "timestamp_col = '2022-01-01T12:00:00.123' AND "
                        + "time_col = '12:00:00.123'";

        PlainSelect plainSelect = convertToPlainSelect(query);
        Predicate predicate =
                SqlToPaimonPredicateConverter.convertSqlWhereToPaimonPredicate(
                        rowType, plainSelect);

        assertNotNull(predicate);

        PredicateBuilder builder = new PredicateBuilder(rowType);

        // Validate each part of the predicate
        Predicate expectedPredicate =
                PredicateBuilder.and(
                        builder.equal(0, "a"),
                        builder.equal(1, "test"),
                        builder.equal(2, true),
                        builder.equal(4, Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2)),
                        builder.equal(5, (byte) 1),
                        builder.equal(6, (short) 2),
                        builder.equal(7, 3),
                        builder.equal(8, 4L),
                        builder.equal(9, 5.5f),
                        builder.equal(10, 6.6d),
                        builder.equal(11, DateTimeUtils.toInternal(LocalDate.parse("2022-01-01"))),
                        builder.equal(
                                12,
                                Timestamp.fromLocalDateTime(
                                        LocalDateTime.parse("2022-01-01T12:00:00.123"))),
                        builder.equal(
                                13, DateTimeUtils.toInternal(LocalTime.parse("12:00:00.123"))));

        assertEquals(expectedPredicate.toString(), predicate.toString());
    }

    @Test
    public void testConvertSqlWhereToPaimonPredicateWithIsNull() {
        String query = "SELECT * FROM table WHERE char_col IS NULL";

        PlainSelect plainSelect = convertToPlainSelect(query);
        Predicate predicate =
                SqlToPaimonPredicateConverter.convertSqlWhereToPaimonPredicate(
                        rowType, plainSelect);

        assertNotNull(predicate);

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate expectedPredicate = builder.isNull(0);

        assertEquals(expectedPredicate.toString(), predicate.toString());
    }

    @Test
    public void testConvertSqlWhereToPaimonPredicateWithIsNotNull() {
        String query = "SELECT * FROM table WHERE char_col IS NOT NULL";

        PlainSelect plainSelect = convertToPlainSelect(query);
        Predicate predicate =
                SqlToPaimonPredicateConverter.convertSqlWhereToPaimonPredicate(
                        rowType, plainSelect);

        assertNotNull(predicate);

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate expectedPredicate = builder.isNotNull(0);

        assertEquals(expectedPredicate.toString(), predicate.toString());
    }

    @Test
    public void testConvertSqlWhereToPaimonPredicateWithAnd() {
        String query = "SELECT * FROM table WHERE int_col > 3 AND double_col < 6.6";

        PlainSelect plainSelect = convertToPlainSelect(query);
        Predicate predicate =
                SqlToPaimonPredicateConverter.convertSqlWhereToPaimonPredicate(
                        rowType, plainSelect);

        assertNotNull(predicate);

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate expectedPredicate =
                PredicateBuilder.and(builder.greaterThan(7, 3), builder.lessThan(10, 6.6d));

        assertEquals(expectedPredicate.toString(), predicate.toString());
    }

    @Test
    public void testConvertSqlWhereToPaimonPredicateWithOr() {
        String query = "SELECT * FROM table WHERE int_col > 3 OR double_col < 6.6";

        PlainSelect plainSelect = convertToPlainSelect(query);
        Predicate predicate =
                SqlToPaimonPredicateConverter.convertSqlWhereToPaimonPredicate(
                        rowType, plainSelect);

        assertNotNull(predicate);

        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate expectedPredicate =
                PredicateBuilder.or(builder.greaterThan(7, 3), builder.lessThan(10, 6.6d));

        assertEquals(expectedPredicate.toString(), predicate.toString());
    }

    @Test
    public void testConvertSqlSelectToPaimonProjectionArrayWithALL() {
        String query = "SELECT * FROM table WHERE int_col > 3 OR double_col < 6.6";

        PlainSelect plainSelect = convertToPlainSelect(query);
        int[] projectionIndex =
                SqlToPaimonPredicateConverter.convertSqlSelectToPaimonProjectionIndex(
                        fieldNames, plainSelect);

        assertNull(projectionIndex);
    }

    @Test
    public void testConvertSqlSelectToPaimonProjectionArrayWithStar() {
        String query =
                "SELECT decimal_col, int_col, char_col, timestamp_col, boolean_col FROM table WHERE int_col > 3 OR double_col < 6.6";

        PlainSelect plainSelect = convertToPlainSelect(query);
        int[] projectionIndex =
                SqlToPaimonPredicateConverter.convertSqlSelectToPaimonProjectionIndex(
                        fieldNames, plainSelect);

        int[] expectedProjectionIndex = {4, 7, 0, 12, 2};
        assertArrayEquals(projectionIndex, expectedProjectionIndex);
    }
}
