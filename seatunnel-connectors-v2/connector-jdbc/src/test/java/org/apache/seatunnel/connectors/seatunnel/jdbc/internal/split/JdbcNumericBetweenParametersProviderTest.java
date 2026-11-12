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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.split;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.Serializable;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcNumericBetweenParametersProviderTest {

    private static final BigDecimal ONE = BigDecimal.ONE;
    private static final BigDecimal DECIMAL_38_MAX =
            new BigDecimal("99999999999999999999999999999999999999");

    @Test
    void splitsRangeByFetchSize() {
        Serializable[][] actual =
                new JdbcNumericBetweenParametersProvider(300, ONE, BigDecimal.valueOf(1000))
                        .getParameterValues();
        BigDecimal[][] expected = {
            {ONE, BigDecimal.valueOf(300)},
            {BigDecimal.valueOf(301), BigDecimal.valueOf(600)},
            {BigDecimal.valueOf(601), BigDecimal.valueOf(900)},
            {BigDecimal.valueOf(901), BigDecimal.valueOf(1000)}
        };
        assertPairs(expected, actual);
    }

    @Test
    void clampsBatchSizeToSingletonRange() {
        Serializable[][] actual =
                new JdbcNumericBetweenParametersProvider(
                                BigDecimal.valueOf(5), BigDecimal.valueOf(5))
                        .ofBatchSize(3)
                        .getParameterValues();
        BigDecimal[][] expected = {{BigDecimal.valueOf(5), BigDecimal.valueOf(5)}};
        assertPairs(expected, actual);
    }

    @Test
    void clampsBatchNumToElementCount() {
        Serializable[][] actual =
                new JdbcNumericBetweenParametersProvider(
                                BigDecimal.valueOf(-2), BigDecimal.valueOf(2))
                        .ofBatchNum(10)
                        .getParameterValues();
        BigDecimal[][] expected = {
            {BigDecimal.valueOf(-2), BigDecimal.valueOf(-2)},
            {BigDecimal.valueOf(-1), BigDecimal.valueOf(-1)},
            {BigDecimal.ZERO, BigDecimal.ZERO},
            {ONE, ONE},
            {BigDecimal.valueOf(2), BigDecimal.valueOf(2)}
        };
        assertPairs(expected, actual);
    }

    @Test
    void keepsExactBoundsForDecimal38Range() {
        Serializable[][] actual =
                new JdbcNumericBetweenParametersProvider(ONE, DECIMAL_38_MAX)
                        .ofBatchNum(3)
                        .getParameterValues();
        assertEquals(3, actual.length);
        assertEquals(ONE, actual[0][0]);
        assertEquals(DECIMAL_38_MAX, actual[2][1]);
        for (int i = 1; i < actual.length; i++) {
            assertEquals(((BigDecimal) actual[i - 1][1]).add(ONE), actual[i][0]);
            assertTrue(((BigDecimal) actual[i][0]).compareTo((BigDecimal) actual[i][1]) <= 0);
        }
    }

    @Test
    void rejectsNonPositiveBatchSize() {
        assertMessage(
                IllegalArgumentException.class,
                "Batch size must be positive",
                () -> new JdbcNumericBetweenParametersProvider(ONE, BigDecimal.TEN).ofBatchSize(0));
    }

    @Test
    void rejectsNonPositiveBatchCount() {
        assertMessage(
                IllegalArgumentException.class,
                "Batch number must be positive",
                () -> new JdbcNumericBetweenParametersProvider(ONE, BigDecimal.TEN).ofBatchNum(0));
    }

    @Test
    void rejectsReversedBounds() {
        assertMessage(
                IllegalArgumentException.class,
                "minVal must not be larger than maxVal",
                () -> new JdbcNumericBetweenParametersProvider(BigDecimal.TEN, ONE));
    }

    @Test
    void rejectsUnconfiguredState() {
        assertMessage(
                IllegalStateException.class,
                "Batch size and batch number must be positive. Have you called `ofBatchSize` or `ofBatchNum`?",
                () ->
                        new JdbcNumericBetweenParametersProvider(ONE, BigDecimal.TEN)
                                .getParameterValues());
    }

    @Test
    void preservesFractionalBoundsWhenClampingToOneBatch() {
        BigDecimal min = new BigDecimal("0.1");
        BigDecimal max = new BigDecimal("0.2");
        assertPairs(
                new BigDecimal[][] {{min, max}},
                new JdbcNumericBetweenParametersProvider(min, max)
                        .ofBatchNum(2)
                        .getParameterValues());
    }

    @Test
    void replacesPreviousPartitionMode() {
        JdbcNumericBetweenParametersProvider provider =
                new JdbcNumericBetweenParametersProvider(ONE, BigDecimal.TEN);
        provider.ofBatchNum(3).ofBatchSize(4);
        assertPairs(
                new BigDecimal[][] {
                    {ONE, BigDecimal.valueOf(4)},
                    {BigDecimal.valueOf(5), BigDecimal.valueOf(8)},
                    {BigDecimal.valueOf(9), BigDecimal.TEN}
                },
                provider.getParameterValues());
        provider.ofBatchNum(3);
        assertPairs(
                new BigDecimal[][] {
                    {ONE, BigDecimal.valueOf(4)},
                    {BigDecimal.valueOf(5), BigDecimal.valueOf(7)},
                    {BigDecimal.valueOf(8), BigDecimal.TEN}
                },
                provider.getParameterValues());
    }

    @Test
    void rejectsTooManyBatchesWithoutChangingConfiguration() {
        BigDecimal min = BigDecimal.valueOf(Long.MIN_VALUE);
        BigDecimal max = BigDecimal.valueOf(Long.MAX_VALUE);
        JdbcNumericBetweenParametersProvider provider =
                new JdbcNumericBetweenParametersProvider(min, max).ofBatchNum(2);
        assertMessage(ArithmeticException.class, "Overflow", () -> provider.ofBatchSize(1));
        assertPairs(
                new BigDecimal[][] {{min, BigDecimal.valueOf(-1)}, {BigDecimal.ZERO, max}},
                provider.getParameterValues());
    }

    private static void assertPairs(BigDecimal[][] expected, Serializable[][] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertArrayEquals(expected[i], actual[i], "pair " + i);
        }
    }

    private static void assertMessage(
            Class<? extends Throwable> type, String message, Executable executable) {
        assertEquals(message, assertThrows(type, executable).getMessage());
    }
}
