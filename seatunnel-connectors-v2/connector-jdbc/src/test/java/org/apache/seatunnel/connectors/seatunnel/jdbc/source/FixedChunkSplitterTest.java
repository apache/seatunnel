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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;

import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@Slf4j
public class FixedChunkSplitterTest {

    @Test
    public void testConvertFloat() throws Exception {
        JdbcSourceConfig config =
                JdbcSourceConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:postgresql://localhost:5432/test")
                                        .driverName("org.postgresql.Driver")
                                        .build())
                        .build();

        FixedChunkSplitter splitter = new FixedChunkSplitter(config);

        // Use reflection to access private method
        Method convertToBigDecimalMethod =
                FixedChunkSplitter.class.getDeclaredMethod("convertToBigDecimal", Object.class);
        convertToBigDecimalMethod.setAccessible(true);

        // Test precision-sensitive Float values
        Float testFloat = 123.456f;
        BigDecimal result = (BigDecimal) convertToBigDecimalMethod.invoke(splitter, testFloat);

        // Verify that using toString() method prevents precision loss
        BigDecimal expected = new BigDecimal(testFloat.toString());
        assertEquals(expected, result);

        // Verify the difference from the old method (this test should demonstrate the fix
        // necessity)
        BigDecimal oldWay = BigDecimal.valueOf(testFloat);
        assertNotEquals(oldWay, result);

        // Test boundary values
        Float maxFloat = Float.MAX_VALUE;
        BigDecimal maxResult = (BigDecimal) convertToBigDecimalMethod.invoke(splitter, maxFloat);
        assertEquals(new BigDecimal(maxFloat.toString()), maxResult);

        Float minFloat = Float.MIN_VALUE;
        BigDecimal minResult = (BigDecimal) convertToBigDecimalMethod.invoke(splitter, minFloat);
        assertEquals(new BigDecimal(minFloat.toString()), minResult);

        // Test values that better demonstrate precision issues
        Float precisionTestFloat = 0.1f;
        BigDecimal precisionResult =
                (BigDecimal) convertToBigDecimalMethod.invoke(splitter, precisionTestFloat);
        assertEquals(new BigDecimal("0.1"), precisionResult);

        // Verify that the old method indeed has precision issues
        BigDecimal oldPrecisionWay = BigDecimal.valueOf(precisionTestFloat);
        assertNotEquals(new BigDecimal("0.1"), oldPrecisionWay);
    }
}
