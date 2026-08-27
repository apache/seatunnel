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

package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@Slf4j
public class ObjectUtilsTest {
    @Test
    public void testObjectUtilsMinusWithFloat() throws Exception {
        // Test precision-sensitive Float values
        Float minuend = 123.456f;
        Float subtrahend = 23.456f;

        BigDecimal result = ObjectUtils.minus(minuend, subtrahend);

        // Verify that using toString() method prevents precision loss
        BigDecimal expected =
                new BigDecimal(minuend.toString()).subtract(new BigDecimal(subtrahend.toString()));
        assertEquals(expected, result);

        // Verify the difference from the old method (this test should demonstrate the fix
        // necessity)
        BigDecimal oldMinuend = BigDecimal.valueOf(minuend);
        BigDecimal oldSubtrahend = BigDecimal.valueOf(subtrahend);
        BigDecimal oldWay = oldMinuend.subtract(oldSubtrahend);
        assertNotEquals(oldWay, result);

        // Test values that better demonstrate precision issues
        Float precisionMinuend = 0.3f;
        Float precisionSubtrahend = 0.1f;
        BigDecimal precisionResult = ObjectUtils.minus(precisionMinuend, precisionSubtrahend);
        BigDecimal precisionExpected =
                new BigDecimal(precisionMinuend.toString())
                        .subtract(new BigDecimal(precisionSubtrahend.toString()));
        assertEquals(precisionExpected, precisionResult);

        // Verify that the old method indeed has precision issues
        BigDecimal oldPrecisionWay =
                BigDecimal.valueOf(precisionMinuend)
                        .subtract(BigDecimal.valueOf(precisionSubtrahend));
        assertNotEquals(oldPrecisionWay, precisionResult);
    }
}
