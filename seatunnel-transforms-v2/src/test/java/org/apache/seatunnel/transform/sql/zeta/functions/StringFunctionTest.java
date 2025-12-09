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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class StringFunctionTest {

    @Test
    public void testSubstringWithString() {
        List<Object> args = new ArrayList<>();
        args.add("Hello World");
        args.add(1);
        Assertions.assertEquals("Hello World", StringFunction.substring(args));

        args.clear();
        args.add("Hello World");
        args.add(7);
        Assertions.assertEquals("World", StringFunction.substring(args));

        args.clear();
        args.add("Hello World");
        args.add(1);
        args.add(5);
        Assertions.assertEquals("Hello", StringFunction.substring(args));
    }

    @Test
    public void testSubstringWithLocalDate() {
        List<Object> args = new ArrayList<>();

        // Test LocalDate
        LocalDate date = LocalDate.of(2023, 12, 25);
        args.add(date);
        args.add(1);
        args.add(4);
        Assertions.assertEquals("2023", StringFunction.substring(args));
    }

    @Test
    public void testSubstringWithLocalDateTime() {
        List<Object> args = new ArrayList<>();

        // Test LocalDateTime
        LocalDateTime dateTime = LocalDateTime.of(2023, 12, 25, 15, 30, 45);
        args.add(dateTime);
        args.add(2);
        args.add(6);
        Assertions.assertEquals("023-12", StringFunction.substring(args));
    }

    @Test
    public void testSubstringWithOffsetDateTime() {
        List<Object> args = new ArrayList<>();

        // Test OffsetDateTime
        OffsetDateTime offsetDateTime =
                LocalDateTime.of(2023, 12, 25, 15, 30, 45).atOffset(ZoneOffset.UTC);
        args.add(offsetDateTime);
        args.add(1);
        args.add(4);
        Assertions.assertEquals("2023", StringFunction.substring(args));
    }

    @Test
    public void testSubstringWithUtilDate() {
        List<Object> args = new ArrayList<>();

        // Test java.util.Date
        Date utilDate = new Date(123, 11, 25); // Year 2023 (123 + 1900), Month 12, Day 25
        args.add(utilDate);
        args.add(1);
        args.add(4);
        // Should extract year part from formatted string "2023-12-25 00:00:00"
        Assertions.assertEquals("2023", StringFunction.substring(args));
    }

    @Test
    public void testSubstringWithNullInput() {
        List<Object> args = new ArrayList<>();
        args.add(null);
        args.add(1);
        Assertions.assertNull(StringFunction.substring(args));
    }

    @Test
    public void testSubstringWithTemporal() {
        List<Object> args = new ArrayList<>();

        // Test LocalTime (as a Temporal implementation not explicitly handled)
        Temporal time = LocalTime.of(15, 30, 45);
        args.add(time);
        args.add(1);
        args.add(5);
        // Should extract time part from formatted string "15:30:45"
        Assertions.assertEquals("15:30", StringFunction.substring(args));
    }

    @Test
    public void testAsciiNullAndEmptyReturnNull() {
        List<Object> args = new ArrayList<>();
        args.add(null);
        Assertions.assertNull(StringFunction.ascii(args));

        args.clear();
        args.add("");
        Assertions.assertNull(StringFunction.ascii(args));
    }

    @Test
    public void testLeftAndRightNegativeCountReturnEmpty() {
        List<Object> args = new ArrayList<>();
        args.add("abc");
        args.add(-1);
        Assertions.assertEquals("", StringFunction.left(args));

        args.clear();
        args.add("abc");
        args.add(-2);
        Assertions.assertEquals("", StringFunction.right(args));
    }
}
