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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Unit tests for JdbcFieldTypeUtils, specifically for OffsetDateTime parsing. Tests cover SQL
 * Server DateTimeOffset format with variable precision.
 */
public class JdbcFieldTypeUtilsTest {

    /**
     * Test parsing SQL Server DateTimeOffset with 6 decimal places (microseconds). This is the
     * standard SQL Server precision.
     */
    @Test
    public void testParseOffsetDateTimeFromStringSqlServer6Decimals() {
        String input = "2025-11-04 21:10:06.891977 +00:00";
        OffsetDateTime result = JdbcFieldTypeUtils.parseOffsetDateTimeFromString(input);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(11, result.getMonthValue());
        Assertions.assertEquals(4, result.getDayOfMonth());
        Assertions.assertEquals(21, result.getHour());
        Assertions.assertEquals(10, result.getMinute());
        Assertions.assertEquals(6, result.getSecond());
        Assertions.assertEquals(ZoneOffset.UTC, result.getOffset());
    }

    /**
     * Test parsing SQL Server DateTimeOffset with 3 decimal places (milliseconds). This is the case
     * from the bug report: "2025-11-05 05:54:15.069 +00:00"
     */
    @Test
    public void testParseOffsetDateTimeFromStringSqlServer3Decimals() {
        String input = "2025-11-05 05:54:15.069 +00:00";
        OffsetDateTime result = JdbcFieldTypeUtils.parseOffsetDateTimeFromString(input);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(11, result.getMonthValue());
        Assertions.assertEquals(5, result.getDayOfMonth());
        Assertions.assertEquals(5, result.getHour());
        Assertions.assertEquals(54, result.getMinute());
        Assertions.assertEquals(15, result.getSecond());
        Assertions.assertEquals(ZoneOffset.UTC, result.getOffset());
    }

    /** Test parsing SQL Server DateTimeOffset with 1 decimal place. */
    @Test
    public void testParseOffsetDateTimeFromStringSqlServer1Decimal() {
        String input = "2025-11-05 05:54:15.1 +00:00";
        OffsetDateTime result = JdbcFieldTypeUtils.parseOffsetDateTimeFromString(input);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(11, result.getMonthValue());
        Assertions.assertEquals(5, result.getDayOfMonth());
        Assertions.assertEquals(5, result.getHour());
        Assertions.assertEquals(54, result.getMinute());
        Assertions.assertEquals(15, result.getSecond());
        Assertions.assertEquals(ZoneOffset.UTC, result.getOffset());
    }

    /** Test parsing SQL Server DateTimeOffset with negative offset. */
    @Test
    public void testParseOffsetDateTimeFromStringSqlServerNegativeOffset() {
        String input = "2025-11-05 05:54:15.069 -05:00";
        OffsetDateTime result = JdbcFieldTypeUtils.parseOffsetDateTimeFromString(input);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(11, result.getMonthValue());
        Assertions.assertEquals(5, result.getDayOfMonth());
        Assertions.assertEquals(5, result.getHour());
        Assertions.assertEquals(54, result.getMinute());
        Assertions.assertEquals(15, result.getSecond());
        Assertions.assertEquals(ZoneOffset.of("-05:00"), result.getOffset());
    }

    /** Test parsing SQL Server DateTimeOffset with positive offset. */
    @Test
    public void testParseOffsetDateTimeFromStringSqlServerPositiveOffset() {
        String input = "2025-11-05 05:54:15.069 +08:00";
        OffsetDateTime result = JdbcFieldTypeUtils.parseOffsetDateTimeFromString(input);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(11, result.getMonthValue());
        Assertions.assertEquals(5, result.getDayOfMonth());
        Assertions.assertEquals(5, result.getHour());
        Assertions.assertEquals(54, result.getMinute());
        Assertions.assertEquals(15, result.getSecond());
        Assertions.assertEquals(ZoneOffset.of("+08:00"), result.getOffset());
    }

    /** Test parsing ISO-8601 format (standard format). */
    @Test
    public void testParseOffsetDateTimeFromStringISO8601() {
        String input = "2025-11-05T05:54:15.069+00:00";
        OffsetDateTime result = JdbcFieldTypeUtils.parseOffsetDateTimeFromString(input);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(11, result.getMonthValue());
        Assertions.assertEquals(5, result.getDayOfMonth());
    }

    /** Test parsing with null input. */
    @Test
    public void testParseOffsetDateTimeFromStringNull() {
        OffsetDateTime result = JdbcFieldTypeUtils.parseOffsetDateTimeFromString(null);
        Assertions.assertNull(result);
    }

    /** Test parsing with empty string. */
    @Test
    public void testParseOffsetDateTimeFromStringEmpty() {
        OffsetDateTime result = JdbcFieldTypeUtils.parseOffsetDateTimeFromString("");
        Assertions.assertNull(result);
    }

    /** Test parsing with whitespace only. */
    @Test
    public void testParseOffsetDateTimeFromStringWhitespace() {
        OffsetDateTime result = JdbcFieldTypeUtils.parseOffsetDateTimeFromString("   ");
        Assertions.assertNull(result);
    }

    /** Test parsing with invalid format should throw exception. */
    @Test
    public void testParseOffsetDateTimeFromStringInvalidFormat() {
        String input = "invalid-date-time";
        Assertions.assertThrows(
                java.time.format.DateTimeParseException.class,
                () -> JdbcFieldTypeUtils.parseOffsetDateTimeFromString(input));
    }

    /** Test parsing SQL Server DateTimeOffset with 7 decimal places. */
    @Test
    public void testParseOffsetDateTimeFromStringSqlServer7Decimals() {
        String input = "2025-11-05 05:54:15.0691234 +00:00";
        OffsetDateTime result = JdbcFieldTypeUtils.parseOffsetDateTimeFromString(input);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(11, result.getMonthValue());
        Assertions.assertEquals(5, result.getDayOfMonth());
        Assertions.assertEquals(5, result.getHour());
        Assertions.assertEquals(54, result.getMinute());
        Assertions.assertEquals(15, result.getSecond());
        Assertions.assertEquals(ZoneOffset.UTC, result.getOffset());
    }

    /** Test parsing SQL Server DateTimeOffset without fractional seconds. */
    @Test
    public void testParseOffsetDateTimeFromStringSqlServerNoFractional() {
        String input = "2025-11-05 05:54:15 +00:00";
        OffsetDateTime result = JdbcFieldTypeUtils.parseOffsetDateTimeFromString(input);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(11, result.getMonthValue());
        Assertions.assertEquals(5, result.getDayOfMonth());
        Assertions.assertEquals(5, result.getHour());
        Assertions.assertEquals(54, result.getMinute());
        Assertions.assertEquals(15, result.getSecond());
        Assertions.assertEquals(ZoneOffset.UTC, result.getOffset());
    }
}
