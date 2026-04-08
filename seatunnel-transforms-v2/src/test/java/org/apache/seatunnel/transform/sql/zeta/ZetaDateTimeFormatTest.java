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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class ZetaDateTimeFormatTest {

    @Test
    public void testFromPatternWithAllDateTimeFormats() {
        // DATETIME_STANDARD
        Optional<ZetaDateTimeFormat> format1 =
                ZetaDateTimeFormat.fromPattern("yyyy-MM-dd HH:mm:ss");
        Assertions.assertTrue(format1.isPresent());
        Assertions.assertEquals(ZetaDateTimeFormat.DATETIME_STANDARD, format1.get());
        Assertions.assertEquals(ZetaDateTimeFormat.FormatType.DATETIME, format1.get().getType());

        // DATETIME_WITH_MILLIS
        Optional<ZetaDateTimeFormat> format2 =
                ZetaDateTimeFormat.fromPattern("yyyy-MM-dd HH:mm:ss.SSS");
        Assertions.assertTrue(format2.isPresent());
        Assertions.assertEquals(ZetaDateTimeFormat.DATETIME_WITH_MILLIS, format2.get());
        Assertions.assertEquals(ZetaDateTimeFormat.FormatType.DATETIME, format2.get().getType());

        // DATETIME_ISO8601
        Optional<ZetaDateTimeFormat> format3 =
                ZetaDateTimeFormat.fromPattern("yyyy-MM-dd'T'HH:mm:ss");
        Assertions.assertTrue(format3.isPresent());
        Assertions.assertEquals(ZetaDateTimeFormat.DATETIME_ISO8601, format3.get());
        Assertions.assertEquals(ZetaDateTimeFormat.FormatType.DATETIME, format3.get().getType());

        // DATETIME_ISO8601_WITH_MILLIS
        Optional<ZetaDateTimeFormat> format4 =
                ZetaDateTimeFormat.fromPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
        Assertions.assertTrue(format4.isPresent());
        Assertions.assertEquals(ZetaDateTimeFormat.DATETIME_ISO8601_WITH_MILLIS, format4.get());
        Assertions.assertEquals(ZetaDateTimeFormat.FormatType.DATETIME, format4.get().getType());

        // DATETIME_SLASH
        Optional<ZetaDateTimeFormat> format5 =
                ZetaDateTimeFormat.fromPattern("yyyy/MM/dd HH:mm:ss");
        Assertions.assertTrue(format5.isPresent());
        Assertions.assertEquals(ZetaDateTimeFormat.DATETIME_SLASH, format5.get());
        Assertions.assertEquals(ZetaDateTimeFormat.FormatType.DATETIME, format5.get().getType());

        // DATETIME_SLASH_WITH_MILLIS
        Optional<ZetaDateTimeFormat> format6 =
                ZetaDateTimeFormat.fromPattern("yyyy/MM/dd HH:mm:ss.SSS");
        Assertions.assertTrue(format6.isPresent());
        Assertions.assertEquals(ZetaDateTimeFormat.DATETIME_SLASH_WITH_MILLIS, format6.get());
        Assertions.assertEquals(ZetaDateTimeFormat.FormatType.DATETIME, format6.get().getType());

        // DATETIME_COMPACT
        Optional<ZetaDateTimeFormat> format7 = ZetaDateTimeFormat.fromPattern("yyyyMMddHHmmss");
        Assertions.assertTrue(format7.isPresent());
        Assertions.assertEquals(ZetaDateTimeFormat.DATETIME_COMPACT, format7.get());
        Assertions.assertEquals(ZetaDateTimeFormat.FormatType.DATETIME, format7.get().getType());
    }

    @Test
    public void testFromPatternWithAllDateFormats() {
        // DATE_ISO8601
        Optional<ZetaDateTimeFormat> format1 = ZetaDateTimeFormat.fromPattern("yyyy-MM-dd");
        Assertions.assertTrue(format1.isPresent());
        Assertions.assertEquals(ZetaDateTimeFormat.DATE_ISO8601, format1.get());
        Assertions.assertEquals(ZetaDateTimeFormat.FormatType.DATE, format1.get().getType());

        // DATE_SLASH
        Optional<ZetaDateTimeFormat> format2 = ZetaDateTimeFormat.fromPattern("yyyy/MM/dd");
        Assertions.assertTrue(format2.isPresent());
        Assertions.assertEquals(ZetaDateTimeFormat.DATE_SLASH, format2.get());
        Assertions.assertEquals(ZetaDateTimeFormat.FormatType.DATE, format2.get().getType());

        // DATE_COMPACT
        Optional<ZetaDateTimeFormat> format3 = ZetaDateTimeFormat.fromPattern("yyyyMMdd");
        Assertions.assertTrue(format3.isPresent());
        Assertions.assertEquals(ZetaDateTimeFormat.DATE_COMPACT, format3.get());
        Assertions.assertEquals(ZetaDateTimeFormat.FormatType.DATE, format3.get().getType());
    }

    @Test
    public void testFromPatternWithAllTimeFormats() {
        // TIME_STANDARD
        Optional<ZetaDateTimeFormat> format1 = ZetaDateTimeFormat.fromPattern("HH:mm:ss");
        Assertions.assertTrue(format1.isPresent());
        Assertions.assertEquals(ZetaDateTimeFormat.TIME_STANDARD, format1.get());
        Assertions.assertEquals(ZetaDateTimeFormat.FormatType.TIME, format1.get().getType());

        // TIME_WITH_MILLIS
        Optional<ZetaDateTimeFormat> format2 = ZetaDateTimeFormat.fromPattern("HH:mm:ss.SSS");
        Assertions.assertTrue(format2.isPresent());
        Assertions.assertEquals(ZetaDateTimeFormat.TIME_WITH_MILLIS, format2.get());
        Assertions.assertEquals(ZetaDateTimeFormat.FormatType.TIME, format2.get().getType());

        // TIME_COMPACT
        Optional<ZetaDateTimeFormat> format3 = ZetaDateTimeFormat.fromPattern("HHmmss");
        Assertions.assertTrue(format3.isPresent());
        Assertions.assertEquals(ZetaDateTimeFormat.TIME_COMPACT, format3.get());
        Assertions.assertEquals(ZetaDateTimeFormat.FormatType.TIME, format3.get().getType());
    }

    @Test
    public void testFromPatternWithInvalidFormat() {
        Optional<ZetaDateTimeFormat> format = ZetaDateTimeFormat.fromPattern("invalid_pattern");

        Assertions.assertFalse(format.isPresent());
    }

    @Test
    public void testFromPatternWithNullFormat() {
        Optional<ZetaDateTimeFormat> format = ZetaDateTimeFormat.fromPattern(null);

        Assertions.assertFalse(format.isPresent());
    }

    @Test
    public void testAllDateTimeFormatsHaveCorrectType() {
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.DATETIME,
                ZetaDateTimeFormat.DATETIME_STANDARD.getType());
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.DATETIME,
                ZetaDateTimeFormat.DATETIME_WITH_MILLIS.getType());
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.DATETIME,
                ZetaDateTimeFormat.DATETIME_ISO8601.getType());
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.DATETIME,
                ZetaDateTimeFormat.DATETIME_ISO8601_WITH_MILLIS.getType());
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.DATETIME,
                ZetaDateTimeFormat.DATETIME_SLASH.getType());
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.DATETIME,
                ZetaDateTimeFormat.DATETIME_SLASH_WITH_MILLIS.getType());
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.DATETIME,
                ZetaDateTimeFormat.DATETIME_COMPACT.getType());
    }

    @Test
    public void testAllDateFormatsHaveCorrectType() {
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.DATE, ZetaDateTimeFormat.DATE_ISO8601.getType());
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.DATE, ZetaDateTimeFormat.DATE_SLASH.getType());
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.DATE, ZetaDateTimeFormat.DATE_COMPACT.getType());
    }

    @Test
    public void testAllTimeFormatsHaveCorrectType() {
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.TIME, ZetaDateTimeFormat.TIME_STANDARD.getType());
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.TIME, ZetaDateTimeFormat.TIME_WITH_MILLIS.getType());
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.TIME, ZetaDateTimeFormat.TIME_COMPACT.getType());
    }

    @Test
    public void testGetPatternForAllFormats() {
        Assertions.assertEquals(
                "yyyy-MM-dd HH:mm:ss", ZetaDateTimeFormat.DATETIME_STANDARD.getPattern());
        Assertions.assertEquals(
                "yyyy-MM-dd HH:mm:ss.SSS", ZetaDateTimeFormat.DATETIME_WITH_MILLIS.getPattern());
        Assertions.assertEquals(
                "yyyy-MM-dd'T'HH:mm:ss", ZetaDateTimeFormat.DATETIME_ISO8601.getPattern());
        Assertions.assertEquals(
                "yyyy-MM-dd'T'HH:mm:ss.SSS",
                ZetaDateTimeFormat.DATETIME_ISO8601_WITH_MILLIS.getPattern());
        Assertions.assertEquals(
                "yyyy/MM/dd HH:mm:ss", ZetaDateTimeFormat.DATETIME_SLASH.getPattern());
        Assertions.assertEquals(
                "yyyy/MM/dd HH:mm:ss.SSS",
                ZetaDateTimeFormat.DATETIME_SLASH_WITH_MILLIS.getPattern());
        Assertions.assertEquals("yyyyMMddHHmmss", ZetaDateTimeFormat.DATETIME_COMPACT.getPattern());

        Assertions.assertEquals("yyyy-MM-dd", ZetaDateTimeFormat.DATE_ISO8601.getPattern());
        Assertions.assertEquals("yyyy/MM/dd", ZetaDateTimeFormat.DATE_SLASH.getPattern());
        Assertions.assertEquals("yyyyMMdd", ZetaDateTimeFormat.DATE_COMPACT.getPattern());

        Assertions.assertEquals("HH:mm:ss", ZetaDateTimeFormat.TIME_STANDARD.getPattern());
        Assertions.assertEquals("HH:mm:ss.SSS", ZetaDateTimeFormat.TIME_WITH_MILLIS.getPattern());
        Assertions.assertEquals("HHmmss", ZetaDateTimeFormat.TIME_COMPACT.getPattern());
    }

    @Test
    public void testFromPatternIsCaseSensitive() {
        Optional<ZetaDateTimeFormat> format = ZetaDateTimeFormat.fromPattern("YYYY-MM-DD HH:MM:SS");

        Assertions.assertFalse(format.isPresent());
    }

    @Test
    public void testAllEnumValuesAreUnique() {
        ZetaDateTimeFormat[] formats = ZetaDateTimeFormat.values();

        for (int i = 0; i < formats.length; i++) {
            for (int j = i + 1; j < formats.length; j++) {
                Assertions.assertNotEquals(
                        formats[i].getPattern(),
                        formats[j].getPattern(),
                        "Duplicate pattern found: " + formats[i].getPattern());
            }
        }
    }

    @Test
    public void testFormatterIsCached() {
        ZetaDateTimeFormat format = ZetaDateTimeFormat.DATETIME_STANDARD;
        Assertions.assertNotNull(format.getFormatter());
        Assertions.assertSame(
                format.getFormatter(),
                format.getFormatter(),
                "Formatter should be cached and return the same instance");
    }

    @Test
    public void testAllFormatsHaveValidFormatter() {
        for (ZetaDateTimeFormat format : ZetaDateTimeFormat.values()) {
            Assertions.assertNotNull(
                    format.getFormatter(),
                    "Format " + format.name() + " should have a valid formatter");
        }
    }

    @Test
    public void testFormatterCanParseValidInput() {
        ZetaDateTimeFormat format = ZetaDateTimeFormat.DATE_ISO8601;
        Assertions.assertDoesNotThrow(
                () -> java.time.LocalDate.parse("2024-06-15", format.getFormatter()));

        ZetaDateTimeFormat compactFormat = ZetaDateTimeFormat.DATE_COMPACT;
        Assertions.assertDoesNotThrow(
                () -> java.time.LocalDate.parse("20240615", compactFormat.getFormatter()));

        ZetaDateTimeFormat slashFormat = ZetaDateTimeFormat.DATE_SLASH;
        Assertions.assertDoesNotThrow(
                () -> java.time.LocalDate.parse("2024/06/15", slashFormat.getFormatter()));
    }
}
