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
    }

    @Test
    public void testFromPatternWithAllDateFormats() {
        // DATE_ISO8601
        Optional<ZetaDateTimeFormat> format1 = ZetaDateTimeFormat.fromPattern("yyyy-MM-dd");
        Assertions.assertTrue(format1.isPresent());
        Assertions.assertEquals(ZetaDateTimeFormat.DATE_ISO8601, format1.get());
        Assertions.assertEquals(ZetaDateTimeFormat.FormatType.DATE, format1.get().getType());
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
    }

    @Test
    public void testAllDateFormatsHaveCorrectType() {
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.DATE, ZetaDateTimeFormat.DATE_ISO8601.getType());
    }

    @Test
    public void testAllTimeFormatsHaveCorrectType() {
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.TIME, ZetaDateTimeFormat.TIME_STANDARD.getType());
        Assertions.assertEquals(
                ZetaDateTimeFormat.FormatType.TIME, ZetaDateTimeFormat.TIME_WITH_MILLIS.getType());
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

        Assertions.assertEquals("yyyy-MM-dd", ZetaDateTimeFormat.DATE_ISO8601.getPattern());

        Assertions.assertEquals("HH:mm:ss", ZetaDateTimeFormat.TIME_STANDARD.getPattern());
        Assertions.assertEquals("HH:mm:ss.SSS", ZetaDateTimeFormat.TIME_WITH_MILLIS.getPattern());
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
}
