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

import org.apache.seatunnel.transform.exception.TransformException;

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

    @Test
    public void testAsciiWithEmptyAndNull() {
        List<Object> args = new ArrayList<>();
        args.add("");
        Assertions.assertNull(StringFunction.ascii(args));

        args.clear();
        args.add(null);
        Assertions.assertNull(StringFunction.ascii(args));
    }

    @Test
    public void testLeftRightWithNegativeAndZeroCount() {
        List<Object> args = new ArrayList<>();
        args.add("Hello");
        args.add(-1);
        Assertions.assertEquals("", StringFunction.left(args));

        args.clear();
        args.add("Hello");
        args.add(0);
        Assertions.assertEquals("", StringFunction.left(args));

        args.clear();
        args.add("Hello");
        args.add(-1);
        Assertions.assertEquals("", StringFunction.right(args));

        args.clear();
        args.add("Hello");
        args.add(0);
        Assertions.assertEquals("", StringFunction.right(args));

        args.clear();
        args.add("Hi");
        args.add(100);
        Assertions.assertEquals("Hi", StringFunction.left(args));

        args.clear();
        args.add("Hi");
        args.add(100);
        Assertions.assertEquals("Hi", StringFunction.right(args));
    }

    @Test
    public void testLengthFunctions() {
        List<Object> args = new ArrayList<>();
        args.add("abc");
        Assertions.assertEquals(24L, StringFunction.bitLength(args));

        args.clear();
        args.add("abc");
        Assertions.assertEquals(3L, StringFunction.charLength(args));

        args.clear();
        args.add("abc");
        Assertions.assertEquals(3L, StringFunction.octetLength(args));

        // Multi-byte characters: length by chars vs bytes
        args.clear();
        args.add("€A");
        Assertions.assertEquals(2L, StringFunction.charLength(args));

        args.clear();
        args.add("€A");
        // '€' is 3 bytes and 'A' is 1 byte in UTF-8
        Assertions.assertEquals(4L, StringFunction.octetLength(args));
    }

    @Test
    public void testChrFunction() {
        List<Object> args = new ArrayList<>();
        args.add(65);
        Assertions.assertEquals("A", StringFunction.chr(args));

        args.clear();
        args.add(null);
        Assertions.assertNull(StringFunction.chr(args));
    }

    @Test
    public void testConcatAndConcatWs() {
        List<Object> args = new ArrayList<>();
        args.add("Hello");
        args.add(null);
        args.add(" ");
        args.add("World");
        Assertions.assertEquals("Hello World", StringFunction.concat(args));

        args.clear();
        args.add(";");
        args.add("a");
        args.add(null);
        args.add("b");
        Assertions.assertEquals("a;b", StringFunction.concatWs(args));

        args.clear();
        args.add(";");
        args.add(new String[] {"1", "2"});
        args.add("3");
        Assertions.assertEquals("1;2;3", StringFunction.concatWs(args));
    }

    @Test
    public void testHexToRawAndRawToHex() {
        List<Object> args = new ArrayList<>();
        args.add("0041");
        Assertions.assertEquals("A", StringFunction.hextoraw(args));

        args.clear();
        args.add(null);
        Assertions.assertNull(StringFunction.hextoraw(args));

        List<Object> badArgs = new ArrayList<>();
        badArgs.add("001");
        Assertions.assertThrows(TransformException.class, () -> StringFunction.hextoraw(badArgs));

        args.clear();
        args.add("A");
        Assertions.assertEquals("0041", StringFunction.rawtohex(args));

        byte[] bytes = new byte[] {0x01, 0x0A};
        args.clear();
        args.add(bytes);
        Assertions.assertEquals("010a", StringFunction.rawtohex(args));
    }

    @Test
    public void testInsertFunction() {
        List<Object> args = new ArrayList<>();
        args.add("abcd");
        args.add(2);
        args.add(2);
        args.add("yy");
        Assertions.assertEquals("ayyd", StringFunction.insert(args));

        args.clear();
        args.add(null);
        args.add(1);
        args.add(2);
        args.add("x");
        Assertions.assertEquals("x", StringFunction.insert(args));

        args.clear();
        args.add("abcd");
        args.add(1);
        args.add(0);
        args.add("yy");
        Assertions.assertEquals("abcd", StringFunction.insert(args));
    }

    @Test
    public void testLowerAndUpper() {
        List<Object> args = new ArrayList<>();
        args.add("AbC");
        Assertions.assertEquals("abc", StringFunction.lower(args));

        args.clear();
        args.add("AbC");
        Assertions.assertEquals("ABC", StringFunction.upper(args));

        args.clear();
        args.add(null);
        Assertions.assertNull(StringFunction.lower(args));
        Assertions.assertNull(StringFunction.upper(args));
    }

    @Test
    public void testLocationAndInstr() {
        List<Object> args = new ArrayList<>();
        // LOCATE behaviour
        args.add("lo");
        args.add("hello");
        Assertions.assertEquals(4, StringFunction.location("LOCATE", args).intValue());

        args.clear();
        args.add("lo");
        args.add("hellollo");
        args.add(-2);
        Assertions.assertEquals(7, StringFunction.location("LOCATE", args).intValue());

        args.clear();
        args.add("lo");
        args.add(null);
        Assertions.assertEquals(0, StringFunction.location("LOCATE", args).intValue());

        // INSTR behaviour
        args.clear();
        args.add("hello");
        args.add("lo");
        Assertions.assertEquals(4, StringFunction.instr(args).intValue());

        args.clear();
        args.add("hello");
        args.add("lo");
        args.add(5);
        Assertions.assertEquals(0, StringFunction.instr(args).intValue());

        args.clear();
        args.add(null);
        args.add("lo");
        Assertions.assertEquals(0, StringFunction.instr(args).intValue());
    }

    @Test
    public void testPadFunction() {
        List<Object> args = new ArrayList<>();
        args.add("ab");
        args.add(5);
        args.add("x");
        Assertions.assertEquals("xxxab", StringFunction.pad("LPAD", args));
        Assertions.assertEquals("abxxx", StringFunction.pad("RPAD", args));

        args.clear();
        args.add("ab");
        args.add(-1);
        args.add("x");
        Assertions.assertEquals("", StringFunction.pad("LPAD", args));
    }

    @Test
    public void testTrimAndSplitFunctions() {
        List<Object> args = new ArrayList<>();
        args.add("xxhelloxx");
        args.add("x");
        Assertions.assertEquals("helloxx", StringFunction.ltrim(args));

        args.clear();
        args.add("xxhelloxx");
        args.add("x");
        Assertions.assertEquals("xxhello", StringFunction.rtrim(args));

        args.clear();
        args.add("xxhelloxx");
        args.add("x");
        Assertions.assertEquals("hello", StringFunction.trim(args));

        args.clear();
        args.add("  hi  ");
        Assertions.assertEquals("hi", StringFunction.trim(args));

        // split
        args.clear();
        args.add("a;b;c");
        args.add(";");
        String[] parts = StringFunction.split(args);
        Assertions.assertArrayEquals(new String[] {"a", "b", "c"}, parts);

        args.clear();
        args.add(null);
        args.add(";");
        Assertions.assertNull(StringFunction.split(args));

        args.clear();
        args.add("");
        args.add(";");
        Assertions.assertNull(StringFunction.split(args));
    }

    @Test
    public void testRegexpReplaceAndLike() {
        List<Object> args = new ArrayList<>();
        args.add("a   b    c");
        args.add(" +");
        args.add(" ");
        String replaced = StringFunction.regexpReplace(args);
        Assertions.assertEquals("a b c", replaced);

        args.clear();
        args.add("Abc");
        args.add("^a");
        args.add("i");
        Assertions.assertTrue(StringFunction.regexpLike(args));

        args.clear();
        args.add("Abc");
        args.add("^a");
        Assertions.assertFalse(StringFunction.regexpLike(args));
    }

    @Test
    public void testRegexpLikeInvalidFlag() {
        List<Object> args = new ArrayList<>();
        args.add("abc");
        args.add("a.*");
        args.add("x"); // unsupported flag
        Assertions.assertThrows(TransformException.class, () -> StringFunction.regexpLike(args));
    }

    @Test
    public void testRegexpSubstr() {
        List<Object> args = new ArrayList<>();
        args.add("abc-123-def");
        args.add("\\d+");
        Assertions.assertEquals("123", StringFunction.regexpSubstr(args));

        // with position / occurrence / subexpression
        args.clear();
        args.add("ab12cd34");
        args.add("[a-z]+");
        args.add(1); // position
        args.add(2); // occurrence
        args.add(null); // regexpMode
        args.add(0); // entire match
        Assertions.assertEquals("cd", StringFunction.regexpSubstr(args));
    }

    @Test
    public void testRepeatReplaceSoundexAndSpace() {
        List<Object> args = new ArrayList<>();
        args.add("ab");
        args.add(3);
        Assertions.assertEquals("ababab", StringFunction.repeat(args));

        args.clear();
        args.add("ab");
        args.add(0);
        Assertions.assertEquals("", StringFunction.repeat(args));

        // replace
        args.clear();
        args.add("old text");
        args.add("old");
        args.add("new");
        Assertions.assertEquals("new text", StringFunction.replace(args));

        args.clear();
        args.add("oldold");
        args.add("old");
        // third arg omitted -> removed
        Assertions.assertEquals("", StringFunction.replace(args));

        // soundex
        args.clear();
        args.add("Smith");
        Assertions.assertEquals("S530", StringFunction.soundex(args));

        // space
        args.clear();
        args.add(3);
        String spaces = StringFunction.space(args);
        Assertions.assertEquals(3, spaces.length());
        Assertions.assertTrue(spaces.chars().allMatch(ch -> ch == ' '));

        args.clear();
        args.add(null);
        Assertions.assertNull(StringFunction.space(args));
    }

    @Test
    public void testToCharAndTranslate() {
        List<Object> args = new ArrayList<>();
        // Number -> string
        args.add(123);
        Assertions.assertEquals("123", StringFunction.toChar(args));

        // Temporal -> formatted string
        args.clear();
        LocalDateTime dt = LocalDateTime.of(2024, 6, 15, 14, 30, 45);
        args.add(dt);
        args.add("yyyy-MM-dd HH:mm:ss");
        Assertions.assertEquals("2024-06-15 14:30:45", StringFunction.toChar(args));

        // translate
        args.clear();
        args.add("Hello world");
        args.add("eo");
        args.add("EO");
        Assertions.assertEquals("HEllO wOrld", StringFunction.translate(args));
    }
}
