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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SQLStringFunctionsTest {

    private SeaTunnelRow runSql(String query, SeaTunnelRowType rowType, Object... values) {
        CatalogTable table = CatalogTableUtil.getCatalogTable("test", rowType);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.singletonMap("query", query));
        SQLTransform transform = new SQLTransform(config, table);
        List<SeaTunnelRow> out = transform.transformRow(new SeaTunnelRow(values));
        Assertions.assertNotNull(out);
        Assertions.assertFalse(out.isEmpty());
        return out.get(0);
    }

    @Test
    public void testBasicStringFunctions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select ASCII(name) as a,"
                                + " CHAR_LENGTH(name) as len,"
                                + " LOWER(name) as lcase,"
                                + " UPPER(name) as ucase"
                                + " from dual",
                        rowType,
                        "Ab");

        Assertions.assertEquals(65, outRow.getField(0));
        Assertions.assertEquals(2L, outRow.getField(1));
        Assertions.assertEquals("ab", outRow.getField(2));
        Assertions.assertEquals("AB", outRow.getField(3));
    }

    @Test
    public void testConcatAndConcatWs() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"first_name", "last_name"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select CONCAT(first_name, '_', last_name) as c1,"
                                + " CONCAT_WS(' ', first_name, last_name) as c2"
                                + " from dual",
                        rowType,
                        "John",
                        "Doe");

        Assertions.assertEquals("John_Doe", outRow.getField(0));
        Assertions.assertEquals("John Doe", outRow.getField(1));
    }

    @Test
    public void testTrimAndNestedFunctions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"first_name", "last_name"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select TRIM(CONCAT('  ', first_name, ' ', last_name, '  ')) as full_name,"
                                + " UPPER(TRIM(first_name)) as upper_first"
                                + " from dual",
                        rowType,
                        "John",
                        "Doe");

        Assertions.assertEquals("John Doe", outRow.getField(0));
        Assertions.assertEquals("JOHN", outRow.getField(1));
    }

    @Test
    public void testRegexpFunctions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select REGEXP_REPLACE(text, ' +', ' ') as r1,"
                                + " REGEXP_LIKE(text, '[A-Z ]*', 'i') as r2,"
                                + " REGEXP_SUBSTR(text, '[0-9]{4}') as r3"
                                + " from dual",
                        rowType, "2020    YEAR");

        Assertions.assertEquals("2020 YEAR", outRow.getField(0));
        Assertions.assertEquals(true, outRow.getField(1));
        Assertions.assertEquals("2020", outRow.getField(2));
    }

    @Test
    public void testRegexpInvalidFlags() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        TransformException ex =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                runSql(
                                        "select REGEXP_LIKE(text, 'a.*', 'x') as r from dual",
                                        rowType,
                                        "abc"));

        Assertions.assertTrue(ex.getMessage().contains("REGEXP_LIKE"));
        if (ex.getCause() != null) {
            Assertions.assertTrue(
                    ex.getCause().getMessage().contains("REGEXP_LIKE"),
                    "Cause message should mention REGEXP_LIKE, but was: "
                            + ex.getCause().getMessage());
        }
    }

    @Test
    public void testSplitFunction() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql("select SPLIT(text, ';') as parts from dual", rowType, "a;b;c");

        Object[] parts = (Object[]) outRow.getField(0);
        Assertions.assertArrayEquals(new Object[] {"a", "b", "c"}, parts);
    }

    @Test
    public void testSplitWithNull() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql("select SPLIT(text, ';') as parts from dual", rowType, (Object) null);

        Assertions.assertNull(outRow.getField(0));
    }

    @Test
    public void testToCharFunction() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"num"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        SeaTunnelRow outRow = runSql("select TO_CHAR(num) as s from dual", rowType, 123);

        Assertions.assertEquals("123", outRow.getField(0));
    }

    @Test
    public void testReplaceAndSpace() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select REPLACE(text, 'old', 'new') as r1,"
                                + " SPACE(3) as r2"
                                + " from dual",
                        rowType,
                        "old text");

        Assertions.assertEquals("new text", outRow.getField(0));
        Assertions.assertEquals(3, ((String) outRow.getField(1)).length());
        Assertions.assertTrue(
                ((String) outRow.getField(1)).chars().allMatch(ch -> ch == ' '),
                "SPACE(3) should return only spaces, but was: "
                        + Arrays.toString(((String) outRow.getField(1)).toCharArray()));
    }

    @Test
    public void testLocateInstrAndPosition() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select LOCATE('lo', text) as l1,"
                                + " LOCATE('lo', text, 5) as l2,"
                                + " INSTR(text, 'lo') as i1,"
                                + " POSITION('lo', text) as p1"
                                + " from dual",
                        rowType,
                        "hello");

        Assertions.assertEquals(4, outRow.getField(0));
        Assertions.assertEquals(0, outRow.getField(1));
        Assertions.assertEquals(4, outRow.getField(2));
        Assertions.assertEquals(4, outRow.getField(3));
    }

    @Test
    public void testInsertLeftRightAndPad() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select INSERT(text, 2, 2, 'yy') as ins,"
                                + " LEFT(text, 3) as l,"
                                + " RIGHT(text, 2) as r,"
                                + " LPAD(text, 5, 'x') as lp,"
                                + " RPAD(text, 5, 'x') as rp"
                                + " from dual",
                        rowType,
                        "abcd");

        Assertions.assertEquals("ayyd", outRow.getField(0));
        Assertions.assertEquals("abc", outRow.getField(1));
        Assertions.assertEquals("cd", outRow.getField(2));
        Assertions.assertEquals("xabcd", outRow.getField(3));
        Assertions.assertEquals("abcdx", outRow.getField(4));
    }

    @Test
    public void testHextorawAndRawtohex() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select HEXTORAW('0041') as s1," + " RAWTOHEX('A') as s2" + " from dual",
                        rowType,
                        1);

        Assertions.assertEquals("A", outRow.getField(0));
        Assertions.assertEquals("0041", outRow.getField(1));
    }

    @Test
    public void testHextorawWithInvalidLength() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        Assertions.assertThrows(
                TransformException.class,
                () -> runSql("select HEXTORAW('001') as s from dual", rowType, 1));
    }

    @Test
    public void testRawtohexWithBytesColumn() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"data"}, new SeaTunnelDataType[] {BasicType.BYTE_TYPE});

        byte[] bytes = new byte[] {0x01, 0x0A};
        SeaTunnelRow outRow = runSql("select RAWTOHEX(data) as s from dual", rowType, bytes);

        Assertions.assertEquals("010a", outRow.getField(0));
    }

    @Test
    public void testToBase64() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"data"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select TO_BASE64(data) as encoded_data,"
                                + " TO_BASE64(data, 'UTF-16') as utf16_encoded_data"
                                + " from dual",
                        rowType,
                        "SeaTunnel");

        Assertions.assertEquals("U2VhVHVubmVs", outRow.getField(0));
        Assertions.assertEquals("/v8AUwBlAGEAVAB1AG4AbgBlAGw=", outRow.getField(1));
    }

    @Test
    public void testFromBase64() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"data"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select FROM_BASE64(data) as decoded_data,"
                                + " FROM_BASE64('/v8AUwBlAGEAVAB1AG4AbgBlAGw=', 'UTF-16') as utf16_decoded_data"
                                + " from dual",
                        rowType,
                        "U2VhVHVubmVs");

        Assertions.assertEquals("SeaTunnel", outRow.getField(0));
        Assertions.assertEquals("SeaTunnel", outRow.getField(1));
    }

    @Test
    public void testToBase64WithBytesColumn() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"data"},
                        new SeaTunnelDataType[] {PrimitiveByteArrayType.INSTANCE});

        byte[] bytes = "SeaTunnel".getBytes(StandardCharsets.UTF_8);
        SeaTunnelRow outRow =
                runSql("select TO_BASE64(data) as encoded_data from dual", rowType, bytes);

        Assertions.assertEquals("U2VhVHVubmVs", outRow.getField(0));
    }

    @Test
    public void testToBase64BytesColumnRejectsCharset() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"data"},
                        new SeaTunnelDataType[] {PrimitiveByteArrayType.INSTANCE});

        byte[] bytes = "SeaTunnel".getBytes(StandardCharsets.UTF_8);
        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                runSql(
                                        "select TO_BASE64(data, 'UTF-16') as encoded_data from dual",
                                        rowType,
                                        bytes));

        Assertions.assertInstanceOf(IllegalArgumentException.class, exception.getCause());
        Assertions.assertEquals(
                "TO_BASE64 does not support charset for bytes input",
                exception.getCause().getMessage());
    }

    @Test
    public void testSoundex() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow = runSql("select SOUNDEX(name) as sx from dual", rowType, "Smith");

        Assertions.assertEquals("S530", outRow.getField(0));
    }

    @Test
    public void testSubstringAndSubstr() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select SUBSTRING(text, 2) as s1,"
                                + " SUBSTRING(text, 2, 2) as s2,"
                                + " SUBSTR(text, -2) as s3"
                                + " from dual",
                        rowType,
                        "Hello");

        Assertions.assertEquals("ello", outRow.getField(0));
        Assertions.assertEquals("el", outRow.getField(1));
        Assertions.assertEquals("lo", outRow.getField(2));
    }

    @Test
    public void testTrimVariants() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select LTRIM(text, 'x') as lt,"
                                + " RTRIM(text, 'x') as rt,"
                                + " TRIM(text, 'x') as tt"
                                + " from dual",
                        rowType,
                        "xxhelloxx");

        Assertions.assertEquals("helloxx", outRow.getField(0));
        Assertions.assertEquals("xxhello", outRow.getField(1));
        Assertions.assertEquals("hello", outRow.getField(2));
    }

    @Test
    public void testTranslate() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql("select TRANSLATE(text, 'eo', 'EO') as t from dual", rowType, "Hello world");

        Assertions.assertEquals("HEllO wOrld", outRow.getField(0));
    }

    // ==================== Boundary Tests ====================

    @Test
    public void testAsciiWithEmptyString() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        // Empty string should return null (after fix)
        SeaTunnelRow outRow = runSql("select ASCII(name) as a from dual", rowType, "");
        Assertions.assertNull(outRow.getField(0));
    }

    @Test
    public void testAsciiWithNull() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow = runSql("select ASCII(name) as a from dual", rowType, (Object) null);
        Assertions.assertNull(outRow.getField(0));
    }

    @Test
    public void testLeftWithNegativeCount() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        // Negative count should return empty string (after fix)
        SeaTunnelRow outRow = runSql("select LEFT(text, -1) as l from dual", rowType, "Hello");
        Assertions.assertEquals("", outRow.getField(0));
    }

    @Test
    public void testRightWithNegativeCount() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        // Negative count should return empty string (after fix)
        SeaTunnelRow outRow = runSql("select RIGHT(text, -1) as r from dual", rowType, "Hello");
        Assertions.assertEquals("", outRow.getField(0));
    }

    @Test
    public void testLeftWithZeroCount() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow = runSql("select LEFT(text, 0) as l from dual", rowType, "Hello");
        Assertions.assertEquals("", outRow.getField(0));
    }

    @Test
    public void testRightWithZeroCount() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow = runSql("select RIGHT(text, 0) as r from dual", rowType, "Hello");
        Assertions.assertEquals("", outRow.getField(0));
    }

    @Test
    public void testLeftRightExceedingLength() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select LEFT(text, 100) as l, RIGHT(text, 100) as r from dual",
                        rowType,
                        "Hi");

        Assertions.assertEquals("Hi", outRow.getField(0));
        Assertions.assertEquals("Hi", outRow.getField(1));
    }

    @Test
    public void testConcatWithNulls() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"a", "b"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

        SeaTunnelRow outRow = runSql("select CONCAT(a, b) as c from dual", rowType, "Hello", null);

        // CONCAT should skip null values
        Assertions.assertEquals("Hello", outRow.getField(0));
    }

    @Test
    public void testSubstringBoundary() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        // Start beyond string length should return null
        SeaTunnelRow outRow =
                runSql("select SUBSTRING(text, 100) as s from dual", rowType, "Hello");
        Assertions.assertNull(outRow.getField(0));
    }

    @Test
    public void testNestedTrimCoalesceAndUpper() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name", "backup"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

        // when name is not null, go through UPPER then TRIM
        SeaTunnelRow row1 =
                runSql(
                        "select TRIM(COALESCE(UPPER(name), backup)) as res from dual",
                        rowType,
                        "  john  ",
                        "fallback");
        Assertions.assertEquals("JOHN", row1.getField(0));

        // when name is null, use backup value then TRIM
        SeaTunnelRow row2 =
                runSql(
                        "select TRIM(COALESCE(UPPER(name), backup)) as res from dual",
                        rowType,
                        null,
                        "  default  ");
        Assertions.assertEquals("default", row2.getField(0));
    }
}
