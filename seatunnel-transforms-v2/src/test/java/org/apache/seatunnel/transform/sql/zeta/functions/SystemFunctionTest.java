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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.transform.sql.SQLTransform;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SystemFunctionTest {

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
    public void testCoalesceAndIfNull() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "stringField", "intField"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });

        SeaTunnelRow row1 =
                runSql(
                        "select id, COALESCE(stringField, intField) as result from dual",
                        rowType,
                        1,
                        "test",
                        123);
        Assertions.assertEquals("test", row1.getField(1));

        SeaTunnelRow row2 =
                runSql(
                        "select id, COALESCE(stringField, intField) as result from dual",
                        rowType,
                        1,
                        null,
                        123);
        Assertions.assertEquals("123", row2.getField(1));

        SeaTunnelRow row3 =
                runSql(
                        "select id, IFNULL(stringField, intField) as result from dual",
                        rowType,
                        1,
                        null,
                        123);
        Assertions.assertEquals("123", row3.getField(1));
    }

    @Test
    public void testNullIf() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"a", "b"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.INT_TYPE});

        SeaTunnelRow row1 = runSql("select NULLIF(a, b) as r from dual", rowType, 1, 1);
        Assertions.assertNull(row1.getField(0));

        SeaTunnelRow row2 = runSql("select NULLIF(a, b) as r from dual", rowType, 2, 1);
        Assertions.assertEquals(2, row2.getField(0));
    }

    @Test
    public void testMultiIf() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"age"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        SeaTunnelRow r1 =
                runSql(
                        "select MULTI_IF(age < 18, 'Minor', age < 30, 'Young', 'Adult') as category from dual",
                        rowType,
                        16);
        SeaTunnelRow r2 =
                runSql(
                        "select MULTI_IF(age < 18, 'Minor', age < 30, 'Young', 'Adult') as category from dual",
                        rowType,
                        25);
        SeaTunnelRow r3 =
                runSql(
                        "select MULTI_IF(age < 18, 'Minor', age < 30, 'Young', 'Adult') as category from dual",
                        rowType,
                        40);

        Assertions.assertEquals("Minor", r1.getField(0));
        Assertions.assertEquals("Young", r2.getField(0));
        Assertions.assertEquals("Adult", r3.getField(0));
    }

    @Test
    public void testUuidFormat() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        SeaTunnelRow outRow = runSql("select UUID() as uuid from dual", rowType, 1);

        Object uuidObj = outRow.getField(0);
        Assertions.assertNotNull(uuidObj);
        Assertions.assertTrue(uuidObj instanceof String);
        String uuid = (String) uuidObj;
        Assertions.assertEquals(36, uuid.length());
        Assertions.assertEquals(4, uuid.chars().filter(ch -> ch == '-').count());
    }

    @Test
    public void testCastAsFromVariousTypes() {
        // INT -> STRING
        List<Object> args = new ArrayList<>();
        args.add(123);
        args.add(SqlType.STRING.toString());
        Assertions.assertEquals("123", SystemFunction.castAs(args));

        // STRING -> INT
        args.clear();
        args.add("456");
        args.add(SqlType.INT.toString());
        Assertions.assertEquals(456, SystemFunction.castAs(args));

        // STRING -> BIGINT
        args.clear();
        args.add("789");
        args.add(SqlType.BIGINT.toString());
        Assertions.assertEquals(789L, SystemFunction.castAs(args));

        // LONG -> DATETIME
        args.clear();
        long epochMillis = 1672545600000L;
        args.add(epochMillis);
        args.add("DATETIME");
        Object dt = SystemFunction.castAs(args);
        Assertions.assertTrue(dt instanceof LocalDateTime);
    }

    @Test
    public void testCastAsDateAndTimeFromEncodedInt() {
        // DATE from 20240615
        List<Object> args = new ArrayList<>();
        args.add(20240615);
        args.add("DATE");
        Object d = SystemFunction.castAs(args);
        Assertions.assertEquals(LocalDate.of(2024, 6, 15), d);

        // TIME from 123045
        args.clear();
        args.add(123045);
        args.add("TIME");
        Object t = SystemFunction.castAs(args);
        Assertions.assertEquals(LocalTime.of(12, 30, 45), t);
    }

    @Test
    public void testCastAsDoesNotReturnNullWhenValueEqualsTypeName() {
        Object result = SystemFunction.castAs(Arrays.asList("VARCHAR", "VARCHAR"));
        Assertions.assertEquals("VARCHAR", result);
    }

    @Test
    public void testCastAsDecimalRounding() {
        List<Object> args = new ArrayList<>();
        args.add("1.234");
        args.add("DECIMAL");
        args.add(5);
        args.add(2);

        Object result = SystemFunction.castAs(args);
        Assertions.assertTrue(result instanceof BigDecimal);
        Assertions.assertEquals(new BigDecimal("1.24"), result);
    }

    @Test
    public void testCoalesceRespectsTargetType() {
        SeaTunnelDataType<?> targetType = BasicType.INT_TYPE;
        Object result = SystemFunction.coalesce(Arrays.asList(null, "123"), targetType);

        Assertions.assertEquals(123, result);
    }

    @Test
    public void testCoalesceIfNullAndArrayHelpers() {
        List<Object> values = new ArrayList<>();
        values.add(null);
        values.add("first");
        values.add("second");

        Object result = SystemFunction.coalesce(values, BasicType.STRING_TYPE);
        Assertions.assertEquals("first", result);

        List<Object> ifNullArgs = new ArrayList<>();
        ifNullArgs.add(null);
        ifNullArgs.add("fallback");
        Object ifNullResult = SystemFunction.ifnull(ifNullArgs, BasicType.STRING_TYPE);
        Assertions.assertEquals("fallback", ifNullResult);

        Assertions.assertThrows(
                org.apache.seatunnel.transform.exception.TransformException.class,
                () ->
                        SystemFunction.ifnull(
                                Collections.singletonList("onlyOneArg"), BasicType.STRING_TYPE));

        List<Object> arrayArgs = new ArrayList<>();
        arrayArgs.add("a");
        arrayArgs.add(null);
        arrayArgs.add(1);
        String[] array = SystemFunction.array(arrayArgs);
        Assertions.assertArrayEquals(new String[] {"a", null, "1"}, array);

        Assertions.assertEquals(0, SystemFunction.array(Collections.emptyList()).length);
    }

    @Test
    public void testNullIfFunctionDirectly() {
        List<Object> args = new ArrayList<>();
        args.add(1);
        args.add(1);
        Assertions.assertNull(SystemFunction.nullif(args));

        args.clear();
        args.add(1);
        args.add(2);
        Assertions.assertEquals(1, SystemFunction.nullif(args));

        args.clear();
        args.add(null);
        args.add(2);
        Assertions.assertNull(SystemFunction.nullif(args));
    }

    @Test
    public void testCastAsPrimitiveAndBinaryTypes() {
        List<Object> args = new ArrayList<>();

        args.add("1");
        args.add("TINYINT");
        Assertions.assertEquals((byte) 1, SystemFunction.castAs(args));

        args.clear();
        args.add("2");
        args.add("SMALLINT");
        Assertions.assertEquals((short) 2, SystemFunction.castAs(args));

        args.clear();
        args.add("3");
        args.add("INT");
        Assertions.assertEquals(3, SystemFunction.castAs(args));

        args.clear();
        args.add("4");
        args.add("BIGINT");
        Assertions.assertEquals(4L, SystemFunction.castAs(args));

        args.clear();
        args.add("5");
        args.add("BYTE");
        Assertions.assertEquals((byte) 5, SystemFunction.castAs(args));

        args.clear();
        args.add("hello");
        args.add("BYTES");
        Object bytesResult = SystemFunction.castAs(args);
        Assertions.assertTrue(bytesResult instanceof byte[]);
        Assertions.assertArrayEquals(
                "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8), (byte[]) bytesResult);

        args.clear();
        args.add("3.14");
        args.add("DOUBLE");
        Assertions.assertEquals(3.14d, (Double) SystemFunction.castAs(args), 1e-9);

        args.clear();
        args.add("1.5");
        args.add("FLOAT");
        Assertions.assertEquals(1.5f, (Float) SystemFunction.castAs(args), 1e-6);
    }

    @Test
    public void testCastAsTimestampAndDateTimeVariants() {
        List<Object> args = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        args.add(now);
        args.add("TIMESTAMP");
        Assertions.assertEquals(now, SystemFunction.castAs(args));

        args.clear();
        long epochMillis = 1700000000000L;
        args.add(epochMillis);
        args.add("TIMESTAMP");
        Object ts = SystemFunction.castAs(args);
        Assertions.assertTrue(ts instanceof LocalDateTime);

        args.clear();
        args.add(now);
        args.add("DATE");
        Object date = SystemFunction.castAs(args);
        Assertions.assertEquals(now.toLocalDate(), date);

        args.clear();
        args.add(now);
        args.add("TIME");
        Object time = SystemFunction.castAs(args);
        Assertions.assertEquals(now.toLocalTime(), time);
    }

    @Test
    public void testCastAsFromOffsetDateTime() {
        OffsetDateTime odt = OffsetDateTime.of(2024, 3, 15, 10, 30, 0, 0, ZoneOffset.ofHours(9));

        // CAST(odt AS TIMESTAMP) → wall-clock LocalDateTime
        List<Object> args = new ArrayList<>();
        args.add(odt);
        args.add("TIMESTAMP");
        Object ts = SystemFunction.castAs(args);
        Assertions.assertTrue(ts instanceof LocalDateTime);
        Assertions.assertEquals(odt.toLocalDateTime(), ts);

        // CAST(odt AS DATE) → wall-clock LocalDate
        args.clear();
        args.add(odt);
        args.add("DATE");
        Object date = SystemFunction.castAs(args);
        Assertions.assertEquals(odt.toLocalDate(), date);

        // CAST(odt AS TIME) → wall-clock LocalTime
        args.clear();
        args.add(odt);
        args.add("TIME");
        Object time = SystemFunction.castAs(args);
        Assertions.assertEquals(odt.toLocalTime(), time);

        // CAST(odt AS BIGINT) → epoch millis (UTC)
        args.clear();
        args.add(odt);
        args.add("BIGINT");
        Object epochMillis = SystemFunction.castAs(args);
        Assertions.assertTrue(epochMillis instanceof Long);
        Assertions.assertEquals(odt.toInstant().toEpochMilli(), epochMillis);
    }

    @Test
    public void testCastAsBooleanFromNumberStringAndBoolean() {
        List<Object> args = new ArrayList<>();

        args.add(1);
        args.add("BOOLEAN");
        Assertions.assertEquals(true, SystemFunction.castAs(args));

        args.clear();
        args.add(0);
        args.add("BOOLEAN");
        Assertions.assertEquals(false, SystemFunction.castAs(args));

        args.clear();
        args.add("true");
        args.add("BOOLEAN");
        Assertions.assertEquals(true, SystemFunction.castAs(args));

        args.clear();
        args.add("FALSE");
        args.add("BOOLEAN");
        Assertions.assertEquals(false, SystemFunction.castAs(args));

        args.clear();
        args.add(true);
        args.add("BOOLEAN");
        Assertions.assertEquals(true, SystemFunction.castAs(args));

        args.clear();
        args.add(2);
        args.add("BOOLEAN");
        Assertions.assertThrows(
                org.apache.seatunnel.transform.exception.TransformException.class,
                () -> SystemFunction.castAs(args));

        args.clear();
        args.add("notBool");
        args.add("BOOLEAN");
        Assertions.assertThrows(
                org.apache.seatunnel.transform.exception.TransformException.class,
                () -> SystemFunction.castAs(args));
    }
}
