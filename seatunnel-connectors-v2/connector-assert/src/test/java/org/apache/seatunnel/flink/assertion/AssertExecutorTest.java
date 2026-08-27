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

package org.apache.seatunnel.flink.assertion;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.assertion.excecutor.AssertExecutor;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertFieldRule;
import org.apache.seatunnel.format.json.JsonToRowConverters;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AssertExecutorTest {
    SeaTunnelRow row = new SeaTunnelRow(new Object[] {"jared", 17});
    SeaTunnelRowType rowType =
            new SeaTunnelRowType(
                    new String[] {"name", "age"},
                    new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
    AssertExecutor assertExecutor = new AssertExecutor();

    @Test
    public void testFailWithType() {
        List<AssertFieldRule> rules = Lists.newArrayList();
        AssertFieldRule rule1 = new AssertFieldRule();
        rule1.setFieldName("name");
        rule1.setFieldType(BasicType.INT_TYPE);
        rules.add(rule1);

        AssertFieldRule failRule = assertExecutor.fail(row, rowType, rules).orElse(null);
        assertNotNull(failRule);
    }

    @Test
    public void testFailWithValue() {
        List<AssertFieldRule> rules = Lists.newArrayList();
        AssertFieldRule rule1 = getFieldRule4Name();
        AssertFieldRule rule2 = getFieldRule4Age();

        rules.add(rule1);
        rules.add(rule2);

        AssertFieldRule failRule = assertExecutor.fail(row, rowType, rules).orElse(null);
        assertNull(failRule);
    }

    private AssertFieldRule getFieldRule4Age() {
        AssertFieldRule rule = new AssertFieldRule();
        rule.setFieldName("age");
        rule.setFieldType(BasicType.INT_TYPE);

        List<AssertFieldRule.AssertRule> valueRules = Lists.newArrayList();

        AssertFieldRule.AssertRule valueRule = new AssertFieldRule.AssertRule();
        valueRule.setRuleType(AssertFieldRule.AssertRuleType.NOT_NULL);
        AssertFieldRule.AssertRule valueRule1 = new AssertFieldRule.AssertRule();
        valueRule1.setRuleType(AssertFieldRule.AssertRuleType.MIN);
        valueRule1.setRuleValue(13.0);
        AssertFieldRule.AssertRule valueRule2 = new AssertFieldRule.AssertRule();
        valueRule2.setRuleType(AssertFieldRule.AssertRuleType.MAX);
        valueRule2.setRuleValue(25.0);

        valueRules.add(valueRule);
        valueRules.add(valueRule1);
        valueRules.add(valueRule2);
        rule.setFieldRules(valueRules);
        return rule;
    }

    private AssertFieldRule getFieldRule4Name() {
        AssertFieldRule rule = new AssertFieldRule();
        rule.setFieldName("name");
        rule.setFieldType(BasicType.STRING_TYPE);

        List<AssertFieldRule.AssertRule> valueRules = Lists.newArrayList();

        AssertFieldRule.AssertRule valueRule = new AssertFieldRule.AssertRule();
        valueRule.setRuleType(AssertFieldRule.AssertRuleType.NOT_NULL);
        AssertFieldRule.AssertRule valueRule1 = new AssertFieldRule.AssertRule();
        valueRule1.setRuleType(AssertFieldRule.AssertRuleType.MIN_LENGTH);
        valueRule1.setRuleValue(3.0);
        AssertFieldRule.AssertRule valueRule2 = new AssertFieldRule.AssertRule();
        valueRule2.setRuleType(AssertFieldRule.AssertRuleType.MAX_LENGTH);
        valueRule2.setRuleValue(5.0);

        valueRules.add(valueRule);
        valueRules.add(valueRule1);
        valueRules.add(valueRule2);
        rule.setFieldRules(valueRules);
        return rule;
    }

    @Test
    public void testDecimalTypeCheck() {
        assertFieldRuleNotNull(new DecimalType(10, 2), new BigDecimal("99999999.90"));
    }

    @Test
    public void testDecimalTypeCheckError() {
        List<AssertFieldRule> rules = Lists.newArrayList();
        AssertFieldRule rule = new AssertFieldRule();
        rule.setFieldName("c_mock");
        DecimalType assertFieldType = new DecimalType(1, 0);
        rule.setFieldType(assertFieldType);

        AssertFieldRule.AssertRule valueRule = new AssertFieldRule.AssertRule();
        valueRule.setRuleType(AssertFieldRule.AssertRuleType.NOT_NULL);
        rule.setFieldRules(Collections.singletonList(valueRule));
        rules.add(rule);

        SeaTunnelRow mockRow = new SeaTunnelRow(new Object[] {BigDecimal.valueOf(99999999.99)});
        SeaTunnelRowType mockType =
                new SeaTunnelRowType(
                        new String[] {"c_mock"}, new SeaTunnelDataType[] {new DecimalType(10, 2)});

        AssertFieldRule failRule = assertExecutor.fail(mockRow, mockType, rules).orElse(null);
        assertNotNull(failRule);
        assertEquals(assertFieldType, failRule.getFieldType());
        assertEquals("c_mock", failRule.getFieldName());
    }

    @Test
    public void testDecimalEqualsTo() {
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = \"999999.90\" }").getValue("equals_to"),
                new DecimalType(10, 2),
                new BigDecimal("999999.90"));
    }

    @Test
    public void testRowTypeCheck() {
        SeaTunnelRowType assertFieldType =
                new SeaTunnelRowType(
                        new String[] {"c_0"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        assertFieldRuleNotNull(assertFieldType, new SeaTunnelRow(new Object[] {0}));
    }

    @Test
    public void testRowEqualsTo() {
        SeaTunnelRowType assertFieldType =
                new SeaTunnelRowType(
                        new String[] {"c_0", "c_1"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = [0, \"xx\"]}").getValue("equals_to"),
                assertFieldType,
                new SeaTunnelRow(new Object[] {0, "xx"}));
    }

    @Test
    public void testNestRowEqualsTo() {
        SeaTunnelRowType assertFieldType =
                new SeaTunnelRowType(
                        new String[] {"c_0"},
                        new SeaTunnelDataType[] {
                            new SeaTunnelRowType(
                                    new String[] {"c_0_0"},
                                    new SeaTunnelDataType[] {BasicType.INT_TYPE})
                        });
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = [[1]]}").getValue("equals_to"),
                assertFieldType,
                new SeaTunnelRow(new Object[] {new SeaTunnelRow(new Object[] {1})}));
    }

    @Test
    public void testArrayTypeCheck() {
        assertFieldRuleNotNull(ArrayType.INT_ARRAY_TYPE, new Integer[] {0, 1, 2});
    }

    @Test
    public void testArrayEqualsTo() {
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = [0, 1, 2]}").getValue("equals_to"),
                ArrayType.INT_ARRAY_TYPE,
                new Integer[] {0, 1, 2});
    }

    @Test
    public void testMapTypeCheck() {
        Map<String, String> map = new HashMap<>();
        map.put("k0", "v0");
        assertFieldRuleNotNull(
                new MapType<String, String>(BasicType.STRING_TYPE, BasicType.STRING_TYPE), map);
    }

    @Test
    public void testMapEqualsTo() {
        Map<String, String> map = new HashMap<>();
        map.put("k0", "v0");
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = { k0 = v0 } }").getValue("equals_to"),
                new MapType<String, String>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                map);
    }

    @Test
    public void testNullTypeCheck() {
        assertFieldRuleNull(BasicType.VOID_TYPE, null);
    }

    @Test
    public void testStringEqualsTo() {
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = \"string\" }").getValue("equals_to"),
                BasicType.STRING_TYPE,
                "string");
    }

    @Test
    public void testBooleanEqualsTo() {
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = false }").getValue("equals_to"),
                BasicType.BOOLEAN_TYPE,
                false);
    }

    @Test
    public void testTinyIntEqualsTo() {
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = 1 }").getValue("equals_to"),
                BasicType.BYTE_TYPE,
                (byte) 1);
    }

    @Test
    public void testSmallIntEqualsTo() {
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = 1 }").getValue("equals_to"),
                BasicType.SHORT_TYPE,
                (short) 1);
    }

    @Test
    public void testIntEqualsTo() {
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = 333 }").getValue("equals_to"),
                BasicType.INT_TYPE,
                (int) 333);
    }

    @Test
    public void testBigIntEqualsTo() {
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = 323232 }").getValue("equals_to"),
                BasicType.LONG_TYPE,
                (long) 323232L);
    }

    @Test
    public void testFloatEqualsTo() {
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = 3.1 }").getValue("equals_to"),
                BasicType.FLOAT_TYPE,
                (float) 3.1);
    }

    @Test
    public void testDoubleEqualsTo() {
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = 19.33333 }").getValue("equals_to"),
                BasicType.DOUBLE_TYPE,
                (double) 19.33333);
    }

    @Test
    public void testBytesEqualsTo() throws IOException {
        byte[] bytes = "010101".getBytes();
        String base64Str = Base64.getEncoder().encodeToString("010101".getBytes());
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = \"" + base64Str + "\" }")
                        .getValue("equals_to"),
                PrimitiveByteArrayType.INSTANCE,
                (byte[]) bytes);
    }

    @Test
    public void testDateEqualsTo() throws IOException {
        String dateStr = "2024-01-24";
        LocalDate date =
                DateTimeFormatter.ISO_LOCAL_DATE.parse(dateStr).query(TemporalQueries.localDate());
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = \"" + dateStr + "\" }")
                        .getValue("equals_to"),
                LocalTimeType.LOCAL_DATE_TYPE,
                (LocalDate) date);
    }

    @Test
    public void testTimeEqualsTo() throws IOException {
        String timeStr = "12:11:34";
        LocalTime time =
                JsonToRowConverters.TIME_FORMAT.parse(timeStr).query(TemporalQueries.localTime());
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = \"" + timeStr + "\" }")
                        .getValue("equals_to"),
                LocalTimeType.LOCAL_TIME_TYPE,
                (LocalTime) time);
    }

    @Test
    public void testTimestampEqualsTo() throws IOException {
        String timestampStr = "2024-01-24T12:11:34.123";
        TemporalAccessor parsedTimestamp =
                DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(timestampStr);
        LocalTime time = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate date = parsedTimestamp.query(TemporalQueries.localDate());
        LocalDateTime timestamp = LocalDateTime.of(date, time);
        assertFieldRuleEqualsTo(
                ConfigFactory.parseString("{equals_to = \"" + timestampStr + "\" }")
                        .getValue("equals_to"),
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                (LocalDateTime) timestamp);
    }

    private void assertFieldRuleNotNull(SeaTunnelDataType<?> type, Object value) {
        assertFieldRuleMayNull(type, value, false);
    }

    private void assertFieldRuleNull(SeaTunnelDataType<?> type, Object value) {
        assertFieldRuleMayNull(type, value, true);
    }

    private void assertFieldRuleMayNull(SeaTunnelDataType<?> type, Object value, boolean isNull) {
        List<AssertFieldRule> rules = Lists.newArrayList();
        AssertFieldRule rule = new AssertFieldRule();
        rule.setFieldName("c_mock");
        rule.setFieldType(type);

        AssertFieldRule.AssertRule valueRule = new AssertFieldRule.AssertRule();
        valueRule.setRuleType(
                isNull
                        ? AssertFieldRule.AssertRuleType.NULL
                        : AssertFieldRule.AssertRuleType.NOT_NULL);

        rule.setFieldRules(Collections.singletonList(valueRule));
        rules.add(rule);

        SeaTunnelRow mockRow = new SeaTunnelRow(new Object[] {value});
        SeaTunnelRowType mockType =
                new SeaTunnelRowType(new String[] {"c_mock"}, new SeaTunnelDataType[] {type});

        AssertFieldRule failRule = assertExecutor.fail(mockRow, mockType, rules).orElse(null);
        assertNull(failRule);
    }

    private void assertFieldRuleEqualsTo(
            ConfigValue equalsTo, SeaTunnelDataType<?> type, Object expected) {
        assertFieldRuleEqualsTo(equalsTo, type, expected, true);
    }

    private void assertFieldRuleEqualsTo(
            ConfigValue equalsTo, SeaTunnelDataType<?> type, Object expected, boolean isEqualsTo) {
        List<AssertFieldRule> rules = Lists.newArrayList();
        AssertFieldRule rule = new AssertFieldRule();
        rule.setFieldName("c_mock");
        rule.setFieldType(type);

        AssertFieldRule.AssertRule valueRule = new AssertFieldRule.AssertRule();
        valueRule.setEqualTo(equalsTo.unwrapped());

        rule.setFieldRules(Collections.singletonList(valueRule));
        rules.add(rule);

        SeaTunnelRow mockRow = new SeaTunnelRow(new Object[] {expected});
        SeaTunnelRowType mockType =
                new SeaTunnelRowType(new String[] {"c_mock"}, new SeaTunnelDataType[] {type});

        AssertFieldRule failRule = assertExecutor.fail(mockRow, mockType, rules).orElse(null);
        if (isEqualsTo) {
            assertNull(failRule);
        } else {
            assertNotNull(failRule);
        }
    }
}
