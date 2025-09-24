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

package org.apache.seatunnel.connectors.sensorsdata.format.utils;

import org.apache.seatunnel.connectors.sensorsdata.format.exception.SensorsDataException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

class TypeUtilTest {

    DateTimeFormatter formatter3 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
    DateTimeFormatter formatter4 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.systemDefault());

    @Test
    void testToTargetType() {
        // 1. Number
        Assertions.assertEquals(123, TypeUtil.toTargetType(123, "NUMBER"));
        Assertions.assertEquals(123L, TypeUtil.toTargetType(123L, "NUMBER"));
        Assertions.assertEquals(123.1, TypeUtil.toTargetType(123.1, "NUMBER"));
        Assertions.assertEquals(123, TypeUtil.toTargetType("123", "NUMBER"));
        Assertions.assertEquals(
                ((Double) 123.1).floatValue(), TypeUtil.toTargetType("123.1", "NUMBER"));
        // 2. Boolean
        Assertions.assertEquals(true, TypeUtil.toTargetType(1, "BOOLEAN"));
        Assertions.assertEquals(false, TypeUtil.toTargetType(0, "BOOLEAN"));
        Assertions.assertEquals(false, TypeUtil.toTargetType(0.0, "BOOLEAN"));
        Assertions.assertEquals(true, TypeUtil.toTargetType("true", "BOOLEAN"));
        Assertions.assertEquals(false, TypeUtil.toTargetType("f", "BOOLEAN"));
        // 3. Timestamp
        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
        Assertions.assertEquals(
                ZonedDateTime.from(formatter.parse("2024-03-16 19:25:07"))
                        .toInstant()
                        .toEpochMilli(),
                TypeUtil.toTargetType("2024-03-16 19:25:07", "TIMESTAMP"));
        Assertions.assertEquals(
                1710588307000L, TypeUtil.toTargetType(new Date(1710588307000L), "TIMESTAMP"));
        Assertions.assertEquals(
                1710588307000.0, TypeUtil.toTargetType(1710588307000.0, "TIMESTAMP"));

        formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneId.systemDefault());
        Assertions.assertEquals(
                ZonedDateTime.from(formatter.parse("20240316_192507")).toInstant().toEpochMilli(),
                TypeUtil.toTargetType("20240316_192507", "TIMESTAMP"));

        formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                        .withZone(ZoneId.systemDefault());
        Assertions.assertEquals(
                ZonedDateTime.from(formatter.parse("2024-03-16 19:25:07.123"))
                        .toInstant()
                        .toEpochMilli(),
                TypeUtil.toTargetType("2024-03-16 19:25:07.123", "TIMESTAMP"));

        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ");
        Assertions.assertEquals(
                ZonedDateTime.from(formatter.parse("2024-03-16T19:25:07+0100"))
                        .toInstant()
                        .toEpochMilli(),
                TypeUtil.toTargetType(
                        "2024-03-16T19:25:07+0100", "TIMESTAMP yyyy-MM-dd'T'HH:mm:ssZ"));
        Assertions.assertEquals(
                "20240316 192507", TypeUtil.toTargetType("20240316 192507", "TIMESTAMP"));

        // 4. List
        Assertions.assertEquals(
                Arrays.asList("123", "456"), TypeUtil.toTargetType("123\n456", "LIST"));
        Assertions.assertEquals(
                Arrays.asList("123", "456"), TypeUtil.toTargetType("123,456", "LIST_COMMA"));
        Assertions.assertEquals(
                Collections.singletonList("456"), TypeUtil.toTargetType(";456", "LIST_SEMICOLON"));
        Assertions.assertEquals(
                Collections.singletonList("123"), TypeUtil.toTargetType("123", "LIST"));
        Assertions.assertThrowsExactly(
                SensorsDataException.class, () -> TypeUtil.toTargetType(123, "LIST"));
    }
}
