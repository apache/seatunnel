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

package org.apache.seatunnel.e2e.connector.doris;

import org.apache.seatunnel.common.utils.JsonUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.security.SecureRandom;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class GenerateTestData {
    private static final String CHARACTERS =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom RANDOM = new SecureRandom();

    private static final LocalDateTime startDateTime = LocalDateTime.of(2022, 1, 1, 0, 0, 0);
    private static final LocalDateTime endDateTime = LocalDateTime.of(2022, 12, 31, 23, 59, 59);

    private static final LocalDate startDate = LocalDate.of(2022, 1, 1);
    private static final LocalDate endDate = LocalDate.of(2022, 12, 31);

    public static String genString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARACTERS.charAt(RANDOM.nextInt(CHARACTERS.length())));
        }
        return sb.toString();
    }

    public static boolean genBoolean() {
        return ThreadLocalRandom.current().nextBoolean();
    }

    public static Double genDouble() {
        return ThreadLocalRandom.current().nextDouble(0.0, 1000.0);
    }

    public static float genFloat(float min, float max) {
        return ThreadLocalRandom.current().nextFloat() * (max - min) + min;
    }

    public static BigInteger genBigInteger(int bits) {
        if (bits > 128) bits = 127;
        return new BigInteger(bits, ThreadLocalRandom.current());
    }

    public static Long genBigint() {
        return ThreadLocalRandom.current().nextLong();
    }

    public static BigInteger genBigInteger() {
        return new BigInteger(128, ThreadLocalRandom.current());
    }

    public static String genDatetimeString(boolean withNano) {
        long startEpochSecond = startDateTime.toEpochSecond(ZoneOffset.UTC);
        long endEpochSecond = endDateTime.toEpochSecond(ZoneOffset.UTC);
        long randomEpochSecond =
                ThreadLocalRandom.current().nextLong(startEpochSecond, endEpochSecond);
        int nano = withNano ? ThreadLocalRandom.current().nextInt(0, 999999) : 0;
        LocalDateTime randomDatetime =
                LocalDateTime.ofEpochSecond(randomEpochSecond, nano, ZoneOffset.UTC);
        return randomDatetime.format(
                DateTimeFormatter.ofPattern(
                        withNano ? "yyyy-MM-dd HH:mm:ss.SSSSSS" : "yyyy-MM-dd HH:mm:ss"));
    }

    public static String genDateString() {
        long startEpochDay = startDate.toEpochDay();
        long endEpochDay = endDate.toEpochDay();
        long randomEpochDay = ThreadLocalRandom.current().nextLong(startEpochDay, endEpochDay + 1);
        LocalDate randomDate = LocalDate.ofEpochDay(randomEpochDay);
        return randomDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }

    public static String genJsonString() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("1", "hai");
        testMap.put("2", "ti");
        String s = JsonUtils.toJsonString(testMap);
        return s;
    }

    public static byte genTinyint() {
        return (byte) ThreadLocalRandom.current().nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE);
    }

    public static short genSmallint() {
        return (short) ThreadLocalRandom.current().nextInt(Short.MIN_VALUE, Short.MAX_VALUE + 1);
    }

    public static Integer genInt() {
        return Integer.valueOf(ThreadLocalRandom.current().nextInt());
    }

    public static BigDecimal genBigDecimal(int totalDigits, int decimalDigits) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long scale = (long) Math.pow(10, decimalDigits);
        long maxValue = (long) Math.pow(10, totalDigits - decimalDigits) - 1;

        long integerPart = Math.abs(random.nextLong() % maxValue);
        long decimalPart = Math.abs(random.nextLong() % scale);

        BigDecimal integer = BigDecimal.valueOf(integerPart);
        BigDecimal decimal = BigDecimal.valueOf(decimalPart, decimalDigits);

        return integer.add(decimal).setScale(decimalDigits, RoundingMode.HALF_UP);
    }
}
