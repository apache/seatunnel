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

package org.apache.seatunnel.connectors.seatunnel.fake.utils;

import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

public class FakeDataRandomUtils {
    private final FakeConfig fakeConfig;

    public FakeDataRandomUtils(FakeConfig fakeConfig) {
        this.fakeConfig = fakeConfig;
    }

    private static <T> T randomFromList(List<T> list) {
        int index = RandomUtils.nextInt(0, list.size() - 1);
        return list.get(index);
    }

    public Boolean randomBoolean() {
        return RandomUtils.nextInt(0, 2) == 1;
    }

    public BigDecimal randomBigDecimal(int precision, int scale) {
        return new BigDecimal(
                RandomStringUtils.randomNumeric(precision - scale)
                        + "."
                        + RandomStringUtils.randomNumeric(scale));
    }

    public byte[] randomBytes() {
        return RandomStringUtils.randomAlphabetic(fakeConfig.getBytesLength()).getBytes();
    }

    public String randomString() {
        List<String> stringTemplate = fakeConfig.getStringTemplate();
        if (!CollectionUtils.isEmpty(stringTemplate)) {
            return randomFromList(stringTemplate);
        }
        return RandomStringUtils.randomAlphabetic(fakeConfig.getStringLength());
    }

    public Byte randomTinyint() {
        List<Integer> tinyintTemplate = fakeConfig.getTinyintTemplate();
        if (!CollectionUtils.isEmpty(tinyintTemplate)) {
            return randomFromList(tinyintTemplate).byteValue();
        }
        return (byte) RandomUtils.nextInt(fakeConfig.getTinyintMin(), fakeConfig.getTinyintMax());
    }

    public Short randomSmallint() {
        List<Integer> smallintTemplate = fakeConfig.getSmallintTemplate();
        if (!CollectionUtils.isEmpty(smallintTemplate)) {
            return randomFromList(smallintTemplate).shortValue();
        }
        return (short)
                RandomUtils.nextInt(fakeConfig.getSmallintMin(), fakeConfig.getSmallintMax());
    }

    public Integer randomInt() {
        List<Integer> intTemplate = fakeConfig.getIntTemplate();
        if (!CollectionUtils.isEmpty(intTemplate)) {
            return randomFromList(intTemplate);
        }
        return RandomUtils.nextInt(fakeConfig.getIntMin(), fakeConfig.getIntMax());
    }

    public Long randomBigint() {
        List<Long> bigTemplate = fakeConfig.getBigTemplate();
        if (!CollectionUtils.isEmpty(bigTemplate)) {
            return randomFromList(bigTemplate);
        }
        return RandomUtils.nextLong(fakeConfig.getBigintMin(), fakeConfig.getBigintMax());
    }

    public Float randomFloat() {
        List<Double> floatTemplate = fakeConfig.getFloatTemplate();
        if (!CollectionUtils.isEmpty(floatTemplate)) {
            return randomFromList(floatTemplate).floatValue();
        }
        return RandomUtils.nextFloat(
                (float) fakeConfig.getFloatMin(), (float) fakeConfig.getFloatMax());
    }

    public Double randomDouble() {
        List<Double> doubleTemplate = fakeConfig.getDoubleTemplate();
        if (!CollectionUtils.isEmpty(doubleTemplate)) {
            return randomFromList(doubleTemplate);
        }
        return RandomUtils.nextDouble(fakeConfig.getDoubleMin(), fakeConfig.getDoubleMax());
    }

    public LocalDate randomLocalDate() {
        return randomLocalDateTime().toLocalDate();
    }

    public LocalTime randomLocalTime() {
        return randomLocalDateTime().toLocalTime();
    }

    public LocalDateTime randomLocalDateTime() {
        int year;
        int month;
        int day;
        int hour;
        int minute;
        int second;
        // init year
        if (!CollectionUtils.isEmpty(fakeConfig.getDateYearTemplate())) {
            year = randomFromList(fakeConfig.getDateYearTemplate());
        } else {
            year = LocalDateTime.now().getYear();
        }
        // init month
        if (!CollectionUtils.isEmpty(fakeConfig.getDateMonthTemplate())) {
            month = randomFromList(fakeConfig.getDateMonthTemplate());
        } else {
            month = RandomUtils.nextInt(1, 13);
        }
        // init day
        if (!CollectionUtils.isEmpty(fakeConfig.getDateDayTemplate())) {
            day = randomFromList(fakeConfig.getDateDayTemplate());
        } else {
            day = RandomUtils.nextInt(1, 29);
        }
        // init hour
        if (!CollectionUtils.isEmpty(fakeConfig.getTimeHourTemplate())) {
            hour = randomFromList(fakeConfig.getTimeHourTemplate());
        } else {
            hour = RandomUtils.nextInt(0, 24);
        }
        // init minute
        if (!CollectionUtils.isEmpty(fakeConfig.getTimeMinuteTemplate())) {
            minute = randomFromList(fakeConfig.getTimeMinuteTemplate());
        } else {
            minute = RandomUtils.nextInt(0, 60);
        }
        // init second
        if (!CollectionUtils.isEmpty(fakeConfig.getTimeSecondTemplate())) {
            second = randomFromList(fakeConfig.getTimeSecondTemplate());
        } else {
            second = RandomUtils.nextInt(0, 60);
        }
        return LocalDateTime.of(year, month, day, hour, minute, second);
    }
}
