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
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FakeDataRandomUtils {
    private final FakeConfig fakeConfig;

    public FakeDataRandomUtils(FakeConfig fakeConfig) {
        this.fakeConfig = fakeConfig;
    }

    private static <T> T randomFromList(List<T> list) {
        int index = RandomUtils.nextInt(0, list.size());
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

    public ByteBuffer randomBinaryVector() {
        int byteCount = fakeConfig.getBinaryVectorDimension() / 8;
        // binary vector doesn't care endian since each byte is independent
        return ByteBuffer.wrap(RandomUtils.nextBytes(byteCount));
    }

    public Float[] randomFloatVector() {
        Float[] floatVector = new Float[fakeConfig.getVectorDimension()];
        for (int i = 0; i < fakeConfig.getVectorDimension(); i++) {
            floatVector[i] =
                    RandomUtils.nextFloat(
                            fakeConfig.getVectorFloatMin(), fakeConfig.getVectorFloatMax());
        }
        return floatVector;
    }

    public ByteBuffer randomFloat16Vector() {
        Short[] float16Vector = new Short[fakeConfig.getVectorDimension()];
        for (int i = 0; i < fakeConfig.getVectorDimension(); i++) {
            float value =
                    RandomUtils.nextFloat(
                            fakeConfig.getVectorFloatMin(), fakeConfig.getVectorFloatMax());
            float16Vector[i] = floatToFloat16(value);
        }
        return shortArrayToByteBuffer(float16Vector);
    }

    public ByteBuffer randomBFloat16Vector() {
        Short[] bfloat16Vector = new Short[fakeConfig.getVectorDimension()];
        for (int i = 0; i < fakeConfig.getVectorDimension(); i++) {
            float value =
                    RandomUtils.nextFloat(
                            fakeConfig.getVectorFloatMin(), fakeConfig.getVectorFloatMax());
            bfloat16Vector[i] = floatToBFloat16(value);
        }
        return shortArrayToByteBuffer(bfloat16Vector);
    }

    public Map<Integer, Float> randomSparseFloatVector() {
        Map<Integer, Float> sparseVector = new HashMap<>();

        Integer nonZeroElements = fakeConfig.getVectorDimension();
        while (nonZeroElements > 0) {
            Integer index = RandomUtils.nextInt();
            Float value =
                    RandomUtils.nextFloat(
                            fakeConfig.getVectorFloatMin(), fakeConfig.getVectorFloatMax());
            if (!sparseVector.containsKey(index)) {
                sparseVector.put(index, value);
                nonZeroElements--;
            }
        }

        return sparseVector;
    }

    private static short floatToFloat16(float value) {
        int intBits = Float.floatToIntBits(value);
        int sign = (intBits >>> 16) & 0x8000;
        int exponent = ((intBits >>> 23) & 0xff) - 112;
        int mantissa = intBits & 0x007fffff;

        if (exponent <= 0) {
            return (short) sign;
        } else if (exponent > 0x1f) {
            return (short) (sign | 0x7c00);
        }
        return (short) (sign | (exponent << 10) | (mantissa >> 13));
    }

    private static ByteBuffer shortArrayToByteBuffer(Short[] shortArray) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(shortArray.length * 2);

        for (Short value : shortArray) {
            byteBuffer.putShort(value);
        }

        // Compatible compilation and running versions are not consistent
        // Flip the buffer to prepare for reading
        ((Buffer) byteBuffer).flip();

        return byteBuffer;
    }

    private static short floatToBFloat16(float value) {
        int intBits = Float.floatToIntBits(value);
        return (short) (intBits >> 16);
    }
}
