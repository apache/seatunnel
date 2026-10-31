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
package org.apache.seatunnel.connectors.seatunnel.common.source.arrow.converter;

import org.apache.seatunnel.shade.org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.Types;

import com.google.auto.service.AutoService;

import java.time.Instant;
import java.time.ZoneId;

/**
 * Converts Arrow {@code TIMESTAMPMICROTZ} values from epoch microseconds to {@link
 * java.time.LocalDateTime} using the time zone declared by the Arrow field.
 *
 * <p>{@link Math#floorDiv(long, long)} and {@link Math#floorMod(long, long)} ensure that timestamps
 * before the Unix epoch are converted correctly.
 */
@AutoService(Converter.class)
public class TimeStampMicroTZConverter implements Converter<TimeStampMicroTZVector> {
    @Override
    public Object convert(int rowIndex, TimeStampMicroTZVector fieldVector) {
        if (fieldVector == null || fieldVector.isNull(rowIndex)) {
            return null;
        }

        long epochMicro = fieldVector.getObject(rowIndex);
        long epochSecond = Math.floorDiv(epochMicro, 1_000_000L);
        long microOfSecond = Math.floorMod(epochMicro, 1_000_000L);

        String timeZone = fieldVector.getTimeZone();
        ZoneId zoneId =
                timeZone == null || timeZone.isEmpty()
                        ? ZoneId.systemDefault()
                        : ZoneId.of(timeZone);

        return Instant.ofEpochSecond(epochSecond, microOfSecond * 1_000L)
                .atZone(zoneId)
                .toLocalDateTime();
    }

    @Override
    public boolean support(Types.MinorType type) {
        return Types.MinorType.TIMESTAMPMICROTZ == type;
    }
}
