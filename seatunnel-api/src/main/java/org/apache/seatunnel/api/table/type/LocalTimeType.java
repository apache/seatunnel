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

package org.apache.seatunnel.api.table.type;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;

public class LocalTimeType<T> implements SeaTunnelDataType<T> {

    private final Class<T> physicalTypeClass;

    public static final LocalTimeType<LocalDateTime> LOCAL_DATE_TIME =
            new LocalTimeType<>(LocalDateTime.class);

    public static final LocalTimeType<LocalDate> LOCAL_DATE = new LocalTimeType<>(LocalDate.class);

    public static final LocalTimeType<LocalTime> LOCAL_TIME = new LocalTimeType<>(LocalTime.class);

    public LocalTimeType(Class<T> physicalTypeClass) {
        if (physicalTypeClass == null) {
            throw new IllegalArgumentException("physicalTypeClass cannot be null");
        }
        this.physicalTypeClass = physicalTypeClass;
    }

    public Class<T> getPhysicalTypeClass() {
        return this.physicalTypeClass;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LocalTimeType<?> that = (LocalTimeType<?>) o;
        return Objects.equals(physicalTypeClass, that.physicalTypeClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(physicalTypeClass);
    }

    @Override
    public String toString() {
        return "LocalTimeType{" +
                "physicalTypeClass=" + physicalTypeClass +
                '}';
    }
}
