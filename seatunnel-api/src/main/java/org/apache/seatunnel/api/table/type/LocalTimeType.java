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
import java.time.temporal.Temporal;

public class LocalTimeType<T extends Temporal> implements SeaTunnelDataType<T> {
    private static final long serialVersionUID = 1L;

    public static final LocalTimeType<LocalDate> LOCAL_DATE_TYPE = new LocalTimeType<>(LocalDate.class);
    public static final LocalTimeType<LocalTime> LOCAL_TIME_TYPE = new LocalTimeType<>(LocalTime.class);
    public static final LocalTimeType<LocalDateTime> LOCAL_DATE_TIME_TYPE = new LocalTimeType<>(LocalDateTime.class);

    private final Class<T> typeClass;

    private LocalTimeType(Class<T> typeClass) {
        this.typeClass = typeClass;
    }

    @Override
    public Class<T> getTypeClass() {
        return typeClass;
    }

    @Override
    public int hashCode() {
        return typeClass.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LocalTimeType) {
            LocalTimeType<?> other = (LocalTimeType<?>) obj;
            return typeClass == other.typeClass;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "LocalTimeType{" +
            "typeClass=" + typeClass +
            '}';
    }
}
