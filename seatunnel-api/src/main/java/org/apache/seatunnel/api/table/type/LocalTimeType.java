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
import java.time.OffsetDateTime;
import java.time.temporal.Temporal;
import java.util.Objects;

public class LocalTimeType<T extends Temporal> implements SeaTunnelDataType<T> {
    private static final long serialVersionUID = 2L;

    public static final LocalTimeType<LocalDate> LOCAL_DATE_TYPE =
            new LocalTimeType<>(LocalDate.class, SqlType.DATE);
    public static final LocalTimeType<LocalTime> LOCAL_TIME_TYPE =
            new LocalTimeType<>(LocalTime.class, SqlType.TIME);
    public static final LocalTimeType<LocalDateTime> LOCAL_DATE_TIME_TYPE =
            new LocalTimeType<>(LocalDateTime.class, SqlType.TIMESTAMP);
    public static final LocalTimeType<OffsetDateTime> OFFSET_DATE_TIME_TYPE =
            new LocalTimeType<>(OffsetDateTime.class, SqlType.TIMESTAMP_TZ);

    private final Class<T> typeClass;
    private final SqlType sqlType;

    private LocalTimeType(Class<T> typeClass, SqlType sqlType) {
        this.typeClass = typeClass;
        this.sqlType = sqlType;
    }

    @Override
    public Class<T> getTypeClass() {
        return typeClass;
    }

    @Override
    public SqlType getSqlType() {
        return this.sqlType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeClass);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof LocalTimeType)) {
            return false;
        }
        LocalTimeType<?> that = (LocalTimeType<?>) obj;
        return Objects.equals(typeClass, that.typeClass);
    }

    @Override
    public String toString() {
        return sqlType.toString();
    }
}
