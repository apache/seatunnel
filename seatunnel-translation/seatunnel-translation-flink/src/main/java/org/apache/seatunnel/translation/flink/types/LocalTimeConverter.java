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

package org.apache.seatunnel.translation.flink.types;

import org.apache.seatunnel.api.table.type.LocalTimeType;

import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class LocalTimeConverter<T1> implements FlinkTypeConverter<LocalTimeType<T1>, TypeInformation<T1>> {

    public static final LocalTimeConverter<LocalDate> LOCAL_DATE_CONVERTER =
            new LocalTimeConverter<>(LocalTimeType.LOCAL_DATE, LocalTimeTypeInfo.LOCAL_DATE);

    public static final LocalTimeConverter<LocalTime> LOCAL_TIME_CONVERTER =
            new LocalTimeConverter<>(LocalTimeType.LOCAL_TIME, LocalTimeTypeInfo.LOCAL_TIME);

    public static final LocalTimeConverter<LocalDateTime> LOCAL_DATE_TIME_CONVERTER =
            new LocalTimeConverter<>(LocalTimeType.LOCAL_DATE_TIME, LocalTimeTypeInfo.LOCAL_DATE_TIME);

    private final LocalTimeType<T1> seaTunnelDataType;
    private final TypeInformation<T1> flinkTypeInformation;

    private LocalTimeConverter(LocalTimeType<T1> seaTunnelDataType, TypeInformation<T1> flinkTypeInformation) {
        this.seaTunnelDataType = seaTunnelDataType;
        this.flinkTypeInformation = flinkTypeInformation;
    }

    @Override
    public TypeInformation<T1> convert(LocalTimeType<T1> seaTunnelDataType) {
        return flinkTypeInformation;
    }

    @Override
    public LocalTimeType<T1> reconvert(TypeInformation<T1> typeInformation) {
        return seaTunnelDataType;
    }
}
