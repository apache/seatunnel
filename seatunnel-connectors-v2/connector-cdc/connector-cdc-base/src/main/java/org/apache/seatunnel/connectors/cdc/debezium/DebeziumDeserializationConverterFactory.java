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

package org.apache.seatunnel.connectors.cdc.debezium;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.Optional;

/**
 * Factory to create {@link DebeziumDeserializationConverter} according to {@link SeaTunnelDataType}. It's
 * usually used to create a user-defined {@link DebeziumDeserializationConverter} which has a higher
 * resolve order than default converter.
 */
public interface DebeziumDeserializationConverterFactory extends Serializable {

    /**
     * A user-defined converter factory which always fallback to default converters.
     */
    DebeziumDeserializationConverterFactory DEFAULT =
        (logicalType, serverTimeZone) -> Optional.empty();

    /**
     * Returns an optional {@link DebeziumDeserializationConverter}. Returns {@link Optional#empty()}
     * if fallback to default converter.
     *
     * @param type the SeaTunnel datatype to be converted from objects of Debezium
     * @param serverTimeZone TimeZone used to convert data with timestamp type
     */
    Optional<DebeziumDeserializationConverter> createUserDefinedConverter(
        SeaTunnelDataType<?> type, ZoneId serverTimeZone);
}
