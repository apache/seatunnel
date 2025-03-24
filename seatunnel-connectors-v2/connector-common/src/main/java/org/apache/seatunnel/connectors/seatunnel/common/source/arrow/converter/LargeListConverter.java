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

import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.LargeListVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.Types;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LargeListConverter implements Converter<LargeListVector> {
    @Override
    public Object convert(int rowIndex, LargeListVector fieldVector) {
        return fieldVector.isNull(rowIndex) ? null : fieldVector.getObject(rowIndex);
    }

    @Override
    public Object convert(
            int rowIndex, LargeListVector fieldVector, Map<String, Function> genericsConverters) {
        if (fieldVector.isNull(rowIndex)) {
            return null;
        }
        if (fieldVector.isEmpty(rowIndex)) {
            return Collections.emptyList();
        }
        List<?> listData = fieldVector.getObject(rowIndex);
        Function converter = genericsConverters.get(ARRAY_KEY);
        return listData.stream()
                .map(
                        item -> {
                            if (item instanceof LocalDateTime) {
                                LocalDateTime localDateTime =
                                        ((LocalDateTime) item)
                                                .atZone(ZoneOffset.UTC)
                                                .withZoneSameInstant(ZoneId.systemDefault())
                                                .toLocalDateTime();
                                return converter.apply(localDateTime);
                            } else {
                                return converter.apply(item);
                            }
                        })
                .collect(Collectors.toList());
    }

    @Override
    public boolean support(Types.MinorType type) {
        return Types.MinorType.LARGELIST == type;
    }
}
