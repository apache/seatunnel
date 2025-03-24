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

import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.StructVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.Types;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class StructConverter implements Converter<StructVector> {
    @Override
    public Object convert(int rowIndex, StructVector fieldVector) {
        return fieldVector.isNull(rowIndex) ? null : fieldVector.getObject(rowIndex);
    }

    @Override
    public Object convert(
            int rowIndex, StructVector fieldVector, Map<String, Function> genericsConverters) {
        Map<String, ?> valueMap = fieldVector.getObject(rowIndex);
        return valueMap.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                e -> {
                                    Optional<Function> optional =
                                            Optional.ofNullable(genericsConverters.get(e.getKey()));
                                    if (optional.isPresent()) {
                                        return optional.get().apply(e.getValue());
                                    } else {
                                        log.warn("No converter found for key:{}", e.getKey());
                                        return e.getValue();
                                    }
                                }));
    }

    @Override
    public boolean support(Types.MinorType type) {
        return Types.MinorType.STRUCT == type;
    }
}
