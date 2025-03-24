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

import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.MapVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.Types;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class MapConverter implements Converter<MapVector> {
    @Override
    public Object convert(int rowIndex, MapVector fieldVector) {
        return fieldVector.isNull(rowIndex) ? null : fieldVector.getObject(rowIndex);
    }

    @Override
    public Object convert(
            int rowIndex, MapVector fieldVector, Map<String, Function> genericsConverters) {
        UnionMapReader reader = fieldVector.getReader();
        reader.setPosition(rowIndex);
        Map<Object, Object> mapValue = new HashMap<>();
        Function keyConverter = genericsConverters.get(MAP_KEY);
        Function valueConverter = genericsConverters.get(MAP_VALUE);
        while (reader.next()) {
            Object key = keyConverter.apply(processTimeZone(reader.key().readObject()));
            Object value = valueConverter.apply(processTimeZone(reader.value().readObject()));
            mapValue.put(key, value);
        }
        return mapValue;
    }

    private Object processTimeZone(Object value) {
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value)
                    .atZone(ZoneOffset.UTC)
                    .withZoneSameInstant(ZoneId.systemDefault())
                    .toLocalDateTime();
        } else {
            return value;
        }
    }

    @Override
    public boolean support(Types.MinorType type) {
        return Types.MinorType.MAP == type;
    }
}
