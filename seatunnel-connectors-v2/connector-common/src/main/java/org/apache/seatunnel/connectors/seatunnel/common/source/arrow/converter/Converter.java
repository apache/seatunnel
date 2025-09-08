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

import org.apache.seatunnel.shade.org.apache.arrow.vector.FieldVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.Types;

import java.util.Map;
import java.util.function.Function;

public interface Converter<T extends FieldVector> {

    String ARRAY_KEY = "ARRAY";
    String MAP_KEY = "KEY";
    String MAP_VALUE = "VALUE";

    Object convert(int rowIndex, T fieldVector);

    default Object convert(int rowIndex, T fieldVector, Map<String, Function> genericsConverters) {
        throw new UnsupportedOperationException("Unsupported generics convert");
    }

    boolean support(Types.MinorType type);
}
