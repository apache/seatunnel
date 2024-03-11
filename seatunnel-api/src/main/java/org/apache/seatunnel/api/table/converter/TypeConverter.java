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

package org.apache.seatunnel.api.table.converter;

import org.apache.seatunnel.api.table.catalog.Column;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Type converter to transfer to/from external system types.
 *
 * @param <T>
 */
public interface TypeConverter<T> extends Serializable {

    String identifier();

    /**
     * Convert an external system's type definition to {@link Column}.
     *
     * @param typeDefine type define
     * @return column
     */
    Column convert(T typeDefine);

    default List<Column> convert(List<T> typeDefines) {
        return typeDefines.stream().map(this::convert).collect(Collectors.toList());
    }

    /**
     * Convert {@link Column} to an external system's type definition.
     *
     * @param column
     * @return
     */
    T reconvert(Column column);

    default List<T> reconvert(List<Column> columns) {
        return columns.stream().map(this::reconvert).collect(Collectors.toList());
    }
}
