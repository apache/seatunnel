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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.io.Serializable;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Data converter to transfer to/from external system data type.
 *
 * @param <T>
 */
public interface DataConverter<T> extends Serializable {

    String identifier();

    /**
     * Convert an external system's data type to {@link SeaTunnelDataType#getTypeClass()}.
     *
     * @param typeDefine
     * @param value
     * @return
     */
    Object convert(SeaTunnelDataType typeDefine, Object value);

    default Object convert(Column columnDefine, Object value) {
        return convert(columnDefine.getDataType(), value);
    }

    default Object convert(T typeDefine, Column columnDefine, Object value) {
        return convert(columnDefine, value);
    }

    default Object[] convert(T[] typeDefine, Column[] columnDefine, Object[] value) {
        for (int i = 0; i < value.length; i++) {
            value[i] =
                    convert(typeDefine != null ? typeDefine[i] : null, columnDefine[i], value[i]);
        }
        return value;
    }

    default Object[] convert(Column[] columnDefine, Function<Column[], Object[]> valueApply) {
        Object[] fields = valueApply.apply(columnDefine);
        if (fields.length != columnDefine.length) {
            throw new IllegalStateException("columnDefine size not match");
        }

        for (int i = 0; i < fields.length; i++) {
            fields[i] = convert(columnDefine[i], fields[i]);
        }
        return fields;
    }

    default Object[] convert(
            T[] typeDefine, Column[] columnDefine, BiFunction<T[], Column[], Object[]> valueApply) {
        boolean hasTypeDefine = typeDefine != null;
        if (hasTypeDefine && typeDefine.length != columnDefine.length) {
            throw new IllegalStateException("typeDefine size not match");
        }

        Object[] fields = valueApply.apply(typeDefine, columnDefine);
        if (fields.length != columnDefine.length) {
            throw new IllegalStateException("columnDefine size not match");
        }

        for (int i = 0; i < fields.length; i++) {
            fields[i] = convert(hasTypeDefine ? typeDefine[i] : null, columnDefine[i], fields[i]);
        }
        return fields;
    }

    default Object reconvert(T typeDefine, Column columnDefine, Object value) {
        return reconvert(typeDefine, value);
    }

    /**
     * Convert object to an external system's data type.
     *
     * @param typeDefine
     * @param value
     * @return
     */
    default Object reconvert(T typeDefine, Object value) {
        throw new UnsupportedOperationException("reconvert not support");
    }

    default Object reconvert(Column columnDefine, Object value) {
        return reconvert(columnDefine.getDataType(), value);
    }

    /**
     * Convert {@link SeaTunnelDataType#getTypeClass()} to an external system's data type.
     *
     * @param typeDefine
     * @param value
     * @return
     */
    default Object reconvert(SeaTunnelDataType typeDefine, Object value) {
        throw new UnsupportedOperationException("reconvert not support");
    }
}
