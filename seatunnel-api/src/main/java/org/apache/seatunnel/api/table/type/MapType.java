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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class MapType<K, V> implements CompositeType<Map<K, V>> {

    private static final List<SqlType> SUPPORTED_KEY_TYPES =
            Arrays.asList(
                    SqlType.NULL,
                    SqlType.BOOLEAN,
                    SqlType.TINYINT,
                    SqlType.SMALLINT,
                    SqlType.INT,
                    SqlType.BIGINT,
                    SqlType.DATE,
                    SqlType.TIME,
                    SqlType.TIMESTAMP,
                    SqlType.TIMESTAMP_TZ,
                    SqlType.FLOAT,
                    SqlType.DOUBLE,
                    SqlType.STRING,
                    SqlType.DECIMAL);

    private final SeaTunnelDataType<K> keyType;
    private final SeaTunnelDataType<V> valueType;

    public MapType(SeaTunnelDataType<K> keyType, SeaTunnelDataType<V> valueType) {
        checkNotNull(keyType, "The key type is required.");
        checkNotNull(valueType, "The value type is required.");
        checkArgument(
                SUPPORTED_KEY_TYPES.contains(keyType.getSqlType()),
                "Unsupported key types: %s",
                keyType);
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public SeaTunnelDataType<K> getKeyType() {
        return keyType;
    }

    public SeaTunnelDataType<V> getValueType() {
        return valueType;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Map<K, V>> getTypeClass() {
        return (Class<Map<K, V>>) (Class<?>) Map.class;
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.MAP;
    }

    @Override
    public List<SeaTunnelDataType<?>> getChildren() {
        return Lists.newArrayList(this.keyType, this.valueType);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof MapType)) {
            return false;
        }
        MapType<?, ?> that = (MapType<?, ?>) obj;
        return Objects.equals(keyType, that.keyType) && Objects.equals(valueType, that.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyType, valueType);
    }

    @Override
    public String toString() {
        return String.format("Map<%s, %s>", keyType, valueType);
    }
}
