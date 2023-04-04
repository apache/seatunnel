/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.api.table.catalog;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Representation of a metadata column. */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MetadataColumn extends Column {

    private final String metadataKey;

    protected MetadataColumn(
            String name,
            SeaTunnelDataType<?> dataType,
            Integer columnLength,
            String metadataKey,
            boolean nullable,
            Object defaultValue,
            String comment) {
        super(name, dataType, columnLength, nullable, defaultValue, comment);
        this.metadataKey = metadataKey;
    }

    public static MetadataColumn of(
            String name,
            SeaTunnelDataType<?> dataType,
            Integer columnLength,
            String metadataKey,
            boolean nullable,
            Object defaultValue,
            String comment) {
        return new MetadataColumn(
                name, dataType, columnLength, metadataKey, nullable, defaultValue, comment);
    }

    @Override
    public boolean isPhysical() {
        return false;
    }

    @Override
    public Column copy(SeaTunnelDataType<?> newType) {
        return MetadataColumn.of(
                name, newType, columnLength, metadataKey, nullable, defaultValue, comment);
    }

    @Override
    public Column copy() {
        return MetadataColumn.of(
                name, dataType, columnLength, metadataKey, nullable, defaultValue, comment);
    }
}
