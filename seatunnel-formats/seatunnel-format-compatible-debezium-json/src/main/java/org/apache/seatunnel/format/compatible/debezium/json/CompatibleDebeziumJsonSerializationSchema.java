/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.compatible.debezium.json;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.RequiredArgsConstructor;

import static org.apache.seatunnel.format.compatible.debezium.json.CompatibleDebeziumJsonDeserializationSchema.FIELD_KEY;
import static org.apache.seatunnel.format.compatible.debezium.json.CompatibleDebeziumJsonDeserializationSchema.FIELD_VALUE;

@RequiredArgsConstructor
public class CompatibleDebeziumJsonSerializationSchema implements SerializationSchema {

    private final boolean isKey;
    private final int index;

    public CompatibleDebeziumJsonSerializationSchema(SeaTunnelRowType rowType, boolean isKey) {
        this.isKey = isKey;
        this.index = rowType.indexOf(isKey ? FIELD_KEY : FIELD_VALUE);
    }

    @Override
    public byte[] serialize(SeaTunnelRow row) {
        String field = (String) row.getField(index);
        if (isKey && field == null) {
            return null;
        }
        return field.getBytes();
    }
}
