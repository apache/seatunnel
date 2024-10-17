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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCompatibleDebeziumJsonSerializationSchema {

    @Test
    public void testDebeziumSerializeKeyIsNull() {
        SeaTunnelRowType rowType =
                CompatibleDebeziumJsonDeserializationSchema.DEBEZIUM_DATA_ROW_TYPE;
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"test_topic", null, "value"});

        CompatibleDebeziumJsonSerializationSchema serializationSchema =
                new CompatibleDebeziumJsonSerializationSchema(rowType, true);
        Assertions.assertNull(serializationSchema.serialize(row));
    }
}
