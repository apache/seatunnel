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

package org.apache.seatunnel.connectors.seatunnel.firebase.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class FirebaseDataDeserializerTest {
    private DeserializationSchema<SeaTunnelRow> deserializer;

    @BeforeEach
    void setUp() {
        // Build schema matching {"name": String, "age": Int, "active": Boolean}
        String[] fieldNames = new String[] {"name", "age", "active"};
        SeaTunnelDataType<?>[] fieldTypes =
                new SeaTunnelDataType<?>[] {
                    BasicType.STRING_TYPE, BasicType.INT_TYPE, BasicType.BOOLEAN_TYPE
                };
        SeaTunnelRowType rowType = new SeaTunnelRowType(fieldNames, fieldTypes);

        // Standard SeaTunnel JSON Deserializer setup used inside FirebaseSourceReader
        this.deserializer = new JsonDeserializationSchema(false, false, rowType);
    }

    @Test
    void testDeserializeSingleRecordJson() throws Exception {
        String jsonPayload = "{\"name\": \"john doe\", \"age\": 22, \"active\": true}";

        SeaTunnelRow row = deserializer.deserialize(jsonPayload.getBytes(StandardCharsets.UTF_8));

        assertNotNull(row);
        assertEquals("john doe", row.getField(0));
        assertEquals(22, row.getField(1));
        assertEquals(true, row.getField(2));
    }

    @Test
    void testDeserializeJsonWithMissingFields() throws Exception {
        // Sparse JSON record (active missing)
        String jsonPayload = "{\"name\": \"my name\", \"age\": 30}";

        SeaTunnelRow row = deserializer.deserialize(jsonPayload.getBytes(StandardCharsets.UTF_8));

        assertNotNull(row);
        assertEquals("my name", row.getField(0));
        assertEquals(30, row.getField(1));
        assertEquals(null, row.getField(2)); // Missing fields map gracefully to null
    }

    @Test
    void testDeserializeNonAsciiJsonRow() throws Exception {
        String jsonPayload = "{\"name\":\"اسم عربي\" , \"age\": 44,\"active\":true}";
        SeaTunnelRow row = deserializer.deserialize(jsonPayload.getBytes(StandardCharsets.UTF_8));

        assertNotNull(row);
        assertEquals("اسم عربي", row.getField(0));
    }
}
