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

package org.apache.seatunnel.translation.flink.schema;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TableSchemaKryoSerializationTest {

    @Test
    void testPopulatedSchemaRoundTripRebuildsColumnLookups() throws Exception {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.LONG_TYPE, 20L, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 255L, true, null, null))
                        .build();
        // Warm the transient lookup cache before sending the schema between Flink operators.
        assertEquals(1, schema.indexOf("name"));

        TableSchema restored = roundTrip(schema);

        assertEquals(schema.getColumns(), restored.getColumns());
        assertEquals(0, restored.indexOf("id"));
        assertEquals(1, restored.indexOf("name"));
        assertEquals(-1, restored.indexOf("missing"));
        assertThrows(
                UnsupportedOperationException.class,
                () -> restored.getColumns().add(schema.getColumn("id")));
    }

    @Test
    void testEmptySchemaRoundTrip() throws Exception {
        TableSchema restored = roundTrip(TableSchema.builder().build());

        assertEquals(Collections.emptyList(), restored.getColumns());
        assertEquals(-1, restored.indexOf("missing"));
        assertFalse(restored.contains("missing"));
    }

    private static TableSchema roundTrip(TableSchema schema) throws Exception {
        KryoSerializer<TableSchema> serializer =
                new KryoSerializer<>(TableSchema.class, new ExecutionConfig());
        DataOutputSerializer output = new DataOutputSerializer(128);
        serializer.serialize(schema, output);
        return serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
    }
}
