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

package org.apache.seatunnel.format.protobuf;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Covers the format entry point the Kafka source uses when {@code strip_schema_registry_header} is
 * left at its default, which is this schema rather than {@link
 * SchemaRegistryAwareProtobufDeserializationSchema}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ProtobufDeserializationSchemaTest {

    private static final String PROTO_CONTENT =
            "syntax = \"proto3\";\n"
                    + "\n"
                    + "package org.apache.seatunnel.format.protobuf;\n"
                    + "\n"
                    + "option java_outer_classname = \"TombstoneProto\";\n"
                    + "\n"
                    + "message TestMessage {\n"
                    + "  int32 id = 1;\n"
                    + "  string name = 2;\n"
                    + "}";

    private static final String MESSAGE_NAME = "TestMessage";

    private ProtobufDeserializationSchema schema;

    @BeforeAll
    void setUpSchema() {
        schema = new ProtobufDeserializationSchema(createCatalogTable());
    }

    private CatalogTable createCatalogTable() {
        Map<String, String> options = new HashMap<>();
        options.put("protobuf_schema", PROTO_CONTENT);
        options.put("protobuf_message_name", MESSAGE_NAME);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("tombstone_table", rowType);
        catalogTable.getOptions().putAll(options);
        return catalogTable;
    }

    @Test
    void deserializeReturnsNoRowForATombstone() throws IOException {
        // A Kafka tombstone delivers a null value. Before the guard this raised a
        // NullPointerException from inside DynamicMessage.parseFrom.
        Assertions.assertNull(schema.deserialize((byte[]) null));
    }

    @Test
    void collectorReceivesNothingForATombstone() throws IOException {
        RecordingCollector collector = new RecordingCollector();

        schema.deserialize((byte[]) null, collector);

        Assertions.assertTrue(
                collector.collected.isEmpty(),
                "a tombstone must not produce a row, so nothing may reach the collector");
    }

    @Test
    void anEmptyPayloadRemainsAValidMessageOfDefaults() throws IOException {
        // Protobuf defines a zero length payload as a valid message whose fields all hold
        // their defaults, so it must keep decoding rather than being treated as a tombstone.
        RecordingCollector collector = new RecordingCollector();

        SeaTunnelRow row = schema.deserialize(new byte[0]);
        schema.deserialize(new byte[0], collector);

        Assertions.assertNotNull(row);
        Assertions.assertEquals(0, row.getField(0));
        Assertions.assertEquals("", row.getField(1));
        Assertions.assertEquals(1, collector.collected.size());
    }

    private static class RecordingCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> collected = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            collected.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return this;
        }
    }
}
