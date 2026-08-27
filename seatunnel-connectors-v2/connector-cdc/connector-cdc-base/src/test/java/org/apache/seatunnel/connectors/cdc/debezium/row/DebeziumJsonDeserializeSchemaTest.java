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

package org.apache.seatunnel.connectors.cdc.debezium.row;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DebeziumJsonDeserializeSchemaTest {
    @Test
    void deserializeNonHeartbeatRecord() throws Exception {
        Map<String, String> debeziumConfig = Collections.EMPTY_MAP;
        DebeziumJsonDeserializeSchema schema = new DebeziumJsonDeserializeSchema(debeziumConfig);

        // Create a schema for the record
        SchemaBuilder schemaBuilder =
                SchemaBuilder.struct()
                        .name("test")
                        .field("field", SchemaBuilder.string().optional().build());
        Struct struct = new Struct(schemaBuilder.build()).put("field", "value");
        SourceRecord record =
                new SourceRecord(
                        null,
                        null,
                        "test",
                        schemaBuilder.build(),
                        struct,
                        schemaBuilder.build(),
                        struct);

        Collector<SeaTunnelRow> collector = mock(Collector.class);
        schema.deserialize(record, collector);

        verify(collector, times(1)).collect(any(SeaTunnelRow.class));
    }

    @Test
    void skipHeartbeatRecord() throws Exception {
        Map<String, String> debeziumConfig = Collections.EMPTY_MAP;
        DebeziumJsonDeserializeSchema schema = new DebeziumJsonDeserializeSchema(debeziumConfig);

        // Create a schema for the record
        SchemaBuilder schemaBuilder =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.common.Heartbeat")
                        .field("field", SchemaBuilder.string().optional().build());
        Struct struct = new Struct(schemaBuilder.build()).put("field", "value");
        SourceRecord record =
                new SourceRecord(
                        null,
                        null,
                        "test",
                        schemaBuilder.build(),
                        struct,
                        schemaBuilder.build(),
                        struct);

        Collector<SeaTunnelRow> collector = mock(Collector.class);
        schema.deserialize(record, collector);

        verify(collector, times(0)).collect(any(SeaTunnelRow.class));
    }
}
