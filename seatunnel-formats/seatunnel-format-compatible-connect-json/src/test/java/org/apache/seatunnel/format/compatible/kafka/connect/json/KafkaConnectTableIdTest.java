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

package org.apache.seatunnel.format.compatible.kafka.connect.json;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Pins the tableId of collected rows to the plain {@code database.schema.table} path. */
class KafkaConnectTableIdTest {

    private static final String TOPIC = "seatunnel_topic";
    private static final String TABLE_ID = "connect_database.connect_schema.connect_table";

    private static final Schema ROW_SCHEMA =
            SchemaBuilder.struct()
                    .name("KafkaConnectRow")
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .build();

    private static SeaTunnelRowType connectRowType() {
        return new SeaTunnelRowType(
                new String[] {"id", "name"}, new SeaTunnelDataType<?>[] {INT_TYPE, STRING_TYPE});
    }

    private static CatalogTable catalogTable(SeaTunnelRowType rowType) {
        return CatalogTableUtil.getCatalogTable(
                "connect_catalog", "connect_database", "connect_schema", "connect_table", rowType);
    }

    private static ConsumerRecord<byte[], byte[]> record(byte[] value) {
        return new ConsumerRecord<>(TOPIC, 2, 17L, null, value);
    }

    /** Serializes a value the way a Kafka Connect producer with schemas enabled would. */
    private static byte[] envelopeBytes(Schema schema, Object value) {
        JsonConverter converter = new JsonConverter();
        converter.configure(
                Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true), false);
        return converter.fromConnectData(TOPIC, schema, value);
    }

    private static Collector<SeaTunnelRow> collectingInto(List<SeaTunnelRow> rows) {
        return new Collector<SeaTunnelRow>() {
            @Override
            public void collect(SeaTunnelRow record) {
                rows.add(record);
            }

            @Override
            public Object getCheckpointLock() {
                return this;
            }
        };
    }

    /** Anchors a collected row to the plain table path, the literal table id and its fields. */
    private static void assertConnectRow(
            SeaTunnelRow row, CatalogTable catalogTable, int id, String name) {
        assertEquals(catalogTable.getTablePath().toString(), row.getTableId());
        assertEquals(TABLE_ID, row.getTableId());
        assertEquals(RowKind.INSERT, row.getRowKind());
        assertEquals(id, row.getField(0));
        assertEquals(name, row.getField(1));
    }

    /** The native wrapper maps record metadata only; the map carries no {@code topic} key. */
    @Test
    void testNativeStampsPlainTablePathAsTableId() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"partition", "offset"},
                        new SeaTunnelDataType<?>[] {INT_TYPE, LONG_TYPE});
        CatalogTable catalogTable = catalogTable(rowType);
        NativeKafkaConnectDeserializationSchema schema =
                new NativeKafkaConnectDeserializationSchema(
                        catalogTable, false, false, false, false);
        List<SeaTunnelRow> rows = new ArrayList<>();
        schema.deserialize(record(new byte[] {1, 2, 3}), collectingInto(rows));

        assertEquals(1, rows.size());
        SeaTunnelRow row = rows.get(0);
        assertEquals(catalogTable.getTablePath().toString(), row.getTableId());
        assertEquals(TABLE_ID, row.getTableId());
        assertEquals(2, row.getField(0));
        assertEquals(17L, row.getField(1));
        assertEquals(RowKind.INSERT, row.getRowKind());
    }

    /** A Connect object payload takes the non-array stamping branch. */
    @Test
    void testCompatibleStampsPlainTablePathForObjectPayload() throws Exception {
        CatalogTable catalogTable = catalogTable(connectRowType());
        CompatibleKafkaConnectDeserializationSchema schema =
                new CompatibleKafkaConnectDeserializationSchema(
                        catalogTable, false, true, false, false);
        Struct value = new Struct(ROW_SCHEMA).put("id", 7).put("name", "seatunnel");
        List<SeaTunnelRow> rows = new ArrayList<>();
        schema.deserialize(record(envelopeBytes(ROW_SCHEMA, value)), collectingInto(rows));

        assertEquals(1, rows.size());
        assertConnectRow(rows.get(0), catalogTable, 7, "seatunnel");
    }

    /** An array payload takes the array branch, which stamps every collected row. */
    @Test
    void testCompatibleStampsPlainTablePathForArrayPayload() throws Exception {
        CatalogTable catalogTable = catalogTable(connectRowType());
        CompatibleKafkaConnectDeserializationSchema schema =
                new CompatibleKafkaConnectDeserializationSchema(
                        catalogTable, false, true, false, false);
        List<Struct> values = new ArrayList<>();
        values.add(new Struct(ROW_SCHEMA).put("id", 1).put("name", "first"));
        values.add(new Struct(ROW_SCHEMA).put("id", 2).put("name", "second"));
        List<SeaTunnelRow> rows = new ArrayList<>();
        schema.deserialize(
                record(envelopeBytes(SchemaBuilder.array(ROW_SCHEMA).build(), values)),
                collectingInto(rows));

        assertEquals(2, rows.size());
        assertConnectRow(rows.get(0), catalogTable, 1, "first");
        assertConnectRow(rows.get(1), catalogTable, 2, "second");
    }

    /** A null consumer record emits nothing; both entry points guard against it. */
    @Test
    void testNullRecordCollectsNothing() throws Exception {
        CatalogTable catalogTable = catalogTable(connectRowType());
        NativeKafkaConnectDeserializationSchema nativeSchema =
                new NativeKafkaConnectDeserializationSchema(
                        catalogTable, false, false, false, false);
        CompatibleKafkaConnectDeserializationSchema compatibleSchema =
                new CompatibleKafkaConnectDeserializationSchema(
                        catalogTable, false, true, false, false);
        List<SeaTunnelRow> rows = new ArrayList<>();
        Collector<SeaTunnelRow> collector = collectingInto(rows);
        ConsumerRecord<byte[], byte[]> missingRecord = null;
        nativeSchema.deserialize(missingRecord, collector);
        compatibleSchema.deserialize(missingRecord, collector);

        assertEquals(0, rows.size());
    }
}
