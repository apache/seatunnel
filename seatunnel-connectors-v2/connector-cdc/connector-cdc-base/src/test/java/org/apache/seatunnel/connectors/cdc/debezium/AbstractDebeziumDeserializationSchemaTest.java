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

package org.apache.seatunnel.connectors.cdc.debezium;

import org.apache.seatunnel.api.table.catalog.CatalogTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class AbstractDebeziumDeserializationSchemaTest {

    @Test
    void restoreCheckpointHistoryTableChangesReplacesStaleHistory() {
        TestingDebeziumDeserializationSchema schema = new TestingDebeziumDeserializationSchema();
        TableId staleTable = new TableId("catalog", "database", "stale_table");
        TableId restoredTable = new TableId("catalog", "database", "restored_table");

        schema.restoreCheckpointHistoryTableChanges(
                Collections.singletonMap(staleTable, new byte[] {1}));

        Map<TableId, byte[]> checkpointHistory = new HashMap<>();
        checkpointHistory.put(restoredTable, new byte[] {2});
        schema.restoreCheckpointHistoryTableChanges(checkpointHistory);

        Map<TableId, byte[]> restoredHistory = schema.getHistoryTableChanges();
        Assertions.assertFalse(restoredHistory.containsKey(staleTable));
        Assertions.assertArrayEquals(new byte[] {2}, restoredHistory.get(restoredTable));
    }

    @Test
    void restoreCheckpointHistoryTableChangesIgnoresEmptyLegacyState() {
        TestingDebeziumDeserializationSchema schema = new TestingDebeziumDeserializationSchema();
        TableId tableId = new TableId("catalog", "database", "table");
        schema.restoreCheckpointHistoryTableChanges(
                Collections.singletonMap(tableId, new byte[] {1}));

        schema.restoreCheckpointHistoryTableChanges(Collections.emptyMap());

        Assertions.assertArrayEquals(new byte[] {1}, schema.getHistoryTableChanges().get(tableId));
    }

    @Test
    void serializationRoundTripDoesNotSerializeTableIds() throws Exception {
        TestingDebeziumDeserializationSchema schema = new TestingDebeziumDeserializationSchema();
        TableId tableId =
                new TableId(
                        null,
                        "schema.with.dots",
                        "table.with.dots",
                        ignored -> "mapped-identifier");
        schema.restoreCheckpointHistoryTableChanges(
                Collections.singletonMap(tableId, new byte[] {1}));

        TestingDebeziumDeserializationSchema restoredSchema = roundTrip(schema);

        Map<TableId, byte[]> history = restoredSchema.getHistoryTableChanges();
        TableId restoredTableId = history.keySet().iterator().next();
        Assertions.assertNull(restoredTableId.catalog());
        Assertions.assertEquals(tableId.schema(), restoredTableId.schema());
        Assertions.assertEquals(tableId.table(), restoredTableId.table());
        Assertions.assertEquals(tableId.identifier(), restoredTableId.identifier());
        Assertions.assertArrayEquals(new byte[] {1}, history.get(tableId));
    }

    @Test
    void legacyEmptyHistoryCanBePopulatedAndSerializedAgain() throws Exception {
        TestingDebeziumDeserializationSchema schema = new TestingDebeziumDeserializationSchema();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output =
                new TableIdRejectingObjectOutputStream(bytes) {
                    @Override
                    protected Object replaceObject(Object object) throws IOException {
                        // Before the fix this field was serialized as a plain HashMap.
                        return object == schema.tableChangesStructMap
                                ? new HashMap<>()
                                : super.replaceObject(object);
                    }
                }) {
            output.writeObject(schema);
        }

        TestingDebeziumDeserializationSchema legacy;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            legacy = (TestingDebeziumDeserializationSchema) input.readObject();
        }
        TableId table = new TableId("inventory", null, "orders");
        legacy.restoreCheckpointHistoryTableChanges(
                Collections.singletonMap(table, new byte[] {3}));
        Assertions.assertArrayEquals(
                new byte[] {3}, roundTrip(legacy).getHistoryTableChanges().get(table));
    }

    private static TestingDebeziumDeserializationSchema roundTrip(
            TestingDebeziumDeserializationSchema schema) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutput = new TableIdRejectingObjectOutputStream(output)) {
            objectOutput.writeObject(schema);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(output.toByteArray()))) {
            return (TestingDebeziumDeserializationSchema) input.readObject();
        }
    }

    /** Fails the test if Java serialization attempts to persist a Debezium TableId instance. */
    private static class TableIdRejectingObjectOutputStream extends ObjectOutputStream {

        private TableIdRejectingObjectOutputStream(ByteArrayOutputStream output)
                throws IOException {
            super(output);
            enableReplaceObject(true);
        }

        @Override
        protected Object replaceObject(Object object) throws IOException {
            if (object instanceof TableId) {
                throw new IOException("Debezium TableId must not be serialized directly");
            }
            return object;
        }
    }

    private static class TestingDebeziumDeserializationSchema
            extends AbstractDebeziumDeserializationSchema<Object> {

        private TestingDebeziumDeserializationSchema() {
            super(Collections.emptyMap());
        }

        @Override
        public List<CatalogTable> getProducedType() {
            return Collections.emptyList();
        }
    }
}
