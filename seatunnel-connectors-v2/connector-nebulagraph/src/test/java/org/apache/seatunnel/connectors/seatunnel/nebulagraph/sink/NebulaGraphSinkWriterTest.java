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

package org.apache.seatunnel.connectors.seatunnel.nebulagraph.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.client.NebulaGraphClient;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.config.NebulaGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception.NebulaGraphConnectorException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NebulaGraphSinkWriterTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name", "age"},
                    new SeaTunnelDataType<?>[] {
                        BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                    });

    @Test
    void flushesAtBatchBoundaryAndCheckpoint() throws Exception {
        RecordingClient client = new RecordingClient();
        NebulaGraphSinkConfig config = config(Collections.singletonMap("batch_size", 2));
        NebulaGraphSinkWriter writer = new NebulaGraphSinkWriter(config, ROW_TYPE, client);

        writer.write(row("p1", "Alice", 31));
        assertEquals(0, client.statements.size());
        writer.write(row("p2", "Bob", 29));
        assertEquals(1, client.statements.size());

        writer.write(row("p3", "Carol", 42));
        writer.prepareCommit();
        assertEquals(2, client.statements.size());
        assertEquals(42L, client.parameters.get(1).get("value_0_1"));
        writer.close();
    }

    @Test
    void rejectsNullVertexIdsBeforeWriting() throws Exception {
        RecordingClient client = new RecordingClient();
        NebulaGraphSinkWriter writer =
                new NebulaGraphSinkWriter(config(Collections.emptyMap()), ROW_TYPE, client);

        assertThrows(
                NebulaGraphConnectorException.class, () -> writer.write(row(null, "Alice", 31)));
        assertEquals(0, client.statements.size());
        writer.close();
    }

    @Test
    void insertModeRejectsChangelogRows() throws Exception {
        RecordingClient client = new RecordingClient();
        NebulaGraphSinkWriter writer =
                new NebulaGraphSinkWriter(config(Collections.emptyMap()), ROW_TYPE, client);
        SeaTunnelRow update = row("p1", "Alice", 32);
        update.setRowKind(RowKind.UPDATE_AFTER);

        IOException exception = assertThrows(IOException.class, () -> writer.write(update));
        assertTrue(exception.getMessage().contains("UPDATE_AFTER"));
        writer.close();
    }

    @Test
    void updateModeIgnoresBeforeImageAndWritesAfterImage() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("write_mode", "UPDATE");
        options.put("batch_size", 1);
        RecordingClient client = new RecordingClient();
        NebulaGraphSinkWriter writer = new NebulaGraphSinkWriter(config(options), ROW_TYPE, client);
        SeaTunnelRow before = row("p1", "Alice", 31);
        before.setRowKind(RowKind.UPDATE_BEFORE);
        SeaTunnelRow after = row("p1", "Alice", 32);
        after.setRowKind(RowKind.UPDATE_AFTER);

        writer.write(before);
        writer.write(after);

        assertEquals(1, client.statements.size());
        assertTrue(client.statements.get(0).startsWith("UPDATE VERTEX"));
        assertEquals(32L, client.parameters.get(0).get("value_0_1"));
        writer.close();
    }

    @Test
    void failedBatchIsNotRetriedDuringClose() throws Exception {
        RecordingClient client = new RecordingClient();
        client.failure = new IOException("connection reset");
        NebulaGraphSinkWriter writer =
                new NebulaGraphSinkWriter(
                        config(Collections.singletonMap("batch_size", 1)), ROW_TYPE, client);

        assertThrows(IOException.class, () -> writer.write(row("p1", "Alice", 31)));
        writer.close();

        assertEquals(1, client.executeCount);
        assertTrue(client.closed);
    }

    @Test
    void rejectsUnsupportedPropertyTypeBeforeClientUse() {
        SeaTunnelRowType decimalRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "amount"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, new DecimalType(10, 2)});
        RecordingClient client = new RecordingClient();

        assertThrows(
                NebulaGraphConnectorException.class,
                () ->
                        new NebulaGraphSinkWriter(
                                config(Collections.emptyMap()), decimalRowType, client));
        assertEquals(0, client.executeCount);
    }

    private static NebulaGraphSinkConfig config(Map<String, Object> overrides) {
        Map<String, Object> values = new HashMap<>();
        values.put("hosts", Arrays.asList("graphd:9669"));
        values.put("username", "root");
        values.put("password", "nebula");
        values.put("space", "test");
        values.put("tag", "person");
        values.put("vid_field", "id");
        values.putAll(overrides);
        return NebulaGraphSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static SeaTunnelRow row(String id, String name, int age) {
        return new SeaTunnelRow(new Object[] {id, name, age});
    }

    private static final class RecordingClient implements NebulaGraphClient {
        private final List<String> statements = new ArrayList<>();
        private final List<Map<String, Object>> parameters = new ArrayList<>();
        private IOException failure;
        private int executeCount;
        private boolean closed;

        @Override
        public void execute(String statement, Map<String, Object> parameters) throws IOException {
            executeCount++;
            if (failure != null) {
                throw failure;
            }
            statements.add(statement);
            this.parameters.add(new HashMap<>(parameters));
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
