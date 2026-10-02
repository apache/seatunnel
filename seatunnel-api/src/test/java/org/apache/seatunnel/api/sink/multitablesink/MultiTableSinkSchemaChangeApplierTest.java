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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.options.MultiTableCommonOptions;
import org.apache.seatunnel.api.options.MultiTableFailurePolicy;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.sink.SchemaChangeApplier;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportCoordinatedSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultiTableSinkSchemaChangeApplierTest {

    @Test
    void logicalTableMatchTakesPrecedenceOverAnotherSinksPhysicalMatch() throws IOException {
        TablePath requestedLogicalTable = TablePath.of("source", "users");
        TablePath otherLogicalTable = TablePath.of("source", "orders");
        RecordingCoordinatedSink logicalMatch =
                new RecordingCoordinatedSink(TablePath.of("target", "users"));
        RecordingCoordinatedSink physicalMatch =
                new RecordingCoordinatedSink(requestedLogicalTable);
        Map<TablePath, SeaTunnelSink> sinks = new LinkedHashMap<>();
        sinks.put(otherLogicalTable, physicalMatch);
        sinks.put(requestedLogicalTable, logicalMatch);

        SchemaChangeApplier selected =
                createMultiTableSink(sinks).createSchemaChangeApplier(requestedLogicalTable);

        assertSame(logicalMatch.applier, selected);
        assertEquals(TablePath.of("target", "users"), logicalMatch.requestedPhysicalTable);
        assertEquals(0, physicalMatch.creationCount);
    }

    @Test
    void uniquePhysicalTableMatchCreatesThatSinksApplier() throws IOException {
        TablePath logicalTable = TablePath.of("source", "users");
        TablePath physicalTable = TablePath.of("target", "users");
        RecordingCoordinatedSink sink = new RecordingCoordinatedSink(physicalTable);

        SchemaChangeApplier selected =
                createMultiTableSink(Collections.singletonMap(logicalTable, sink))
                        .createSchemaChangeApplier(physicalTable);

        assertSame(sink.applier, selected);
        assertEquals(physicalTable, sink.requestedPhysicalTable);
        assertEquals(1, sink.creationCount);
    }

    @Test
    void sharedPhysicalTableMatchIsRejectedAsAmbiguous() {
        TablePath firstLogicalTable = TablePath.of("source", "users_kr");
        TablePath secondLogicalTable = TablePath.of("source", "users_us");
        TablePath sharedPhysicalTable = TablePath.of("target", "users");
        RecordingCoordinatedSink firstSink = new RecordingCoordinatedSink(sharedPhysicalTable);
        RecordingCoordinatedSink secondSink = new RecordingCoordinatedSink(sharedPhysicalTable);
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(firstLogicalTable, firstSink);
        sinks.put(secondLogicalTable, secondSink);

        IOException error =
                assertThrows(
                        IOException.class,
                        () ->
                                createMultiTableSink(sinks)
                                        .createSchemaChangeApplier(sharedPhysicalTable));

        assertTrue(error.getMessage().contains("Ambiguous physical sink table"));
        assertTrue(error.getMessage().contains(firstLogicalTable.getFullName()));
        assertTrue(error.getMessage().contains(secondLogicalTable.getFullName()));
        assertEquals(0, firstSink.creationCount);
        assertEquals(0, secondSink.creationCount);
    }

    private MultiTableSink createMultiTableSink(Map<TablePath, SeaTunnelSink> sinks) {
        Map<String, Object> options = new HashMap<>();
        options.put(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA.key(), 1);
        options.put(
                MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY.key(),
                MultiTableFailurePolicy.FAIL_FAST.name());
        options.put(EnvCommonOptions.JOB_RETRY_TIMES.key(), 0);
        options.put(EnvCommonOptions.JOB_RETRY_INTERVAL_SECONDS.key(), 0);
        return new MultiTableSink(
                new MultiTableFactoryContext(
                        ReadonlyConfig.fromMap(options),
                        Thread.currentThread().getContextClassLoader(),
                        sinks));
    }

    private static class RecordingCoordinatedSink
            implements SeaTunnelSink<SeaTunnelRow, String, String, String>,
                    SupportCoordinatedSchemaEvolutionSink {

        private final TablePath physicalTable;
        private final SchemaChangeApplier applier = event -> {};
        private int creationCount;
        private TablePath requestedPhysicalTable;

        private RecordingCoordinatedSink(TablePath physicalTable) {
            this.physicalTable = physicalTable;
        }

        @Override
        public SinkWriter<SeaTunnelRow, String, String> createWriter(SinkWriter.Context context) {
            throw new UnsupportedOperationException("Writer creation is not used by this test");
        }

        @Override
        public Optional<CatalogTable> getWriteCatalogTable() {
            return Optional.of(
                    CatalogTable.of(
                            TableIdentifier.of("test", physicalTable),
                            TableSchema.builder().build(),
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            ""));
        }

        @Override
        public List<SchemaChangeType> supports() {
            return Collections.singletonList(SchemaChangeType.ADD_COLUMN);
        }

        @Override
        public SchemaChangeApplier createSchemaChangeApplier(TablePath sinkTablePath) {
            creationCount++;
            requestedPhysicalTable = sinkTablePath;
            return applier;
        }

        @Override
        public String getPluginName() {
            return "recording-coordinated-sink";
        }
    }
}
