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

package org.apache.seatunnel.connectors.seatunnel.iceberg.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergCatalogLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IcebergTimestampDeleteTest {
    @Test
    void deleteOffsetTimestampWindowPreservesOtherRecords() throws Exception {
        Map<String, Object> catalogProperties = new HashMap<>();
        catalogProperties.put("type", "hadoop");
        Map<String, Object> config = new HashMap<>();
        config.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "test");
        config.put(IcebergCommonOptions.CATALOG_PROPS.key(), catalogProperties);
        // Keep real Iceberg commits and Parquet I/O without Hadoop's native filesystem tools.
        try (InMemoryCatalog setup = new InMemoryCatalog();
                MockedConstruction<IcebergCatalogLoader> ignored =
                        Mockito.mockConstruction(
                                IcebergCatalogLoader.class,
                                (loader, context) ->
                                        Mockito.when(loader.loadCatalog()).thenReturn(setup))) {
            setup.initialize("test", Collections.emptyMap());
            setup.createNamespace(Namespace.of("test"));
            IcebergCatalog catalog = new IcebergCatalog("test", ReadonlyConfig.fromMap(config));
            Schema schema =
                    new Schema(
                            Types.NestedField.required(1, "id", Types.IntegerType.get()),
                            Types.NestedField.optional(
                                    2, "event_time", Types.TimestampType.withZone()));
            Table table =
                    setup.createTable(
                            TableIdentifier.of("test", "events"),
                            schema,
                            PartitionSpec.unpartitioned());
            OffsetDateTime boundary = OffsetDateTime.parse("2026-09-12T04:30:00.123456Z");
            appendRecord(table, 1, boundary.minusNanos(1000));
            appendRecord(table, 2, boundary);
            appendRecord(table, 3, boundary.plusNanos(1000));
            appendRecord(table, 4, null);
            catalog.open();
            try {
                TablePath path = TablePath.of("test", "events");
                long snapshotId = table.currentSnapshot().snapshotId();
                assertThrows(
                        DateTimeParseException.class,
                        () ->
                                catalog.executeSql(
                                        path,
                                        "DELETE FROM test.events WHERE event_time >= 'invalid'"));
                table.refresh();
                assertEquals(snapshotId, table.currentSnapshot().snapshotId());
                assertEquals(new HashSet<>(Arrays.asList(1, 2, 3, 4)), readIds(table));
                catalog.executeSql(
                        path,
                        "DELETE FROM test.events WHERE event_time >= '2026-09-12 10:00:00.123456+05:30' AND event_time < '2026-09-11 23:30:00.123457-05:00'");
                table.refresh();
                assertEquals(new HashSet<>(Arrays.asList(1, 3, 4)), readIds(table));
                appendRecords(table, 5, new OffsetDateTime[] {boundary, boundary.plusNanos(1000)});
                long mixedFileSnapshotId = table.currentSnapshot().snapshotId();
                assertThrows(
                        ValidationException.class,
                        () ->
                                catalog.executeSql(
                                        path,
                                        "DELETE FROM test.events WHERE event_time = '2026-09-12 10:00:00.123456+05:30'"));
                table.refresh();
                assertEquals(mixedFileSnapshotId, table.currentSnapshot().snapshotId());
                assertEquals(new HashSet<>(Arrays.asList(1, 3, 4, 5, 6)), readIds(table));
            } finally {
                catalog.close();
            }
        }
    }

    private void appendRecord(Table table, int id, OffsetDateTime timestamp) throws Exception {
        appendRecords(table, id, new OffsetDateTime[] {timestamp});
    }

    private void appendRecords(Table table, int id, OffsetDateTime[] timestamps) throws Exception {
        DataWriter<Record> writer =
                new GenericAppenderFactory(table.schema(), table.spec())
                        .newDataWriter(
                                EncryptedFiles.plainAsEncryptedOutput(
                                        table.io()
                                                .newOutputFile(
                                                        table.location()
                                                                + "/data/"
                                                                + id
                                                                + ".parquet")),
                                FileFormat.PARQUET,
                                null);
        try (DataWriter<Record> closeable = writer) {
            for (OffsetDateTime timestamp : timestamps) {
                GenericRecord record = GenericRecord.create(table.schema());
                record.setField("id", id++);
                record.setField("event_time", timestamp);
                closeable.write(record);
            }
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
    }

    private Set<Integer> readIds(Table table) throws Exception {
        Set<Integer> ids = new HashSet<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                ids.add((Integer) record.getField("id"));
            }
        }
        return ids;
    }
}
