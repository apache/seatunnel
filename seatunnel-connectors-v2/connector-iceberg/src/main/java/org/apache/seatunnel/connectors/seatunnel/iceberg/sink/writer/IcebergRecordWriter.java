/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.RowConverter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.schema.SchemaChangeWrapper;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.SchemaUtils;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class IcebergRecordWriter implements RecordWriter {
    private final Table table;
    private final IcebergSinkConfig config;
    private final List<WriteResult> writerResults;
    private volatile TaskWriter<Record> writer;
    private RowConverter recordConverter;
    private final IcebergWriterFactory writerFactory;
    /** Files from flushed v3 generations that have not yet been handed to prepareCommit. */
    private final Set<String> pendingV3Files = new HashSet<>();

    private boolean v3Aborted;
    private Exception v3Failure;
    /** Rows written by the current task writer; it is rotated when this reaches the limit. */
    private long rowsInWriter;

    public IcebergRecordWriter(
            Table table, IcebergWriterFactory writerFactory, IcebergSinkConfig config) {
        this.config = config;
        this.table = table;
        this.writerResults = Lists.newArrayList();
        this.recordConverter = new RowConverter(table, config);
        this.writerFactory = writerFactory;
    }

    private TaskWriter<Record> createTaskWriter() {
        return writerFactory.createTaskWriter(table, config);
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow, SeaTunnelRowType rowType) {
        checkForV3Failure();
        if (writer == null) {
            resetWriter();
        }
        SchemaChangeWrapper updates = new SchemaChangeWrapper();
        Record record = recordConverter.convert(seaTunnelRow, rowType, updates);
        if (!updates.empty()) {
            // Apply for schema update
            applySchemaUpdate(updates);
            // convert the row again, this time using the new table schema
            record = recordConverter.convert(seaTunnelRow, rowType);
        }
        IcebergRecord icebergRecord = new IcebergRecord(record, seaTunnelRow.getRowKind());
        try {
            this.writer.write(icebergRecord);
        } catch (IOException e) {
            abortAfterV3Failure(e);
            throw new RuntimeException(e);
        } catch (RuntimeException e) {
            abortAfterV3Failure(e);
            throw e;
        }
        rowsInWriter++;
        Long flushRowSize = config.getFlushRowSize();
        if (flushRowSize != null && rowsInWriter >= flushRowSize) {
            // A delta writer remembers every key it has inserted until it completes, so a long
            // checkpoint interval keeps all of them in memory. Completing it here frees that
            // memory. Each completed writer becomes its own WriteResult, and the committer keeps
            // their order so that later equality deletes still apply to earlier rows.
            flush();
        }
    }

    @Override
    public void applySchemaChange(SeaTunnelRowType afterRowType, SchemaChangeEvent event) {
        checkForV3Failure();
        log.info("Apply schema change start. Event type: {}", event.getEventType());
        SchemaChangeWrapper updates = new SchemaChangeWrapper();
        // get the latest schema in case another process updated it
        table.refresh();
        Schema schema = table.schema();
        if (event instanceof AlterTableColumnsEvent) {
            AlterTableColumnsEvent columnsEvent = (AlterTableColumnsEvent) event;
            log.info(
                    "Processing AlterTableColumnsEvent with {} events",
                    columnsEvent.getEvents().size());
            for (AlterTableColumnEvent columnEvent : columnsEvent.getEvents()) {
                applySchemaChange(afterRowType, columnEvent);
            }
            return;
        } else if (event instanceof AlterTableDropColumnEvent) {
            AlterTableDropColumnEvent dropColumnEvent = (AlterTableDropColumnEvent) event;
            updates.deleteColumn(dropColumnEvent.getColumn());
        } else if (event instanceof AlterTableAddColumnEvent) {
            AlterTableAddColumnEvent addColumnEvent = (AlterTableAddColumnEvent) event;
            Column column = addColumnEvent.getColumn();
            Type columnType = SchemaUtils.toIcebergType(column.getDataType());
            updates.addColumn(null, column.getName(), columnType);
        } else if (event instanceof AlterTableModifyColumnEvent) {
            AlterTableModifyColumnEvent modifyColumnEvent = (AlterTableModifyColumnEvent) event;
            Column column = modifyColumnEvent.getColumn();
            Type columnType = SchemaUtils.toIcebergType(column.getDataType());
            if (columnType instanceof Type.PrimitiveType) {
                updates.modifyColumn(column.getName(), (Type.PrimitiveType) columnType);
            } else {
                log.warn(
                        "Cannot modify column {} to non-primitive type {}",
                        column.getName(),
                        columnType);
            }
        } else if (event instanceof AlterTableChangeColumnEvent) {
            // rename
            AlterTableChangeColumnEvent changeColumnEvent = (AlterTableChangeColumnEvent) event;
            changeColumn(
                    schema,
                    changeColumnEvent.getColumn(),
                    changeColumnEvent.getOldColumn(),
                    updates);
        }
        if (!updates.empty()) {
            applySchemaUpdate(updates);
        }
        log.info("Apply schema change end.");
    }

    private void changeColumn(
            Schema schema, Column column, String oldColumn, SchemaChangeWrapper updates) {
        Types.NestedField nestedField = schema.findField(oldColumn);
        if (nestedField != null) {
            updates.changeColumn(oldColumn, column.getName());
        }
    }
    /** apply schema update */
    private void applySchemaUpdate(SchemaChangeWrapper updates) {
        // complete the current file
        flush();
        // apply the schema updates, this will refresh the table
        try {
            SchemaUtils.applySchemaUpdates(table, updates);
            // initialize a new writer with the new schema
            resetWriter();
        } catch (RuntimeException e) {
            abortAfterV3Failure(e);
            throw e;
        }
    }

    @Override
    public List<WriteResult> complete() {
        checkForV3Failure();
        flush();
        List<WriteResult> result = Lists.newArrayList(writerResults);
        writerResults.clear();
        // From this point the engine owns the returned files, even if publication has not started.
        pendingV3Files.clear();
        return result;
    }

    /** Release only the files still owned by this attempt, never a completed prepare's output. */
    @Override
    public void close() {
        if (writer instanceof DeletionVectorTaskWriter || !pendingV3Files.isEmpty()) {
            try {
                abortV3Files();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** Reset record writer */
    private void resetWriter() {
        this.writer = createTaskWriter();
        this.recordConverter = new RowConverter(table, config);
    }

    private void flush() {
        if (writer == null) {
            return;
        }
        try {
            org.apache.iceberg.io.WriteResult writeResult = writer.complete();
            writerResults.add(
                    new WriteResult(
                            Arrays.asList(writeResult.dataFiles()),
                            Arrays.asList(writeResult.deleteFiles()),
                            table.spec().partitionType()));
            if (writer instanceof DeletionVectorTaskWriter) {
                ((DeletionVectorTaskWriter) writer).transferCreatedFiles(pendingV3Files);
            }
        } catch (IOException e) {
            abortAfterV3Failure(e);
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            abortAfterV3Failure(e);
            throw e;
        }
        writer = null;
        rowsInWriter = 0;
    }

    private void checkForV3Failure() {
        if (v3Aborted) {
            throw new IllegalStateException("The Iceberg v3 writer attempt was aborted", v3Failure);
        }
    }

    private void abortAfterV3Failure(Exception failure) {
        if (writer instanceof DeletionVectorTaskWriter || !pendingV3Files.isEmpty()) {
            v3Failure = failure;
            try {
                abortV3Files();
            } catch (IOException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
    }

    private void abortV3Files() throws IOException {
        // A retry must not prepare an empty checkpoint after this attempt discarded accepted rows.
        v3Aborted = true;
        writerResults.clear();
        IOException cleanupFailure = null;
        if (writer instanceof DeletionVectorTaskWriter) {
            try {
                writer.abort();
                writer = null;
            } catch (IOException e) {
                cleanupFailure = e;
            }
        }
        if (!pendingV3Files.isEmpty()) {
            try {
                DeletionVectorTaskWriter.deleteOwnedFiles(table.io(), pendingV3Files);
            } catch (IOException e) {
                if (cleanupFailure == null) {
                    cleanupFailure = e;
                } else {
                    cleanupFailure.addSuppressed(e);
                }
            }
        }
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }
}
