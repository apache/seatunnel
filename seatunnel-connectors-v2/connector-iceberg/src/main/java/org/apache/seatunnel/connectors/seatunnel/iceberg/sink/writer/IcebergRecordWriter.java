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
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.RowConverter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.schema.SchemaChangeWrapper;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.SchemaUtils;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Types;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class IcebergRecordWriter implements RecordWriter {
    private final Table table;
    private final SinkConfig config;
    private final List<WriteResult> writerResults;
    private TaskWriter<Record> writer;
    private RowConverter recordConverter;
    private final IcebergWriterFactory writerFactory;

    public IcebergRecordWriter(Table table, IcebergWriterFactory writerFactory, SinkConfig config) {
        this.config = config;
        this.table = table;
        this.writerResults = Lists.newArrayList();
        this.recordConverter = new RowConverter(table, config);
        this.writerFactory = writerFactory;
        this.writer = createTaskWriter();
    }

    private TaskWriter<Record> createTaskWriter() {
        return writerFactory.createTaskWriter(table, config);
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow, SeaTunnelRowType rowType) {
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
            throw new RuntimeException(e);
        }
    }

    @Override
    public void applySchemaChange(SeaTunnelRowType afterRowType, SchemaChangeEvent event) {
        log.info("Apply schema change start.");
        SchemaChangeWrapper updates = new SchemaChangeWrapper();
        // get the latest schema in case another process updated it
        table.refresh();
        Schema schema = table.schema();
        if (event instanceof AlterTableDropColumnEvent) {
            AlterTableDropColumnEvent dropColumnEvent = (AlterTableDropColumnEvent) event;
            updates.deleteColumn(dropColumnEvent.getColumn());
        } else if (event instanceof AlterTableAddColumnEvent) {
            // Update column , during data consumption process
        } else if (event instanceof AlterTableModifyColumnEvent) {
            // Update type , during data consumption process
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
        SchemaUtils.applySchemaUpdates(table, updates);
        // initialize a new writer with the new schema
        resetWriter();
    }

    @Override
    public List<WriteResult> complete() {
        flush();
        List<WriteResult> result = Lists.newArrayList(writerResults);
        writerResults.clear();
        resetWriter();
        return result;
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
        org.apache.iceberg.io.WriteResult writeResult;
        try {
            writeResult = writer.complete();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        writerResults.add(
                new WriteResult(
                        Arrays.asList(writeResult.dataFiles()),
                        Arrays.asList(writeResult.deleteFiles()),
                        table.spec().partitionType()));
        writer = null;
    }
}
