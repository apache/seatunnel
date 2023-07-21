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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.util.RowDataWrapper;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.ArrayUtil;

import lombok.SneakyThrows;

import java.io.Closeable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class SeaTunnelRowDataTaskWriterFactory
        implements TaskWriterFactory<SeaTunnelRow>, Closeable {
    private final IcebergTableLoader tableLoader;
    private final SeaTunnelRowType seaTunnelSchema;
    private final long targetFileSizeBytes;
    private final FileFormat format;
    private final List<Integer> equalityFieldIds;
    private final boolean upsert;
    private final Map<String, String> writeProperties;
    private transient FileAppenderFactory<SeaTunnelRow> appenderFactory;
    private transient OutputFileFactory outputFileFactory;
    private transient Table table;
    private transient Schema schema;
    private transient PartitionSpec spec;
    private transient FileIO io;

    public SeaTunnelRowDataTaskWriterFactory(
            IcebergTableLoader tableLoader,
            SeaTunnelRowType seaTunnelSchema,
            long targetFileSizeBytes,
            FileFormat format,
            Map<String, String> writeProperties,
            List<Integer> equalityFieldIds,
            boolean upsert) {
        this.tableLoader = tableLoader;
        this.seaTunnelSchema = seaTunnelSchema;
        this.targetFileSizeBytes = targetFileSizeBytes;
        this.format = format;
        this.equalityFieldIds = equalityFieldIds;
        this.upsert = upsert;
        this.writeProperties = writeProperties;
    }

    @Override
    public void initialize(int taskId, int attemptId) {
        tableLoader.open();
        this.table = tableLoader.loadTable();
        this.schema = table.schema();
        this.spec = table.spec();
        this.io = table.io();

        if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
            this.appenderFactory =
                    new SeaTunnelAppenderFactory(
                            table,
                            schema,
                            seaTunnelSchema,
                            writeProperties,
                            spec,
                            null,
                            null,
                            null);
        } else if (upsert) {
            // In upsert mode, only the new row is emitted using INSERT row kind. Therefore, any
            // column of
            // the inserted row
            // may differ from the deleted row other than the primary key fields, and the delete
            // file must
            // contain values
            // that are correct for the deleted row. Therefore, only write the equality delete
            // fields.
            this.appenderFactory =
                    new SeaTunnelAppenderFactory(
                            table,
                            schema,
                            seaTunnelSchema,
                            writeProperties,
                            spec,
                            ArrayUtil.toIntArray(equalityFieldIds),
                            TypeUtil.select(schema, new HashSet<>(equalityFieldIds)),
                            null);
        } else {
            this.appenderFactory =
                    new SeaTunnelAppenderFactory(
                            table,
                            schema,
                            seaTunnelSchema,
                            writeProperties,
                            spec,
                            ArrayUtil.toIntArray(equalityFieldIds),
                            schema,
                            null);
        }

        this.outputFileFactory =
                OutputFileFactory.builderFor(table, taskId, attemptId).format(format).build();
    }

    @Override
    public TaskWriter<SeaTunnelRow> create() {
        checkNotNull(
                outputFileFactory,
                "The outputFileFactory shouldn't be null if we have invoked the initialize().");

        if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
            // Initialize a task writer to write INSERT only.
            if (spec.isUnpartitioned()) {
                return new UnpartitionedWriter<>(
                        spec, format, appenderFactory, outputFileFactory, io, targetFileSizeBytes);
            } else {
                return new RowDataPartitionedFanoutWriter(
                        spec,
                        format,
                        appenderFactory,
                        outputFileFactory,
                        io,
                        targetFileSizeBytes,
                        schema,
                        seaTunnelSchema);
            }
        } else {
            // Initialize a task writer to write both INSERT and equality DELETE.
            if (spec.isUnpartitioned()) {
                return new UnpartitionedDeltaWriter(
                        spec,
                        format,
                        appenderFactory,
                        outputFileFactory,
                        io,
                        targetFileSizeBytes,
                        schema,
                        seaTunnelSchema,
                        equalityFieldIds,
                        upsert);
            } else {
                return new PartitionedDeltaWriter(
                        spec,
                        format,
                        appenderFactory,
                        outputFileFactory,
                        io,
                        targetFileSizeBytes,
                        schema,
                        seaTunnelSchema,
                        equalityFieldIds,
                        upsert);
            }
        }
    }

    @Override
    @SneakyThrows
    public void close() {
        if (tableLoader != null) {
            tableLoader.close();
        }
    }

    private static class RowDataPartitionedFanoutWriter
            extends PartitionedFanoutWriter<SeaTunnelRow> {

        private final PartitionKey partitionKey;
        private final RowDataWrapper rowDataWrapper;

        RowDataPartitionedFanoutWriter(
                PartitionSpec spec,
                FileFormat format,
                FileAppenderFactory<SeaTunnelRow> appenderFactory,
                OutputFileFactory fileFactory,
                FileIO io,
                long targetFileSize,
                Schema schema,
                SeaTunnelRowType seaTunnelSchema) {
            super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
            this.partitionKey = new PartitionKey(spec, schema);
            this.rowDataWrapper = new RowDataWrapper(seaTunnelSchema, schema.asStruct());
        }

        @Override
        protected PartitionKey partition(SeaTunnelRow row) {
            partitionKey.partition(rowDataWrapper.wrap(row));
            return partitionKey;
        }
    }
}
