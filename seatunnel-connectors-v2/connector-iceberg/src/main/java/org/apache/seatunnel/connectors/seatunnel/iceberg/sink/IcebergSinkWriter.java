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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.DataTypeChangeEventDispatcher;
import org.apache.seatunnel.api.table.schema.handler.DataTypeChangeEventHandler;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergFilesCommitter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.state.IcebergSinkState;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.IcebergWriterFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.RecordWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.WriteResult;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Iceberg sink writer */
@Slf4j
public class IcebergSinkWriter
        implements SinkWriter<SeaTunnelRow, IcebergCommitInfo, IcebergSinkState>,
                SupportMultiTableSinkWriter<Void>,
                SupportSchemaEvolutionSinkWriter {
    private TableSchema tableSchema;
    private SeaTunnelRowType rowType;
    private final SinkConfig config;
    private final IcebergTableLoader icebergTableLoader;
    private volatile RecordWriter writer;
    private final IcebergFilesCommitter filesCommitter;
    private final List<WriteResult> results = Lists.newArrayList();
    private String commitUser = UUID.randomUUID().toString();

    private final DataTypeChangeEventHandler dataTypeChangeEventHandler;

    public IcebergSinkWriter(
            IcebergTableLoader icebergTableLoader,
            SinkConfig config,
            TableSchema tableSchema,
            List<IcebergSinkState> states) {
        this.config = config;
        this.icebergTableLoader = icebergTableLoader;
        this.tableSchema = tableSchema;
        this.rowType = tableSchema.toPhysicalRowDataType();
        this.filesCommitter = IcebergFilesCommitter.of(config, icebergTableLoader);
        this.dataTypeChangeEventHandler = new DataTypeChangeEventDispatcher();
        if (Objects.nonNull(states) && !states.isEmpty()) {
            this.commitUser = states.get(0).getCommitUser();
            preCommit(states);
        }
    }

    private void preCommit(List<IcebergSinkState> states) {
        states.forEach(
                icebergSinkState -> {
                    filesCommitter.doCommit(icebergSinkState.getWriteResults());
                });
    }

    private void tryCreateRecordWriter() {
        if (this.writer == null) {
            IcebergWriterFactory icebergWriterFactory =
                    new IcebergWriterFactory(icebergTableLoader, config);
            this.writer = icebergWriterFactory.createWriter(this.tableSchema);
        }
    }

    public static IcebergSinkWriter of(SinkConfig config, CatalogTable catalogTable) {
        return of(config, catalogTable, null);
    }

    public static IcebergSinkWriter of(
            SinkConfig config, CatalogTable catalogTable, List<IcebergSinkState> states) {
        IcebergTableLoader icebergTableLoader = IcebergTableLoader.create(config, catalogTable);
        return new IcebergSinkWriter(
                icebergTableLoader, config, catalogTable.getTableSchema(), states);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        tryCreateRecordWriter();
        writer.write(element, rowType);
    }

    @Override
    public Optional<IcebergCommitInfo> prepareCommit() throws IOException {
        List<WriteResult> writeResults;
        if (writer != null) {
            writeResults = writer.complete();
        } else {
            writeResults = Collections.emptyList();
        }
        IcebergCommitInfo icebergCommitInfo = new IcebergCommitInfo(writeResults);
        this.results.addAll(writeResults);
        return Optional.of(icebergCommitInfo);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        // Waiting cdc connector support schema change event
        if (config.isTableSchemaEvolutionEnabled()) {
            log.info("changed rowType before: {}", fieldsInfo(rowType));
            this.rowType = dataTypeChangeEventHandler.reset(rowType).apply(event);
            log.info("changed rowType after: {}", fieldsInfo(rowType));
            tryCreateRecordWriter();
            writer.applySchemaChange(this.rowType, event);
        }
    }

    @Override
    public List<IcebergSinkState> snapshotState(long checkpointId) throws IOException {
        IcebergSinkState icebergSinkState = new IcebergSinkState(results, commitUser, checkpointId);
        results.clear();
        return Collections.singletonList(icebergSinkState);
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        try {
            if (writer != null) {
                writer.close();
            }
            icebergTableLoader.close();
        } finally {
            results.clear();
        }
    }

    private String fieldsInfo(SeaTunnelRowType seaTunnelRowType) {
        String[] fieldsInfo = new String[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            fieldsInfo[i] =
                    String.format(
                            "%s<%s>",
                            seaTunnelRowType.getFieldName(i), seaTunnelRowType.getFieldType(i));
        }
        return StringUtils.join(fieldsInfo, ", ");
    }
}
