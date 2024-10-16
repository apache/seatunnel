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

import org.apache.seatunnel.shade.com.google.common.collect.Maps;
import org.apache.seatunnel.shade.com.google.common.collect.Sets;
import org.apache.seatunnel.shade.com.google.common.primitives.Ints;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.SchemaUtils;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static java.util.stream.Collectors.toSet;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

@Slf4j
public class IcebergWriterFactory {
    private final IcebergTableLoader tableLoader;
    private final SinkConfig config;

    public IcebergWriterFactory(IcebergTableLoader tableLoader, SinkConfig config) {
        this.tableLoader = tableLoader;
        this.config = config;
    }

    public RecordWriter createWriter(TableSchema tableSchema) {
        Table table;
        try {
            table = tableLoader.loadTable();
        } catch (NoSuchTableException exception) {
            // for e2e test , Normally, IcebergCatalog should be used to create a table
            switch (config.getSchemaSaveMode()) {
                case CREATE_SCHEMA_WHEN_NOT_EXIST:
                    table =
                            SchemaUtils.autoCreateTable(
                                    tableLoader.getCatalog(),
                                    tableLoader.getTableIdentifier(),
                                    config,
                                    tableSchema);
                    // Create an empty snapshot for the branch
                    if (config.getCommitBranch() != null) {
                        table.manageSnapshots().createBranch(config.getCommitBranch()).commit();
                    }
                    break;
                default:
                    throw exception;
            }
        }
        return new IcebergRecordWriter(table, this, config);
    }

    public TaskWriter<Record> createTaskWriter(Table table, SinkConfig config) {
        Map<String, String> tableProps = Maps.newHashMap(table.properties());
        tableProps.putAll(config.getWriteProps());

        String formatStr =
                tableProps.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
        FileFormat format = FileFormat.valueOf(formatStr.toUpperCase());

        long targetFileSize =
                PropertyUtil.propertyAsLong(
                        tableProps,
                        WRITE_TARGET_FILE_SIZE_BYTES,
                        WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

        Set<Integer> identifierFieldIds = table.schema().identifierFieldIds();

        // override the identifier fields if the config is set
        List<String> idCols = config.getPrimaryKeys();
        if (!idCols.isEmpty()) {
            identifierFieldIds =
                    idCols.stream()
                            .map(
                                    colName ->
                                            config.isCaseSensitive()
                                                    ? table.schema()
                                                            .caseInsensitiveFindField(colName)
                                                            .fieldId()
                                                    : table.schema().findField(colName).fieldId())
                            .collect(toSet());
        }

        FileAppenderFactory<Record> appenderFactory;
        if (identifierFieldIds == null || identifierFieldIds.isEmpty()) {
            appenderFactory =
                    new GenericAppenderFactory(table.schema(), table.spec(), null, null, null)
                            .setAll(tableProps);
        } else {
            appenderFactory =
                    new GenericAppenderFactory(
                                    table.schema(),
                                    table.spec(),
                                    Ints.toArray(identifierFieldIds),
                                    TypeUtil.select(
                                            table.schema(), Sets.newHashSet(identifierFieldIds)),
                                    null)
                            .setAll(tableProps);
        }

        // (partition ID + task ID + operation ID) must be unique
        OutputFileFactory fileFactory =
                OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                        .defaultSpec(table.spec())
                        .operationId(UUID.randomUUID().toString())
                        .format(format)
                        .build();

        TaskWriter<Record> writer;
        if (table.spec().isUnpartitioned()) {
            if (identifierFieldIds.isEmpty() && !config.isUpsertModeEnabled()) {
                // No delta writer
                writer =
                        new UnpartitionedWriter<>(
                                table.spec(),
                                format,
                                appenderFactory,
                                fileFactory,
                                table.io(),
                                targetFileSize);
            } else {
                // Delta writer
                writer =
                        new UnpartitionedDeltaWriter(
                                table.spec(),
                                format,
                                appenderFactory,
                                fileFactory,
                                table.io(),
                                targetFileSize,
                                table.schema(),
                                identifierFieldIds,
                                config.isUpsertModeEnabled());
            }
        } else {
            if (identifierFieldIds.isEmpty() && !config.isUpsertModeEnabled()) {
                // No delta writer
                writer =
                        new PartitionedAppendWriter(
                                table.spec(),
                                format,
                                appenderFactory,
                                fileFactory,
                                table.io(),
                                targetFileSize,
                                table.schema());
            } else {
                // Delta writer
                writer =
                        new PartitionedDeltaWriter(
                                table.spec(),
                                format,
                                appenderFactory,
                                fileFactory,
                                table.io(),
                                targetFileSize,
                                table.schema(),
                                identifierFieldIds,
                                config.isUpsertModeEnabled());
            }
        }
        return writer;
    }
}
