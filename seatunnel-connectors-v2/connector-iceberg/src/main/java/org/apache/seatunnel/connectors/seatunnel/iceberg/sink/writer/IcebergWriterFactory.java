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
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.SchemaUtils;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
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
    private final IcebergSinkConfig config;

    public IcebergWriterFactory(IcebergTableLoader tableLoader, IcebergSinkConfig config) {
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

    public TaskWriter<Record> createTaskWriter(Table table, IcebergSinkConfig config) {
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

        Set<Integer> identifierFieldIds = identifierFieldIds(table, config);

        if (writesDeltas(identifierFieldIds, config)) {
            checkDeltaWriteSupported(table);
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
            if (!writesDeltas(identifierFieldIds, config)) {
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
            if (!writesDeltas(identifierFieldIds, config)) {
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

    /**
     * Returns the ids of the fields the writer applies deletes and upserts by: the identifier
     * fields of the table, or the columns of {@code iceberg.table.primary-keys} when it is set.
     *
     * @param table the Iceberg table the writer writes to
     * @param config the sink config
     * @return the identifier field ids, empty when the table has none and no key is configured
     */
    static Set<Integer> identifierFieldIds(Table table, IcebergSinkConfig config) {
        List<String> idCols = config.getPrimaryKeys();
        if (idCols.isEmpty()) {
            return table.schema().identifierFieldIds();
        }
        return idCols.stream()
                .map(
                        colName ->
                                config.isCaseSensitive()
                                        ? table.schema().caseInsensitiveFindField(colName).fieldId()
                                        : table.schema().findField(colName).fieldId())
                .collect(toSet());
    }

    /**
     * Whether the writer for these identifier fields is a delta writer, which applies deletes and
     * upserts by key, rather than an append writer.
     */
    static boolean writesDeltas(Set<Integer> identifierFieldIds, IcebergSinkConfig config) {
        return !identifierFieldIds.isEmpty() || config.isUpsertModeEnabled();
    }

    /**
     * The delta writers emit position delete files for rows that change more than once before a
     * commit. Format version 3 only accepts deletion vectors for position deletes, so such a commit
     * would fail; reject the write before any data file is produced.
     */
    private static void checkDeltaWriteSupported(Table table) {
        int formatVersion = TableUtil.formatVersion(table);
        if (formatVersion >= 3) {
            throw new IcebergConnectorException(
                    IcebergConnectorErrorCode.UNSUPPORTED_WRITE_FOR_TABLE_FORMAT_VERSION,
                    String.format(
                            "Upsert and CDC writes are not supported for Iceberg tables with format"
                                    + " version %d yet, table: %s. They need deletion vectors, which"
                                    + " this connector does not write. Use a format version 2 table,"
                                    + " or append without %s, %s and table identifier fields.",
                            formatVersion,
                            table.name(),
                            IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(),
                            IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP.key()));
        }
    }
}
