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

package org.apache.seatunnel.connectors.seatunnel.iceberg.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.IcebergTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.schema.SchemaAddColumn;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.schema.SchemaChangeColumn;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.schema.SchemaChangeWrapper;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.schema.SchemaDeleteColumn;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.schema.SchemaModifyColumn;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;

import org.jetbrains.annotations.NotNull;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Slf4j
public class SchemaUtils {
    private static final Pattern TRANSFORM_REGEX = Pattern.compile("(\\w+)\\((.+)\\)");

    private SchemaUtils() {}

    public static Type.PrimitiveType needsDataTypeUpdate(Type currentIcebergType, Type afterType) {
        if (currentIcebergType.typeId() == Type.TypeID.FLOAT
                && afterType.typeId() == Type.TypeID.DOUBLE) {
            return Types.DoubleType.get();
        }
        if (currentIcebergType.typeId() == Type.TypeID.INTEGER
                && afterType.typeId() == Type.TypeID.LONG) {
            return Types.LongType.get();
        }
        return null;
    }

    public static void applySchemaUpdates(Table table, SchemaChangeWrapper wrapper) {
        if (wrapper == null || wrapper.empty()) {
            // no updates to apply
            return;
        }
        Tasks.range(1)
                .retry(SinkConfig.SCHEMA_UPDATE_RETRIES)
                .run(notUsed -> commitSchemaUpdates(table, wrapper));
    }

    public static Table autoCreateTable(
            Catalog catalog, TablePath tablePath, CatalogTable table, ReadonlyConfig readonlyConfig)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        TableSchema tableSchema = table.getTableSchema();
        // Convert to iceberg schema
        Schema schema = toIcebergSchema(tableSchema, readonlyConfig);
        // Convert sink config
        SinkConfig config = new SinkConfig(readonlyConfig);
        // build auto create table
        Map<String, String> options = new HashMap<>(table.getOptions());
        // override
        options.putAll(config.getAutoCreateProps());
        return createTable(catalog, toIcebergTableIdentifier(tablePath), config, schema, options);
    }

    public static Table autoCreateTable(
            Catalog catalog,
            TableIdentifier tableIdentifier,
            SinkConfig config,
            TableSchema tableSchema) {
        // Generate struct type
        Schema schema = toIcebergSchema(tableSchema, config.getReadonlyConfig());
        return createTable(catalog, tableIdentifier, config, schema, config.getAutoCreateProps());
    }

    private static Table createTable(
            Catalog catalog,
            TableIdentifier tableIdentifier,
            SinkConfig config,
            Schema schema,
            Map<String, String> autoCreateProps) {

        List<String> partitionBy = config.getPartitionKeys();
        PartitionSpec spec;
        try {
            spec = SchemaUtils.createPartitionSpec(schema, partitionBy);
        } catch (Exception e) {
            log.error(
                    "Unable to create partition spec {}, table {} will be unpartitioned",
                    partitionBy,
                    tableIdentifier,
                    e);
            spec = PartitionSpec.unpartitioned();
        }
        PartitionSpec partitionSpec = spec;
        AtomicReference<Table> result = new AtomicReference<>();
        Tasks.range(1)
                .retry(SinkConfig.CREATE_TABLE_RETRIES)
                .run(
                        notUsed -> {
                            Table table =
                                    catalog.createTable(
                                            tableIdentifier,
                                            schema,
                                            partitionSpec,
                                            autoCreateProps);
                            result.set(table);
                        });
        return result.get();
    }

    @VisibleForTesting
    @NotNull protected static Schema toIcebergSchema(
            TableSchema tableSchema, ReadonlyConfig readonlyConfig) {
        Types.StructType structType = SchemaUtils.toIcebergType(tableSchema);
        Set<Integer> identifierFieldIds =
                readonlyConfig.getOptional(SinkConfig.TABLE_PRIMARY_KEYS)
                        .map(e -> SinkConfig.stringToList(e, ","))
                        .orElseGet(
                                () ->
                                        Optional.ofNullable(tableSchema.getPrimaryKey())
                                                .map(e -> e.getColumnNames())
                                                .orElse(Collections.emptyList()))
                        .stream()
                        .map(f -> structType.field(f).fieldId())
                        .collect(Collectors.toSet());
        List<Types.NestedField> fields = new ArrayList<>();
        structType
                .fields()
                .forEach(
                        field ->
                                fields.add(
                                        identifierFieldIds.contains(field.fieldId())
                                                ? field.asRequired()
                                                : field.asOptional()));
        return new Schema(fields, identifierFieldIds);
    }

    public static TableIdentifier toIcebergTableIdentifier(TablePath tablePath) {
        return TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    public static TablePath toTablePath(TableIdentifier tableIdentifier) {
        return TablePath.of(tableIdentifier.namespace().toString(), tableIdentifier.name());
    }

    /** Commit table schema updates */
    private static void commitSchemaUpdates(Table table, SchemaChangeWrapper wrapper) {
        // get the latest schema in case another process updated it
        table.refresh();
        // filter out columns that have already been added
        List<SchemaAddColumn> addColumns =
                wrapper.addColumns().stream()
                        .filter(addCol -> !columnExists(table.schema(), addCol))
                        .collect(toList());

        // filter out columns that have the updated type
        List<SchemaModifyColumn> modifyColumns =
                wrapper.modifyColumns().stream()
                        .filter(updateType -> !typeMatches(table.schema(), updateType))
                        .collect(toList());

        // filter out columns that have already been deleted
        List<SchemaDeleteColumn> deleteColumns =
                wrapper.deleteColumns().stream()
                        .filter(deleteColumn -> findColumns(table.schema(), deleteColumn))
                        .collect(toList());

        // filter out columns that have already been changed
        List<SchemaChangeColumn> changeColumns =
                wrapper.changeColumns().stream()
                        .filter(changeColumn -> findColumns(table.schema(), changeColumn))
                        .collect(toList());

        if (addColumns.isEmpty()
                && modifyColumns.isEmpty()
                && deleteColumns.isEmpty()
                && changeColumns.isEmpty()) {
            // no updates to apply
            log.info("Schema for table {} already up-to-date", table.name());
            return;
        }

        // apply the updates
        UpdateSchema updateSchema = table.updateSchema();
        addColumns.forEach(
                update ->
                        updateSchema.addColumn(update.parentName(), update.name(), update.type()));
        modifyColumns.forEach(update -> updateSchema.updateColumn(update.name(), update.type()));
        deleteColumns.forEach(delete -> updateSchema.deleteColumn(delete.name()));
        changeColumns.forEach(
                changeColumn ->
                        updateSchema.renameColumn(changeColumn.oldName(), changeColumn.newName()));
        updateSchema.commit();
        log.info("Schema for table {} updated with new columns", table.name());
    }

    private static boolean columnExists(Schema schema, SchemaAddColumn update) {
        Types.StructType struct =
                update.parentName() == null
                        ? schema.asStruct()
                        : schema.findType(update.parentName()).asStructType();
        return struct.field(update.name()) != null;
    }

    private static boolean typeMatches(Schema schema, SchemaModifyColumn update) {
        return schema.findType(update.name()).typeId() == update.type().typeId();
    }

    private static boolean findColumns(Schema schema, SchemaDeleteColumn deleteColumn) {
        return schema.findField(deleteColumn.name()) != null;
    }

    private static boolean findColumns(
            org.apache.iceberg.Schema schema, SchemaChangeColumn changeColumn) {
        return schema.findField(changeColumn.oldName()) != null;
    }

    public static SeaTunnelDataType<?> toSeaTunnelType(String fieldName, Type type) {
        return IcebergTypeMapper.mapping(fieldName, type);
    }

    public static Type toIcebergType(SeaTunnelDataType<?> rowType) {
        return IcebergTypeMapper.toIcebergType(rowType);
    }

    public static Types.StructType toIcebergType(TableSchema tableSchema) {
        List<Types.NestedField> structFields = new ArrayList<>();
        AtomicInteger idIncrementer = new AtomicInteger(1);
        for (Column column : tableSchema.getColumns()) {
            Types.NestedField icebergField =
                    Types.NestedField.of(
                            idIncrementer.getAndIncrement(),
                            column.isNullable(),
                            column.getName(),
                            IcebergTypeMapper.toIcebergType(column.getDataType(), idIncrementer),
                            column.getComment());
            structFields.add(icebergField);
        }
        return Types.StructType.of(structFields);
    }

    public static PartitionSpec createPartitionSpec(Schema schema, List<String> partitionBy) {
        if (partitionBy.isEmpty()) {
            return PartitionSpec.unpartitioned();
        }
        PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
        partitionBy.forEach(
                partitionField -> {
                    Matcher matcher = TRANSFORM_REGEX.matcher(partitionField);
                    if (matcher.matches()) {
                        String transform = matcher.group(1);
                        switch (transform) {
                            case "year":
                            case "years":
                                specBuilder.year(matcher.group(2));
                                break;
                            case "month":
                            case "months":
                                specBuilder.month(matcher.group(2));
                                break;
                            case "day":
                            case "days":
                                specBuilder.day(matcher.group(2));
                                break;
                            case "hour":
                            case "hours":
                                specBuilder.hour(matcher.group(2));
                                break;
                            case "bucket":
                                {
                                    Pair<String, Integer> args = transformArgPair(matcher.group(2));
                                    specBuilder.bucket(args.first(), args.second());
                                    break;
                                }
                            case "truncate":
                                {
                                    Pair<String, Integer> args = transformArgPair(matcher.group(2));
                                    specBuilder.truncate(args.first(), args.second());
                                    break;
                                }
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported transform: " + transform);
                        }
                    } else {
                        specBuilder.identity(partitionField);
                    }
                });
        return specBuilder.build();
    }

    private static Pair<String, Integer> transformArgPair(String argsStr) {
        String[] parts = argsStr.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid argument " + argsStr + ", should have 2 parts");
        }
        return Pair.of(parts[0].trim(), Integer.parseInt(parts[1].trim()));
    }
}
