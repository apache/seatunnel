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

package org.apache.seatunnel.transform.rename;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class TableRenameTransform extends AbstractCatalogSupportMapTransform {
    public static String PLUGIN_NAME = "TableRename";

    private final CatalogTable inputTable;
    private final TableRenameConfig config;

    private TablePath outputTablePath;
    private String outputTableId;

    public TableRenameTransform(TableRenameConfig config, CatalogTable table) {
        super(table);
        this.inputTable = table;
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected TableSchema transformTableSchema() {
        return TableSchema.builder()
                .columns(inputTable.getTableSchema().getColumns())
                .constraintKey(inputTable.getTableSchema().getConstraintKeys())
                .primaryKey(inputTable.getTableSchema().getPrimaryKey())
                .build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        TablePath inputTablePath = inputTable.getTablePath();
        String inputDatabaseName = inputTablePath.getDatabaseName();
        String inputSchemaName = inputTablePath.getSchemaName();
        String inputTableName = inputTablePath.getTableName();

        String outputDatabaseName =
                Optional.ofNullable(inputDatabaseName).map(this::convertCase).orElse(null);
        String outputSchemaName =
                Optional.ofNullable(inputSchemaName).map(this::convertCase).orElse(null);
        String outputTableName = convertName(inputTableName);
        TablePath outputTablePath =
                TablePath.of(outputDatabaseName, outputSchemaName, outputTableName);
        this.outputTablePath = outputTablePath;
        this.outputTableId = outputTablePath.getFullName();
        return TableIdentifier.of(inputTable.getCatalogName(), outputTablePath);
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        if (inputRow.getTableId() == null) {
            log.debug("Table id is null, skip renaming");
            return inputRow;
        }
        if (outputTableId.equals(inputRow.getTableId())) {
            return inputRow;
        }

        SeaTunnelRow outputRow = inputRow.copy();
        outputRow.setTableId(outputTableId);
        return outputRow;
    }

    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
        TablePath inputTablePath = event.tablePath();
        if (inputTablePath == null) {
            return event;
        }
        if (outputTablePath.equals(inputTablePath)) {
            return event;
        }

        if (event instanceof AlterTableColumnsEvent) {
            TableIdentifier newTableIdentifier =
                    TableIdentifier.of(event.tableIdentifier().getCatalogName(), outputTablePath);
            AlterTableColumnsEvent alterTableColumnsEvent = (AlterTableColumnsEvent) event;
            AlterTableColumnsEvent newEvent =
                    new AlterTableColumnsEvent(
                            newTableIdentifier,
                            alterTableColumnsEvent.getEvents().stream()
                                    .map(this::convertName)
                                    .collect(Collectors.toList()));

            newEvent.setJobId(event.getJobId());
            newEvent.setStatement(((AlterTableColumnsEvent) event).getStatement());
            newEvent.setSourceDialectName(((AlterTableColumnsEvent) event).getSourceDialectName());
            if (event.getChangeAfter() != null) {
                newEvent.setChangeAfter(
                        CatalogTable.of(newTableIdentifier, event.getChangeAfter()));
            }
            return newEvent;
        }
        if (event instanceof AlterTableColumnEvent) {
            return convertName((AlterTableColumnEvent) event);
        }
        return event;
    }

    public String convertCase(String name) {
        if (config.getConvertCase() != null) {
            switch (config.getConvertCase()) {
                case UPPER:
                    return name.toUpperCase();
                case LOWER:
                    return name.toLowerCase();
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported convert case: " + config.getConvertCase());
            }
        }
        return name;
    }

    @VisibleForTesting
    public String convertName(String tableName) {
        String replaceTo = null;
        Map<Integer, Integer> replaceIndex = new LinkedHashMap<>();

        if (CollectionUtils.isNotEmpty(config.getReplacementsWithRegex())) {
            for (TableRenameConfig.ReplacementsWithRegex replacementsWithRegex :
                    config.getReplacementsWithRegex()) {
                Boolean isRegex = replacementsWithRegex.getIsRegex();
                String replacement = replacementsWithRegex.getReplaceFrom();
                if (StringUtils.isNotEmpty(replacement)) {
                    Map<Integer, Integer> matched = new LinkedHashMap<>();
                    if (BooleanUtils.isNotTrue(isRegex)) {
                        if (StringUtils.equals(replacement, tableName)) {
                            matched.put(0, tableName.length());
                        }
                    } else {
                        Matcher matcher = Pattern.compile(replacement).matcher(tableName);
                        while (matcher.find()) {
                            matched.put(matcher.start(), matcher.end());
                        }
                    }
                    if (!matched.isEmpty()) {
                        replaceTo = replacementsWithRegex.getReplaceTo();
                        replaceIndex = matched;
                    }
                }
            }
        }

        tableName = convertCase(tableName);

        int offset = 0;
        for (Map.Entry<Integer, Integer> index : replaceIndex.entrySet()) {
            int indexStart = index.getKey();
            int indexEnd = index.getValue();
            tableName =
                    tableName.substring(0, indexStart + offset)
                            + replaceTo.trim()
                            + tableName.substring(indexEnd + offset);
            offset += replaceTo.trim().length() - (indexEnd - indexStart);
        }
        if (StringUtils.isNotBlank(config.getPrefix())) {
            tableName = config.getPrefix().trim() + tableName;
        }
        if (StringUtils.isNotBlank(config.getSuffix())) {
            tableName = tableName + config.getSuffix().trim();
        }
        return tableName;
    }

    @VisibleForTesting
    public AlterTableColumnEvent convertName(AlterTableColumnEvent event) {
        TableIdentifier newTableIdentifier =
                TableIdentifier.of(event.tableIdentifier().getCatalogName(), outputTablePath);
        AlterTableColumnEvent newEvent = event;
        switch (event.getEventType()) {
            case SCHEMA_CHANGE_ADD_COLUMN:
                AlterTableAddColumnEvent addColumnEvent = (AlterTableAddColumnEvent) event;
                newEvent =
                        new AlterTableAddColumnEvent(
                                newTableIdentifier,
                                addColumnEvent.getColumn(),
                                addColumnEvent.isFirst(),
                                addColumnEvent.getAfterColumn());
                break;
            case SCHEMA_CHANGE_DROP_COLUMN:
                AlterTableDropColumnEvent dropColumnEvent = (AlterTableDropColumnEvent) event;
                newEvent =
                        new AlterTableDropColumnEvent(
                                newTableIdentifier, dropColumnEvent.getColumn());
                break;
            case SCHEMA_CHANGE_MODIFY_COLUMN:
                AlterTableModifyColumnEvent modifyColumnEvent = (AlterTableModifyColumnEvent) event;
                newEvent =
                        new AlterTableModifyColumnEvent(
                                newTableIdentifier,
                                modifyColumnEvent.getColumn(),
                                modifyColumnEvent.isFirst(),
                                modifyColumnEvent.getAfterColumn());
                break;
            case SCHEMA_CHANGE_CHANGE_COLUMN:
                AlterTableChangeColumnEvent changeColumnEvent = (AlterTableChangeColumnEvent) event;
                newEvent =
                        new AlterTableChangeColumnEvent(
                                newTableIdentifier,
                                changeColumnEvent.getOldColumn(),
                                changeColumnEvent.getColumn(),
                                changeColumnEvent.isFirst(),
                                changeColumnEvent.getAfterColumn());
                break;
            default:
                log.warn("Unsupported event: {}", event);
                return event;
        }

        newEvent.setJobId(event.getJobId());
        newEvent.setStatement(event.getStatement());
        newEvent.setSourceDialectName(event.getSourceDialectName());
        if (event.getChangeAfter() != null) {
            newEvent.setChangeAfter(CatalogTable.of(newTableIdentifier, event.getChangeAfter()));
        }
        return newEvent;
    }
}
