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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventHandler;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class FieldRenameTransform extends AbstractCatalogSupportMapTransform {
    public static String PLUGIN_NAME = "FieldRename";

    private CatalogTable inputTable;
    private final FieldRenameConfig config;
    private TableSchemaChangeEventHandler tableSchemaChangeEventHandler;

    public FieldRenameTransform(FieldRenameConfig config, CatalogTable table) {
        super(table);
        this.config = config;
        this.inputTable = table;
        this.tableSchemaChangeEventHandler = new TableSchemaChangeEventDispatcher();
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        return inputRow;
    }

    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
        TableSchema newTableSchema =
                tableSchemaChangeEventHandler.reset(inputTable.getTableSchema()).apply(event);
        this.inputTable =
                CatalogTable.of(
                        inputTable.getTableId(),
                        newTableSchema,
                        inputTable.getOptions(),
                        inputTable.getPartitionKeys(),
                        inputTable.getComment());

        if (event instanceof AlterTableColumnsEvent) {
            AlterTableColumnsEvent alterTableColumnsEvent = (AlterTableColumnsEvent) event;
            AlterTableColumnsEvent newEvent =
                    new AlterTableColumnsEvent(
                            event.tableIdentifier(),
                            alterTableColumnsEvent.getEvents().stream()
                                    .map(this::convertName)
                                    .collect(Collectors.toList()));

            newEvent.setJobId(event.getJobId());
            newEvent.setStatement(((AlterTableColumnsEvent) event).getStatement());
            newEvent.setSourceDialectName(((AlterTableColumnsEvent) event).getSourceDialectName());
            if (event.getChangeAfter() != null) {
                newEvent.setChangeAfter(
                        CatalogTable.of(
                                event.getChangeAfter().getTableId(), event.getChangeAfter()));
            }
            return newEvent;
        }
        if (event instanceof AlterTableColumnEvent) {
            return convertName((AlterTableColumnEvent) event);
        }
        return event;
    }

    @Override
    protected TableSchema transformTableSchema() {
        return convertTableSchema(inputTable.getTableSchema());
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputTable.getTableId();
    }

    @VisibleForTesting
    public String convertName(String name) {
        if (name == null) {
            return null;
        }

        Optional<FieldRenameConfig.SpecificModify> specificValue = getSpecificModify(name);
        if (specificValue.isPresent()) {
            return specificValue.get().getTargetName();
        }
        String replaceTo = null;
        Map<Integer, Integer> replaceIndex = new LinkedHashMap<>();

        if (CollectionUtils.isNotEmpty(config.getReplacementsWithRegex())) {
            for (FieldRenameConfig.ReplacementsWithRegex replacementsWithRegex :
                    config.getReplacementsWithRegex()) {
                Boolean isRegex = replacementsWithRegex.getIsRegex();
                String replacement = replacementsWithRegex.getReplaceFrom();
                if (StringUtils.isNotEmpty(replacement)) {
                    Map<Integer, Integer> matched = new LinkedHashMap<>();
                    if (BooleanUtils.isNotTrue(isRegex)) {
                        if (StringUtils.equals(replacement, name)) {
                            matched.put(0, name.length());
                        }
                    } else {
                        Matcher matcher = Pattern.compile(replacement).matcher(name);
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

        if (config.getConvertCase() != null) {
            switch (config.getConvertCase()) {
                case UPPER:
                    name = name.toUpperCase();
                    break;
                case LOWER:
                    name = name.toLowerCase();
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported convert case: " + config.getConvertCase());
            }
        }
        int offset = 0;
        for (Map.Entry<Integer, Integer> index : replaceIndex.entrySet()) {
            int indexStart = index.getKey();
            int indexEnd = index.getValue();
            name =
                    name.substring(0, indexStart + offset)
                            + replaceTo.trim()
                            + name.substring(indexEnd + offset);
            offset += replaceTo.trim().length() - (indexEnd - indexStart);
        }
        if (StringUtils.isNotBlank(config.getPrefix())) {
            name = config.getPrefix().trim() + name;
        }
        if (StringUtils.isNotBlank(config.getSuffix())) {
            name = name + config.getSuffix().trim();
        }
        return name;
    }

    private Optional<FieldRenameConfig.SpecificModify> getSpecificModify(String oldColumnName) {
        if (config.getSpecific() == null) {
            return Optional.empty();
        }
        return config.getSpecific().stream()
                .filter(specific -> specific.getFieldName().equals(oldColumnName))
                .findFirst();
    }

    @VisibleForTesting
    public AlterTableColumnEvent convertName(AlterTableColumnEvent event) {
        AlterTableColumnEvent newEvent = event;
        switch (event.getEventType()) {
            case SCHEMA_CHANGE_ADD_COLUMN:
                AlterTableAddColumnEvent addColumnEvent = (AlterTableAddColumnEvent) event;
                newEvent =
                        new AlterTableAddColumnEvent(
                                event.tableIdentifier(),
                                convertName(addColumnEvent.getColumn()),
                                addColumnEvent.isFirst(),
                                convertName(addColumnEvent.getAfterColumn()));
                break;
            case SCHEMA_CHANGE_DROP_COLUMN:
                AlterTableDropColumnEvent dropColumnEvent = (AlterTableDropColumnEvent) event;
                newEvent =
                        new AlterTableDropColumnEvent(
                                event.tableIdentifier(), convertName(dropColumnEvent.getColumn()));
                break;
            case SCHEMA_CHANGE_MODIFY_COLUMN:
                AlterTableModifyColumnEvent modifyColumnEvent = (AlterTableModifyColumnEvent) event;
                newEvent =
                        new AlterTableModifyColumnEvent(
                                event.tableIdentifier(),
                                convertName(modifyColumnEvent.getColumn()),
                                modifyColumnEvent.isFirst(),
                                convertName(modifyColumnEvent.getAfterColumn()));
                break;
            case SCHEMA_CHANGE_CHANGE_COLUMN:
                AlterTableChangeColumnEvent changeColumnEvent = (AlterTableChangeColumnEvent) event;
                boolean nameChanged =
                        !changeColumnEvent
                                .getOldColumn()
                                .equals(changeColumnEvent.getColumn().getName());
                if (nameChanged) {
                    log.warn(
                            "FieldRenameTransform does not support changing column name, "
                                    + "old column name: {}, new column name: {}",
                            changeColumnEvent.getOldColumn(),
                            changeColumnEvent.getColumn().getName());
                    return changeColumnEvent;
                }

                newEvent =
                        new AlterTableChangeColumnEvent(
                                event.tableIdentifier(),
                                convertName(changeColumnEvent.getOldColumn()),
                                convertName(changeColumnEvent.getColumn()),
                                changeColumnEvent.isFirst(),
                                convertName(changeColumnEvent.getAfterColumn()));
                break;
            default:
                log.warn("Unsupported event: {}", event);
                return event;
        }

        newEvent.setJobId(event.getJobId());
        newEvent.setStatement(event.getStatement());
        newEvent.setSourceDialectName(event.getSourceDialectName());
        if (event.getChangeAfter() != null) {
            CatalogTable newChangeAfter =
                    CatalogTable.of(
                            event.getChangeAfter().getTableId(),
                            convertTableSchema(event.getChangeAfter().getTableSchema()),
                            event.getChangeAfter().getOptions(),
                            event.getChangeAfter().getPartitionKeys(),
                            event.getChangeAfter().getComment());
            newEvent.setChangeAfter(newChangeAfter);
        }
        return newEvent;
    }

    private Column convertName(Column column) {
        return column.rename(convertName(column.getName()));
    }

    private TableSchema convertTableSchema(TableSchema tableSchema) {
        List<Column> columns =
                tableSchema.getColumns().stream()
                        .map(
                                column -> {
                                    String newColumnName = convertName(column.getName());
                                    return column.rename(newColumnName);
                                })
                        .collect(Collectors.toList());
        PrimaryKey primaryKey =
                Optional.ofNullable(tableSchema.getPrimaryKey())
                        .map(
                                pk ->
                                        PrimaryKey.of(
                                                pk.getPrimaryKey(),
                                                pk.getColumnNames().stream()
                                                        .map(this::convertName)
                                                        .collect(Collectors.toList()),
                                                pk.getEnableAutoId()))
                        .orElse(null);
        List<ConstraintKey> constraintKeys =
                Optional.ofNullable(tableSchema.getConstraintKeys())
                        .map(
                                keyList ->
                                        keyList.stream()
                                                .map(
                                                        key ->
                                                                ConstraintKey.of(
                                                                        key.getConstraintType(),
                                                                        key.getConstraintName(),
                                                                        key.getColumnNames()
                                                                                .stream()
                                                                                .map(
                                                                                        column ->
                                                                                                ConstraintKey
                                                                                                        .ConstraintKeyColumn
                                                                                                        .of(
                                                                                                                convertName(
                                                                                                                        column
                                                                                                                                .getColumnName()),
                                                                                                                column
                                                                                                                        .getSortType()))
                                                                                .collect(
                                                                                        Collectors
                                                                                                .toList())))
                                                .collect(Collectors.toList()))
                        .orElse(null);
        return TableSchema.builder()
                .columns(columns)
                .primaryKey(primaryKey)
                .constraintKey(constraintKeys)
                .build();
    }
}
