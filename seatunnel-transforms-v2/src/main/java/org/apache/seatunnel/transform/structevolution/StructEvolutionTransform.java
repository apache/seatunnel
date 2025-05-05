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

package org.apache.seatunnel.transform.structevolution;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil.convertSqlTypeToSeaTunnelDataType;

@Slf4j
public class StructEvolutionTransform extends MultipleFieldOutputTransform {
    public static final String PLUGIN_NAME = "StructEvolution";
    private final TableIdentifier inputTableIdentifier;
    private final TableIdentifier outputTableIdentifier;
    private final List<Column> inputColumns;
    private final List<ColumnWrapper> outputColumns;
    private final StructEvolutionConfig.SpecificModify specificModified;
    private Map<Integer, Integer> positionMapping;

    public StructEvolutionTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.inputTableIdentifier = inputCatalogTable.getTableId();

        this.inputColumns = inputCatalogTable.getTableSchema().getColumns();
        this.specificModified =
                config.get(StructEvolutionConfig.SPECIFIC).stream()
                        .filter(
                                specificModify -> {
                                    TablePath tablePath =
                                            TablePath.of(specificModify.getInputName(), true);
                                    return tablePath
                                                    .getSchemaName()
                                                    .equals(inputTableIdentifier.getSchemaName())
                                            && tablePath
                                                    .getTableName()
                                                    .equals(inputTableIdentifier.getTableName());
                                })
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "No specific modify found for table: "
                                                        + inputTableIdentifier.getTableName()));
        this.outputTableIdentifier = initTableIdentifier();
        this.outputColumns = initColumns();
    }

    private TableIdentifier initTableIdentifier() {
        String[] split = specificModified.getOutputName().split("\\.");
        if (split.length == 1) {
            return TableIdentifier.of(
                    inputCatalogTable.getCatalogName(),
                    inputTableIdentifier.getDatabaseName(),
                    inputTableIdentifier.getSchemaName(),
                    split[0]);
        } else if (split.length == 2) {
            return TableIdentifier.of(
                    inputCatalogTable.getCatalogName(),
                    inputTableIdentifier.getDatabaseName(),
                    split[0],
                    split[1]);
        } else {
            throw new IllegalArgumentException(
                    "Invalid output table identifier: " + specificModified.getOutputName());
        }
    }

    private List<ColumnWrapper> initColumns() {
        AtomicInteger position = new AtomicInteger();
        List<ColumnWrapper> collect =
                inputColumns.stream()
                        .map(
                                column -> {
                                    int origPos = position.getAndIncrement();
                                    StructEvolutionConfig.Column.ColumnBuilder builder =
                                            StructEvolutionConfig.Column.builder();
                                    builder.inputName(column.getName())
                                            .outputName(column.getName())
                                            .position(origPos)
                                            .dataType(column.getDataType().getSqlType())
                                            .length(column.getColumnLength())
                                            .scale(column.getScale())
                                            .nullable(column.isNullable())
                                            .outputType(column.getSinkType())
                                            .defaultValue(column.getDefaultValue())
                                            .comment(column.getComment());

                                    StructEvolutionConfig.Column build = builder.build();
                                    ColumnWrapper columnWrapper = new ColumnWrapper();
                                    columnWrapper.setColumn(build);
                                    columnWrapper.setDataType(column.getDataType());
                                    columnWrapper.setOriginalPosition(origPos);
                                    return columnWrapper;
                                })
                        .collect(Collectors.toList());

        for (StructEvolutionConfig.Column conditionColumn : specificModified.getColumns()) {
            switch (conditionColumn.getAction()) {
                case ADD:
                    collect.add(
                            ColumnWrapper.of(
                                    StructEvolutionConfig.Column.builder()
                                            .inputName(conditionColumn.getInputName())
                                            .outputName(conditionColumn.getOutputName())
                                            .position(position.getAndIncrement())
                                            .dataType(conditionColumn.getDataType())
                                            .length(conditionColumn.getLength())
                                            .scale(conditionColumn.getScale())
                                            .nullable(conditionColumn.isNullable())
                                            .defaultValue(conditionColumn.getDefaultValue())
                                            .comment(conditionColumn.getComment())
                                            .build()));
                    break;
                case MODIFY:
                    collect.stream()
                            .filter(
                                    cw ->
                                            cw.getColumn()
                                                    .getInputName()
                                                    .equals(conditionColumn.getInputName()))
                            .forEach(
                                    cw -> {
                                        StructEvolutionConfig.Column col = cw.getColumn();
                                        if (conditionColumn.getDataType() != col.getDataType()) {
                                            cw.setTypeChanged(true);
                                        }
                                        col.setPosition(conditionColumn.getPosition());
                                        col.setOutputName(conditionColumn.getOutputName());
                                        col.setDataType(conditionColumn.getDataType());
                                        col.setLength(conditionColumn.getLength());
                                        col.setScale(conditionColumn.getScale());
                                        col.setNullable(conditionColumn.isNullable());
                                        col.setDefaultValue(conditionColumn.getDefaultValue());
                                        col.setComment(conditionColumn.getComment());
                                    });
                    break;
                case DROP:
                    collect.removeIf(
                            cw ->
                                    cw.getColumn()
                                            .getInputName()
                                            .equals(conditionColumn.getInputName()));
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported action: " + conditionColumn.getAction());
            }
        }

        List<String> columnNames =
                collect.stream()
                        .map(cw -> cw.getColumn().getOutputName())
                        .collect(Collectors.toList());
        List<String> duplicateColumnNames =
                columnNames.stream()
                        .filter(name -> columnNames.indexOf(name) != columnNames.lastIndexOf(name))
                        .distinct()
                        .collect(Collectors.toList());
        if (!duplicateColumnNames.isEmpty()) {
            throw new IllegalArgumentException(
                    "Duplicate column names found: " + String.join(", ", duplicateColumnNames));
        }

        collect.sort(Comparator.comparingInt(cw -> cw.getColumn().getPosition()));

        Map<Integer, Integer> positionMapping = new HashMap<>();
        for (int newPos = 0; newPos < collect.size(); newPos++) {
            ColumnWrapper cw = collect.get(newPos);
            Integer origPos = cw.getOriginalPosition();
            if (origPos != null) {
                positionMapping.put(origPos, newPos);
            }
        }

        this.positionMapping = positionMapping;

        return collect;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        Object[] fields = inputRow.getFields();
        Object[] newFields = new Object[fields.length];
        positionMapping.forEach(
                (origPos, newPos) -> {
                    newFields[newPos] = fields[origPos];
                });
        return newFields;
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return outputTableIdentifier;
    }

    @Override
    protected Column[] getOutputColumns() {
        if (specificModified.getColumns() == null || specificModified.getColumns().isEmpty()) {
            return inputColumns.toArray(new Column[0]);
        }
        return outputColumns.stream()
                .map(
                        columnWrapper -> {
                            StructEvolutionConfig.Column column = columnWrapper.getColumn();
                            SeaTunnelDataType<?> dataType = columnWrapper.getDataType();
                            return PhysicalColumn.of(
                                    column.getOutputName(),
                                    dataType != null
                                            ? dataType
                                            : convertSqlTypeToSeaTunnelDataType(
                                                    column.getDataType()),
                                    column.getLength(),
                                    column.getScale(),
                                    column.isNullable(),
                                    column.getDefaultValue(),
                                    column.getComment(),
                                    column.getOutputType());
                        })
                .toArray(Column[]::new);
    }

    @Data
    private static class ColumnWrapper {
        private StructEvolutionConfig.Column column;
        private SeaTunnelDataType<?> dataType;
        private boolean isTypeChanged = false;
        private Integer originalPosition;

        public static ColumnWrapper of(StructEvolutionConfig.Column column) {
            ColumnWrapper columnWrapper = new ColumnWrapper();
            columnWrapper.setColumn(column);
            return columnWrapper;
        }
    }

    @Override
    protected List<ConstraintKey> getOutputConstraintKey() {
        if (specificModified.getIndexes() == null || specificModified.getIndexes().isEmpty()) {
            return super.getOutputConstraintKey();
        }
        List<ConstraintKey> addedConstraintKeys =
                specificModified.getIndexes().stream()
                        .filter(
                                constraint ->
                                        (constraint.getAction()
                                                == StructEvolutionConfig.Action.ADD))
                        .map(StructEvolutionConfig.Index::copy)
                        .map(
                                index ->
                                        ConstraintKey.of(
                                                index.isUnique()
                                                        ? ConstraintKey.ConstraintType.UNIQUE_KEY
                                                        : ConstraintKey.ConstraintType.INDEX_KEY,
                                                index.getName(),
                                                index.getColumns().stream()
                                                        .map(
                                                                StructEvolutionConfig
                                                                                .ReferenceColumn
                                                                        ::toConstraintKeyColumn)
                                                        .collect(Collectors.toList())))
                        .collect(Collectors.toList());

        List<String> dropConstraintKeys =
                specificModified.getIndexes().stream()
                        .filter(
                                constraint ->
                                        (constraint.getAction()
                                                == StructEvolutionConfig.Action.DROP))
                        .map(StructEvolutionConfig.Index::copy)
                        .map(StructEvolutionConfig.Index::getName)
                        .collect(Collectors.toList());

        List<ConstraintKey> modifyConstraintKeys =
                specificModified.getIndexes().stream()
                        .filter(
                                constraint ->
                                        (constraint.getAction()
                                                == StructEvolutionConfig.Action.MODIFY))
                        .map(StructEvolutionConfig.Index::copy)
                        .map(
                                index ->
                                        ConstraintKey.of(
                                                index.isUnique()
                                                        ? ConstraintKey.ConstraintType.UNIQUE_KEY
                                                        : ConstraintKey.ConstraintType.INDEX_KEY,
                                                index.getName(),
                                                index.getColumns().stream()
                                                        .map(
                                                                StructEvolutionConfig
                                                                                .ReferenceColumn
                                                                        ::toConstraintKeyColumn)
                                                        .collect(Collectors.toList())))
                        .collect(Collectors.toList());

        return mergeConstraintKeys(
                inputCatalogTable.getTableSchema().getConstraintKeys(),
                addedConstraintKeys,
                dropConstraintKeys,
                modifyConstraintKeys);
    }

    private List<ConstraintKey> mergeConstraintKeys(
            List<ConstraintKey> constraintKeys,
            List<ConstraintKey> addedConstraintKeys,
            List<String> dropConstraintKeys,
            List<ConstraintKey> modifyConstraintKeys) {
        List<ConstraintKey> mergedConstraintKeys =
                constraintKeys.stream()
                        .filter(
                                constraintKey ->
                                        !dropConstraintKeys.contains(
                                                constraintKey.getConstraintName()))
                        .collect(Collectors.toList());

        mergedConstraintKeys.addAll(addedConstraintKeys);
        for (ConstraintKey modifyConstraintKey : modifyConstraintKeys) {
            mergedConstraintKeys.removeIf(
                    constraintKey ->
                            constraintKey
                                    .getConstraintName()
                                    .equals(modifyConstraintKey.getConstraintName()));
            mergedConstraintKeys.add(modifyConstraintKey);
        }
        return mergedConstraintKeys;
    }

    @Override
    protected PrimaryKey getOutputPrimaryKey() {
        if (specificModified.getPrimaryKey() == null) {
            return super.getOutputPrimaryKey();
        }
        switch (specificModified.getPrimaryKey().getAction()) {
                // Adding modify operations to a single object is for the diff operations that may
                // be added later
            case MODIFY:
            case ADD:
                return PrimaryKey.of(
                        specificModified.getPrimaryKey().getOutputName(),
                        specificModified.getPrimaryKey().getColumns().stream()
                                .map(StructEvolutionConfig.ReferenceColumn::getReferenceName)
                                .collect(Collectors.toList()));
            case DROP:
                return null;
            default:
                throw new IllegalArgumentException(
                        "Unsupported action: " + specificModified.getPrimaryKey().getAction());
        }
    }

    @Override
    protected String transformComment() {
        if (specificModified.getColumns() == null) {
            return inputCatalogTable.getComment();
        }
        switch (specificModified.getComment().getAction()) {
            case MODIFY:
            case ADD:
                return specificModified.getComment().getContent();
            case DROP:
                return null;
            default:
                throw new IllegalArgumentException(
                        "Unsupported action: " + specificModified.getComment().getAction());
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }
}
