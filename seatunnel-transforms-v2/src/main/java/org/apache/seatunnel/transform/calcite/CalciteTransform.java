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

package org.apache.seatunnel.transform.calcite;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.AlterTableSchemaEventHandler;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.calcite.engine.CalciteSQLEngine;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportFlatMapTransform;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class CalciteTransform extends AbstractCatalogSupportFlatMapTransform {

    public static final String PLUGIN_NAME = "Calcite";

    private final String sql;
    private final String inputTableName;
    private transient CalciteSQLEngine engine;

    public CalciteTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable, config.get(TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION));
        this.sql = config.get(CalciteTransformConfig.SQL);
        List<String> pluginInputIdentifiers = config.get(ConnectorCommonOptions.PLUGIN_INPUT);
        if (pluginInputIdentifiers != null && !pluginInputIdentifiers.isEmpty()) {
            this.inputTableName = pluginInputIdentifiers.get(0);
        } else {
            this.inputTableName = catalogTable.getTableId().getTableName();
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void open() {
        engine = new CalciteSQLEngine(sql, inputTableName, inputCatalogTable.getSeaTunnelRowType());
        engine.init();
    }

    private void tryOpen() {
        if (engine == null) {
            open();
        }
    }

    @Override
    protected List<SeaTunnelRow> transformRow(SeaTunnelRow inputRow) {
        tryOpen();
        return engine.execute(inputRow);
    }

    @Override
    protected TableSchema transformTableSchema() {
        tryOpen();
        SeaTunnelRowType outRowType = engine.getOutputRowType();
        List<String> outputColumns = Arrays.asList(outRowType.getFieldNames());

        TableSchema.Builder builder = TableSchema.builder();
        if (inputCatalogTable.getTableSchema().getPrimaryKey() != null
                && new HashSet<>(outputColumns)
                        .containsAll(
                                inputCatalogTable
                                        .getTableSchema()
                                        .getPrimaryKey()
                                        .getColumnNames())) {
            builder.primaryKey(inputCatalogTable.getTableSchema().getPrimaryKey().copy());
        }

        List<ConstraintKey> outputConstraintKeys =
                inputCatalogTable.getTableSchema().getConstraintKeys().stream()
                        .filter(
                                key -> {
                                    List<String> constraintColumnNames =
                                            key.getColumnNames().stream()
                                                    .map(
                                                            ConstraintKey.ConstraintKeyColumn
                                                                    ::getColumnName)
                                                    .collect(Collectors.toList());
                                    return new HashSet<>(outputColumns)
                                            .containsAll(constraintColumnNames);
                                })
                        .map(ConstraintKey::copy)
                        .collect(Collectors.toList());
        builder.constraintKey(outputConstraintKeys);

        String[] fieldNames = outRowType.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = outRowType.getFieldTypes();
        List<Column> columns = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            Column inputColumn = findInputColumn(fieldNames[i]);
            Column column;
            if (inputColumn != null) {
                column =
                        new PhysicalColumn(
                                fieldNames[i],
                                fieldTypes[i],
                                inputColumn.getColumnLength(),
                                inputColumn.getScale(),
                                inputColumn.isNullable(),
                                inputColumn.getDefaultValue(),
                                inputColumn.getComment(),
                                inputColumn.getSourceType(),
                                inputColumn.getOptions());
            } else {
                column = PhysicalColumn.of(fieldNames[i], fieldTypes[i], 0, true, null, null);
            }
            columns.add(column);
        }
        return builder.columns(columns).build();
    }

    private Column findInputColumn(String name) {
        for (Column col : inputCatalogTable.getTableSchema().getColumns()) {
            if (col.getName().equalsIgnoreCase(name)) {
                return col;
            }
        }
        return null;
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
        if (event instanceof AlterTableEvent) {
            TableSchema newSchema =
                    new AlterTableSchemaEventHandler()
                            .reset(inputCatalogTable.getTableSchema())
                            .apply(event);
            inputCatalogTable =
                    CatalogTable.of(
                            inputCatalogTable.getTableId(),
                            newSchema,
                            inputCatalogTable.getOptions(),
                            inputCatalogTable.getPartitionKeys(),
                            inputCatalogTable.getComment(),
                            inputCatalogTable.getTableId().getCatalogName(),
                            inputCatalogTable.getMetadataSchema());
            closeEngine();
            outputCatalogTable = null;
        }
        return event;
    }

    @Override
    public void setInputCatalogTable(@NonNull CatalogTable inputCatalogTable) {
        super.setInputCatalogTable(inputCatalogTable);
        closeEngine();
    }

    private void closeEngine() {
        if (engine != null) {
            engine.close();
            engine = null;
        }
    }

    @Override
    public void close() {
        closeEngine();
    }
}
