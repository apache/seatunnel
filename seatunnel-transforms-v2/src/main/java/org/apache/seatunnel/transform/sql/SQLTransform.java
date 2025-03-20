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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportFlatMapTransform;
import org.apache.seatunnel.transform.sql.SQLEngineFactory.EngineType;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.transform.sql.SQLEngineFactory.EngineType.ZETA;

@Slf4j
public class SQLTransform extends AbstractCatalogSupportFlatMapTransform {
    public static final String PLUGIN_NAME = "Sql";

    public static final Option<String> KEY_QUERY =
            Options.key("query").stringType().noDefaultValue().withDescription("The query SQL");

    public static final Option<String> KEY_ENGINE =
            Options.key("engine")
                    .stringType()
                    .defaultValue(ZETA.name())
                    .withDescription("The SQL engine type");

    private final String query;

    private final EngineType engineType;

    private SeaTunnelRowType outRowType;

    private transient SQLEngine sqlEngine;

    private final String inputTableName;

    public SQLTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.query = config.get(KEY_QUERY);
        if (config.getOptional(KEY_ENGINE).isPresent()) {
            this.engineType = EngineType.valueOf(config.get(KEY_ENGINE).toUpperCase());
        } else {
            this.engineType = ZETA;
        }

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
        sqlEngine = SQLEngineFactory.getSQLEngine(engineType);
        sqlEngine.init(
                inputTableName,
                inputCatalogTable.getTableId().getTableName(),
                inputCatalogTable.getSeaTunnelRowType(),
                query);
    }

    private void tryOpen() {
        if (sqlEngine == null) {
            open();
        }
    }

    @Override
    protected List<SeaTunnelRow> transformRow(SeaTunnelRow inputRow) {
        tryOpen();
        return sqlEngine.transformBySQL(inputRow, outRowType);
    }

    @Override
    protected TableSchema transformTableSchema() {
        tryOpen();
        List<String> inputColumnsMapping = new ArrayList<>();
        outRowType = sqlEngine.typeMapping(inputColumnsMapping);
        List<String> outputColumns = Arrays.asList(outRowType.getFieldNames());

        TableSchema.Builder builder = TableSchema.builder();
        if (inputCatalogTable.getTableSchema().getPrimaryKey() != null
                && outputColumns.containsAll(
                        inputCatalogTable.getTableSchema().getPrimaryKey().getColumnNames())) {
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
                                    return outputColumns.containsAll(constraintColumnNames);
                                })
                        .map(ConstraintKey::copy)
                        .collect(Collectors.toList());

        builder.constraintKey(outputConstraintKeys);

        String[] fieldNames = outRowType.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = outRowType.getFieldTypes();
        List<Column> columns = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            Column simpleColumn = null;
            String inputColumnName = inputColumnsMapping.get(i);
            if (inputColumnName != null) {
                for (Column inputColumn : inputCatalogTable.getTableSchema().getColumns()) {
                    if (inputColumnName.equals(inputColumn.getName())) {
                        simpleColumn = inputColumn;
                        break;
                    }
                }
            }
            Column column;
            if (simpleColumn != null) {
                column =
                        new PhysicalColumn(
                                fieldNames[i],
                                fieldTypes[i],
                                simpleColumn.getColumnLength(),
                                simpleColumn.getScale(),
                                simpleColumn.isNullable(),
                                simpleColumn.getDefaultValue(),
                                simpleColumn.getComment(),
                                simpleColumn.getSourceType(),
                                simpleColumn.getOptions());
            } else {
                column = PhysicalColumn.of(fieldNames[i], fieldTypes[i], 0, true, null, null);
            }
            columns.add(column);
        }
        return builder.columns(columns).build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    @Override
    public void close() {
        sqlEngine.close();
    }
}
