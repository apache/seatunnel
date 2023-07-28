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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;
import org.apache.seatunnel.transform.sql.SQLEngineFactory.EngineType;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.transform.sql.SQLEngineFactory.EngineType.ZETA;

@Slf4j
@NoArgsConstructor
@AutoService(SeaTunnelTransform.class)
public class SQLTransform extends AbstractCatalogSupportTransform {
    public static final String PLUGIN_NAME = "Sql";

    public static final Option<String> KEY_QUERY =
            Options.key("query").stringType().noDefaultValue().withDescription("The query SQL");

    public static final Option<String> KEY_ENGINE =
            Options.key("engine")
                    .stringType()
                    .defaultValue(ZETA.name())
                    .withDescription("The SQL engine type");

    private String query;

    private EngineType engineType;

    private transient SQLEngine sqlEngine;

    public SQLTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.query = config.get(KEY_QUERY);
        if (config.getOptional(KEY_ENGINE).isPresent()) {
            this.engineType = EngineType.valueOf(config.get(KEY_ENGINE).toUpperCase());
        } else {
            this.engineType = ZETA;
        }

        List<String> sourceTableNames = config.get(CommonOptions.SOURCE_TABLE_NAME);
        if (sourceTableNames != null && !sourceTableNames.isEmpty()) {
            this.inputTableName = sourceTableNames.get(0);
        } else {
            this.inputTableName = catalogTable.getTableId().getTableName();
        }
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        String[] fieldNames = new String[columns.size()];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType<?>[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            fieldNames[i] = column.getName();
            fieldTypes[i] = column.getDataType();
        }
        this.inputRowType = new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        ConfigValidator.of(readonlyConfig).validate(new SQLTransformFactory().optionRule());
        this.query = readonlyConfig.get(KEY_QUERY);
        if (readonlyConfig.getOptional(KEY_ENGINE).isPresent()) {
            this.engineType = EngineType.valueOf(readonlyConfig.get(KEY_ENGINE).toUpperCase());
        } else {
            this.engineType = ZETA;
        }
    }

    @Override
    public void open() {
        sqlEngine = SQLEngineFactory.getSQLEngine(engineType);
        sqlEngine.init(inputTableName, inputRowType, query);
    }

    private void tryOpen() {
        if (sqlEngine == null) {
            open();
        }
    }

    @Override
    protected SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType) {
        tryOpen();
        return sqlEngine.typeMapping(null);
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        tryOpen();
        return sqlEngine.transformBySQL(inputRow);
    }

    @Override
    protected TableSchema transformTableSchema() {
        tryOpen();
        List<String> inputColumnsMapping = new ArrayList<>();
        SeaTunnelRowType outRowType = sqlEngine.typeMapping(inputColumnsMapping);

        TableSchema.Builder builder = TableSchema.builder();
        if (inputCatalogTable.getTableSchema().getPrimaryKey() != null) {
            List<String> outPkColumnNames = new ArrayList<>();
            for (String pkColumnName :
                    inputCatalogTable.getTableSchema().getPrimaryKey().getColumnNames()) {
                for (int i = 0; i < inputColumnsMapping.size(); i++) {
                    if (pkColumnName.equals(inputColumnsMapping.get(i))) {
                        outPkColumnNames.add(outRowType.getFieldName(i));
                    }
                }
            }
            if (!outPkColumnNames.isEmpty()) {
                builder.primaryKey(
                        PrimaryKey.of(
                                inputCatalogTable.getTableSchema().getPrimaryKey().getPrimaryKey(),
                                outPkColumnNames));
            }
        }
        if (inputCatalogTable.getTableSchema().getConstraintKeys() != null) {
            List<ConstraintKey> outConstraintKey = new ArrayList<>();
            for (ConstraintKey constraintKey :
                    inputCatalogTable.getTableSchema().getConstraintKeys()) {
                List<ConstraintKey.ConstraintKeyColumn> outConstraintColumnKeys = new ArrayList<>();
                for (ConstraintKey.ConstraintKeyColumn constraintKeyColumn :
                        constraintKey.getColumnNames()) {
                    String constraintColumnName = constraintKeyColumn.getColumnName();
                    for (int i = 0; i < inputColumnsMapping.size(); i++) {
                        if (constraintColumnName.equals(inputColumnsMapping.get(i))) {
                            outConstraintColumnKeys.add(
                                    ConstraintKey.ConstraintKeyColumn.of(
                                            outRowType.getFieldName(i),
                                            constraintKeyColumn.getSortType()));
                        }
                    }
                }
                if (!outConstraintColumnKeys.isEmpty()) {
                    outConstraintKey.add(
                            ConstraintKey.of(
                                    constraintKey.getConstraintType(),
                                    constraintKey.getConstraintName(),
                                    outConstraintColumnKeys));
                }
            }
            if (!outConstraintKey.isEmpty()) {
                builder.constraintKey(outConstraintKey);
            }
        }

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
                        PhysicalColumn.of(
                                fieldNames[i],
                                fieldTypes[i],
                                simpleColumn.getColumnLength(),
                                simpleColumn.isNullable(),
                                simpleColumn.getDefaultValue(),
                                simpleColumn.getComment());
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
