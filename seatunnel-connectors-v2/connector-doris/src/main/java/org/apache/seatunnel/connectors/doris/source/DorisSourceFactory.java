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

package org.apache.seatunnel.connectors.doris.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalog;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalogFactory;
import org.apache.seatunnel.connectors.doris.config.DorisOptions;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@AutoService(Factory.class)
public class DorisSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Doris";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DorisOptions.FENODES,
                        DorisOptions.USERNAME,
                        DorisOptions.PASSWORD,
                        DorisOptions.DATABASE,
                        DorisOptions.TABLE)
                .optional(DorisOptions.DORIS_FILTER_QUERY)
                .optional(DorisOptions.DORIS_READ_FIELD)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ReadonlyConfig options = context.getOptions();
        CatalogTable table;
        DorisCatalogFactory dorisCatalogFactory = new DorisCatalogFactory();
        DorisCatalog catalog = (DorisCatalog) dorisCatalogFactory.createCatalog("doris", options);
        catalog.open();
        String tableIdentifier =
                options.get(DorisOptions.DATABASE) + "." + options.get(DorisOptions.TABLE);
        TablePath tablePath = TablePath.of(tableIdentifier);
        table = catalog.getTable(tablePath);
        try {
            String read_fields = options.get(DorisOptions.DORIS_READ_FIELD);
            if (StringUtils.isNotBlank(read_fields)) {
                List<String> readFiledList =
                        Arrays.stream(read_fields.split(","))
                                .map(String::trim)
                                .collect(Collectors.toList());
                List<Column> tableColumns = table.getTableSchema().getColumns();
                Map<String, Column> tableColumnsMap =
                        tableColumns.stream()
                                .collect(Collectors.toMap(Column::getName, column -> column));
                List<String> matchingFieldNames =
                        getMatchingFieldNames(readFiledList, tableColumnsMap);

                table =
                        reconstructCatalogTable(
                                matchingFieldNames, tableColumnsMap, table, tablePath);
            }
        } catch (Exception e) {
            log.error("create source error");
            throw e;
        }
        CatalogTable finalTable = table;
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new DorisSource(options, finalTable);
    }

    private static CatalogTable reconstructCatalogTable(
            List<String> matchingFieldNames,
            Map<String, Column> tableColumnsMap,
            CatalogTable table,
            TablePath tablePath) {
        // reconstruct CatalogTable
        TableSchema.Builder builder = TableSchema.builder();
        for (String field : matchingFieldNames) {
            Column column = tableColumnsMap.get(field);
            builder.column(column);
        }
        if (table.getTableSchema().getPrimaryKey() != null) {
            List<String> columns =
                    table.getTableSchema().getPrimaryKey().getColumnNames().stream()
                            .filter(matchingFieldNames::contains)
                            .collect(Collectors.toList());
            if (!columns.isEmpty()) {
                builder.primaryKey(
                        new PrimaryKey(
                                table.getTableSchema().getPrimaryKey().getPrimaryKey(), columns));
            }
        }

        if (table.getTableSchema().getConstraintKeys() != null) {
            List<ConstraintKey> keys =
                    table.getTableSchema().getConstraintKeys().stream()
                            .filter(
                                    k ->
                                            k.getColumnNames().stream()
                                                    .map(
                                                            ConstraintKey.ConstraintKeyColumn
                                                                    ::getColumnName)
                                                    .allMatch(matchingFieldNames::contains))
                            .collect(Collectors.toList());
            if (!keys.isEmpty()) {
                builder.constraintKey(keys);
            }
        }

        table =
                CatalogTable.of(
                        TableIdentifier.of(
                                "Doris", tablePath.getDatabaseName(), tablePath.getTableName()),
                        builder.build(),
                        table.getOptions(),
                        Collections.emptyList(),
                        StringUtils.EMPTY);
        return table;
    }

    public static List<String> getMatchingFieldNames(
            List<String> readFieldList, Map<String, Column> tableColumnsMap) {
        List<String> matchingFieldNames =
                readFieldList.stream()
                        .filter(tableColumnsMap::containsKey)
                        .collect(Collectors.toList());

        if (matchingFieldNames.size() != readFieldList.size()) {
            List<String> nonMatchingFields =
                    readFieldList.stream()
                            .filter(field -> !matchingFieldNames.contains(field))
                            .collect(Collectors.toList());
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.SCHEMA_FAILED,
                    "The following fields are not present in the table columns: "
                            + nonMatchingFields);
        }

        return matchingFieldNames;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return DorisSource.class;
    }
}
