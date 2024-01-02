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

package org.apache.seatunnel.connectors.cdc.base.utils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class CatalogTableUtils {

    public static List<CatalogTable> mergeCatalogTableConfig(
            List<CatalogTable> tables,
            List<JdbcSourceTableConfig> tableConfigs,
            Function<String, TablePath> parser) {
        Map<TablePath, CatalogTable> catalogTableMap =
                tables.stream()
                        .collect(Collectors.toMap(t -> t.getTableId().toTablePath(), t -> t));
        for (JdbcSourceTableConfig catalogTableConfig : tableConfigs) {
            TablePath tablePath = parser.apply(catalogTableConfig.getTable());
            CatalogTable catalogTable = catalogTableMap.get(tablePath);
            if (catalogTable != null) {
                catalogTable = mergeCatalogTableConfig(catalogTable, catalogTableConfig);
                catalogTableMap.put(tablePath, catalogTable);
                log.info(
                        "Override primary key({}) for catalog table {}",
                        catalogTableConfig.getPrimaryKeys(),
                        catalogTableConfig.getTable());
            } else {
                log.warn(
                        "Table {} is not found in catalog tables, skip to merge config",
                        catalogTableConfig.getTable());
            }
        }
        return new ArrayList<>(catalogTableMap.values());
    }

    public static CatalogTable mergeCatalogTableConfig(
            final CatalogTable table, JdbcSourceTableConfig config) {
        List<String> columnNames =
                table.getTableSchema().getColumns().stream()
                        .map(c -> c.getName())
                        .collect(Collectors.toList());
        for (String pk : config.getPrimaryKeys()) {
            if (!columnNames.contains(pk)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Primary key(%s) is not in table(%s) columns(%s)",
                                pk, table.getTablePath(), columnNames));
            }
        }
        PrimaryKey primaryKeys =
                PrimaryKey.of(
                        "pk" + (config.getPrimaryKeys().hashCode() & Integer.MAX_VALUE),
                        config.getPrimaryKeys());
        List<Column> columns =
                table.getTableSchema().getColumns().stream()
                        .map(
                                column -> {
                                    if (config.getPrimaryKeys().contains(column.getName())
                                            && column.isNullable()) {
                                        log.warn(
                                                "Primary key({}) is nullable for catalog table {}",
                                                column.getName(),
                                                table.getTablePath());
                                        return PhysicalColumn.of(
                                                column.getName(),
                                                column.getDataType(),
                                                column.getColumnLength(),
                                                false,
                                                column.getDefaultValue(),
                                                column.getComment());
                                    }
                                    return column;
                                })
                        .collect(Collectors.toList());

        return CatalogTable.of(
                table.getTableId(),
                TableSchema.builder()
                        .primaryKey(primaryKeys)
                        .columns(columns)
                        .constraintKey(table.getTableSchema().getConstraintKeys())
                        .build(),
                table.getOptions(),
                table.getPartitionKeys(),
                table.getComment());
    }

    public static Table mergeCatalogTableConfig(Table debeziumTable, CatalogTable catalogTable) {
        PrimaryKey pk = catalogTable.getTableSchema().getPrimaryKey();
        if (pk != null) {
            debeziumTable = debeziumTable.edit().setPrimaryKeyNames(pk.getColumnNames()).create();
            log.info(
                    "Override primary key({}) for catalog table {}",
                    pk.getColumnNames(),
                    debeziumTable.id());
        }
        return debeziumTable;
    }

    public static Map<TableId, CatalogTable> convertTables(List<CatalogTable> catalogTables) {
        Map<TableId, CatalogTable> tableMap =
                catalogTables.stream()
                        .collect(
                                Collectors.toMap(
                                        e ->
                                                new TableId(
                                                        e.getTableId().getDatabaseName(),
                                                        e.getTableId().getSchemaName(),
                                                        e.getTableId().getTableName()),
                                        e -> e));
        return Collections.unmodifiableMap(tableMap);
    }
}
