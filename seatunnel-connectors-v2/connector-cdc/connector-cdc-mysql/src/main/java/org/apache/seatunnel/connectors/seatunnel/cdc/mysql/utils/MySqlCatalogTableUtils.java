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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for building SeaTunnel catalog metadata from MySQL Debezium metadata.
 *
 * <p>These helpers are used when table metadata comes from binlog schema records instead of the
 * startup catalog discovery phase.
 */
public class MySqlCatalogTableUtils {

    /**
     * Converts a Debezium MySQL table change into a SeaTunnel {@link CatalogTable}. It is used when
     * a table appears in binlog after the source reader has already been initialized.
     */
    public static CatalogTable toCatalogTable(
            Table table, MySqlConnectorConfig dbzConnectorConfig) {
        TableId tableId = table.id();
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        List<Column> columns =
                table.columns().stream()
                        .map(
                                column ->
                                        MySqlTypeUtils.convertToSeaTunnelColumn(
                                                column, dbzConnectorConfig))
                        .collect(Collectors.toList());
        schemaBuilder.columns(columns);
        if (!table.primaryKeyColumnNames().isEmpty()) {
            schemaBuilder.primaryKey(
                    PrimaryKey.of(
                            "pk_" + (tableId.toString().hashCode() & Integer.MAX_VALUE),
                            table.primaryKeyColumnNames()));
        }
        return CatalogTable.of(
                TableIdentifier.of(
                        DatabaseIdentifier.MYSQL,
                        tableId.catalog(),
                        tableId.schema(),
                        tableId.table()),
                schemaBuilder.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private MySqlCatalogTableUtils() {}
}
