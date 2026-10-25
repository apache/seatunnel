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
package org.apache.seatunnel.connectors.seatunnel.fluss.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.StartMode;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FlussSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ReadonlyConfig readonlyConfig;
    @Getter private final String database;
    @Getter private final String table;
    @Getter private final long pollTimeoutMs;
    @Getter private final StartMode startMode;
    private final CatalogTable catalogTable;
    @Getter private final RowType flussRowType;

    public FlussSourceConfig(ReadonlyConfig readonlyConfig) {
        this.readonlyConfig = readonlyConfig;
        this.database = readonlyConfig.get(FlussSourceOptions.DATABASE);
        this.table = readonlyConfig.get(FlussSourceOptions.TABLE);
        this.pollTimeoutMs = readonlyConfig.get(FlussSourceOptions.POLL_TIMEOUT_MS);
        this.startMode = readonlyConfig.get(FlussSourceOptions.START_MODE);
        TableInfo tableInfo = loadTableInfo();
        this.catalogTable = toCatalogTable(tableInfo);
        this.flussRowType = tableInfo.getRowType();
    }

    private TableInfo loadTableInfo() {
        try (FlussAdminClient adminClient =
                new FlussAdminClient(buildFlussConfig(), getTablePath().getFullName())) {
            return adminClient.getTableInfo(getTablePath());
        }
    }

    private CatalogTable toCatalogTable(TableInfo tableInfo) {
        if (tableInfo.isPartitioned()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Fluss source does not support partitioned tables yet: %s.%s",
                            database, table));
        }
        RowType rowType = tableInfo.getRowType();
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (DataField field : rowType.getFields()) {
            DataType fieldType = field.getType();
            schemaBuilder.column(
                    PhysicalColumn.of(
                            field.getName(),
                            FlussTypeConverter.toSeaTunnelType(field.getName(), fieldType),
                            FlussTypeConverter.columnLength(fieldType),
                            FlussTypeConverter.columnScale(fieldType),
                            fieldType.isNullable(),
                            null,
                            field.getDescription().orElse(null)));
        }
        if (tableInfo.hasPrimaryKey()) {
            schemaBuilder.primaryKey(PrimaryKey.of(table + "_pk", tableInfo.getPrimaryKeys()));
        }
        TableIdentifier tableIdentifier =
                TableIdentifier.of(FlussSourceOptions.CONNECTOR_IDENTITY, database, table);
        return CatalogTable.of(
                tableIdentifier,
                schemaBuilder.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                tableInfo.getComment().orElse(null));
    }

    public Configuration buildFlussConfig() {
        Configuration flussConfig = new Configuration();
        flussConfig.setString(
                FlussSourceOptions.BOOTSTRAP_SERVERS.key(),
                readonlyConfig.get(FlussSourceOptions.BOOTSTRAP_SERVERS));
        Optional<Map<String, String>> clientConfig =
                readonlyConfig.getOptional(FlussSourceOptions.CLIENT_CONFIG);
        clientConfig.ifPresent(m -> m.forEach(flussConfig::setString));
        return flussConfig;
    }

    public TablePath getTablePath() {
        return TablePath.of(database, table);
    }

    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }
}
