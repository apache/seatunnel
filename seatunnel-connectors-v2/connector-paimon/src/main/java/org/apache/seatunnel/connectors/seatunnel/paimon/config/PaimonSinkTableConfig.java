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

package org.apache.seatunnel.connectors.seatunnel.paimon.config;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.schema.ReadonlyConfigParser;

import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class PaimonSinkTableConfig implements Serializable {

    private final String database;
    private final String table;
    private final SchemaSaveMode schemaSaveMode;
    private final DataSaveMode dataSaveMode;
    private final String primaryKeys;
    private final String partitionKeys;
    private final Map<String, String> writeProps;
    private final CatalogTable catalogTable;

    private PaimonSinkTableConfig(
            String database,
            String table,
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            String primaryKeys,
            String partitionKeys,
            Map<String, String> writeProps,
            CatalogTable catalogTable) {
        this.database = database;
        this.table = table;
        this.schemaSaveMode = schemaSaveMode;
        this.dataSaveMode = dataSaveMode;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.writeProps = writeProps;
        this.catalogTable = catalogTable;
    }

    public static PaimonSinkTableConfig parsePaimonSinkConfig(ReadonlyConfig config) {
        String database = config.get(PaimonBaseOptions.DATABASE);
        String table = config.get(PaimonBaseOptions.TABLE);
        SchemaSaveMode schemaSaveMode = config.get(PaimonSinkOptions.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = config.get(PaimonSinkOptions.DATA_SAVE_MODE);
        String primaryKeys = config.getOptional(PaimonSinkOptions.PRIMARY_KEYS).orElse(null);
        String partitionKeys = config.getOptional(PaimonSinkOptions.PARTITION_KEYS).orElse(null);
        Map<String, String> writeProps = config.get(PaimonSinkOptions.WRITE_PROPS);
        
        TablePath tablePath = TablePath.of(database, table);
        TableSchema tableSchema = new ReadonlyConfigParser().parse(config);
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("", tablePath),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "");

        return new PaimonSinkTableConfig(
                database, table, schemaSaveMode, dataSaveMode, 
                primaryKeys, partitionKeys, writeProps, catalogTable);
    }

    public static List<PaimonSinkTableConfig> of(ReadonlyConfig config) {
        if (config.getOptional(PaimonSinkOptions.TABLE_LIST).isPresent()) {
            List<Map<String, Object>> maps = config.get(PaimonSinkOptions.TABLE_LIST);
            return maps.stream()
                    .map(ReadonlyConfig::fromMap)
                    .map(PaimonSinkTableConfig::parsePaimonSinkConfig)
                    .collect(Collectors.toList());
        }
        return Lists.newArrayList(parsePaimonSinkConfig(config));
    }

    public TablePath getTablePath() {
        return TablePath.of(database, table);
    }
}
