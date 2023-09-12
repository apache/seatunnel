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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_SCHEMA_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.AUTO_COMMIT;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.COMPATIBLE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.CUSTOM_SQL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.DATA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.DRIVER;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.GENERATE_SINK_SQL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.IS_EXACTLY_ONCE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.MAX_COMMIT_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.PRIMARY_KEYS;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.QUERY;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.SCHEMA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.SUPPORT_UPSERT_BY_QUERY_PRIMARY_KEY_EXIST;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.TRANSACTION_TIMEOUT_SEC;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.URL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.USER;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.XA_DATA_SOURCE_CLASS_NAME;

@AutoService(Factory.class)
public class JdbcSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Jdbc";
    }

    @Override
    public TableSink createSink(TableFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        Map<String, String> catalogOptions = config.get(CatalogOptions.CATALOG_OPTIONS);
        Optional<String> optionalTable = config.getOptional(TABLE);
        Optional<String> optionalDatabase = config.getOptional(DATABASE);
        if (!optionalTable.isPresent()) {
            optionalTable = Optional.of(REPLACE_TABLE_NAME_KEY);
        }
        if (!optionalDatabase.isPresent()) {
            optionalDatabase = Optional.of(REPLACE_DATABASE_NAME_KEY);
        }
        if (catalogOptions != null && !catalogOptions.isEmpty()) {
            // get source table relevant information
            TableIdentifier tableId = catalogTable.getTableId();
            String sourceDatabaseName = tableId.getDatabaseName();
            String sourceSchemaName = tableId.getSchemaName();
            String sourceTableName = tableId.getTableName();
            // get sink table relevant information
            String sinkDatabaseName = optionalDatabase.get();
            String sinkTableNameBefore = optionalTable.get();
            String[] sinkTableSplitArray = sinkTableNameBefore.split("\\.");
            String sinkTableName = sinkTableSplitArray[sinkTableSplitArray.length - 1];
            String sinkSchemaName;
            if (sinkTableSplitArray.length > 1) {
                sinkSchemaName = sinkTableSplitArray[sinkTableSplitArray.length - 2];
            } else {
                sinkSchemaName = null;
            }
            if (StringUtils.isNotBlank(catalogOptions.get(JdbcCatalogOptions.SCHEMA.key()))) {
                sinkSchemaName = catalogOptions.get(JdbcCatalogOptions.SCHEMA.key());
            }
            // to add tablePrefix and tableSuffix
            String tempTableName;
            String prefix = catalogOptions.get(JdbcCatalogOptions.TABLE_PREFIX.key());
            String suffix = catalogOptions.get(JdbcCatalogOptions.TABLE_SUFFIX.key());
            if (StringUtils.isNotEmpty(prefix) || StringUtils.isNotEmpty(suffix)) {
                tempTableName =
                        StringUtils.isNotEmpty(prefix) ? prefix + sinkTableName : sinkTableName;
                tempTableName =
                        StringUtils.isNotEmpty(suffix) ? tempTableName + suffix : tempTableName;

            } else {
                tempTableName = sinkTableName;
            }
            // to replace
            String finalDatabaseName =
                    sinkDatabaseName.replace(REPLACE_DATABASE_NAME_KEY, sourceDatabaseName);
            String finalSchemaName;
            if (sinkSchemaName != null) {
                if (sourceSchemaName == null) {
                    finalSchemaName = sinkSchemaName;
                } else {
                    finalSchemaName =
                            sinkSchemaName.replace(REPLACE_SCHEMA_NAME_KEY, sourceSchemaName);
                }
            } else {
                finalSchemaName = null;
            }
            String finalTableName = tempTableName.replace(REPLACE_TABLE_NAME_KEY, sourceTableName);
            // rebuild TableIdentifier and catalogTable
            TableIdentifier newTableId =
                    TableIdentifier.of(
                            tableId.getCatalogName(),
                            finalDatabaseName,
                            finalSchemaName,
                            finalTableName);
            catalogTable =
                    CatalogTable.of(
                            newTableId,
                            catalogTable.getTableSchema(),
                            catalogTable.getOptions(),
                            catalogTable.getPartitionKeys(),
                            catalogTable.getCatalogName());
            Map<String, String> map = config.toMap();
            if (catalogTable.getTableId().getSchemaName() != null) {
                map.put(
                        TABLE.key(),
                        catalogTable.getTableId().getSchemaName()
                                + "."
                                + catalogTable.getTableId().getTableName());
            } else {
                map.put(TABLE.key(), catalogTable.getTableId().getTableName());
            }
            map.put(DATABASE.key(), catalogTable.getTableId().getDatabaseName());
            PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
            if (primaryKey != null && !CollectionUtils.isEmpty(primaryKey.getColumnNames())) {
                map.put(PRIMARY_KEYS.key(), String.join(",", primaryKey.getColumnNames()));
            }
            config = ReadonlyConfig.fromMap(new HashMap<>(map));
        }
        // always execute
        final ReadonlyConfig options = config;
        JdbcSinkConfig sinkConfig = JdbcSinkConfig.of(config);
        String fieldIde =
                config.get(JdbcOptions.FIELD_IDE) == null
                        ? FieldIdeEnum.ORIGINAL.getValue()
                        : config.get(JdbcOptions.FIELD_IDE).getValue();
        catalogTable.getOptions().put("fieldIde", fieldIde);
        JdbcDialect dialect =
                JdbcDialectLoader.load(sinkConfig.getJdbcConnectionConfig().getUrl(), fieldIde);
        CatalogTable finalCatalogTable = catalogTable;
        // get saveMode
        DataSaveMode dataSaveMode = config.get(DATA_SAVE_MODE);
        SchemaSaveMode schemaSaveMode = config.get(SCHEMA_SAVE_MODE);
        return () ->
                new JdbcSink(
                        options,
                        sinkConfig,
                        dialect,
                        schemaSaveMode,
                        dataSaveMode,
                        finalCatalogTable);
    }

    // todo
    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, DRIVER, SCHEMA_SAVE_MODE, DATA_SAVE_MODE)
                .optional(
                        USER,
                        PASSWORD,
                        CONNECTION_CHECK_TIMEOUT_SEC,
                        BATCH_SIZE,
                        IS_EXACTLY_ONCE,
                        GENERATE_SINK_SQL,
                        AUTO_COMMIT,
                        SUPPORT_UPSERT_BY_QUERY_PRIMARY_KEY_EXIST,
                        PRIMARY_KEYS,
                        COMPATIBLE_MODE)
                .conditional(
                        IS_EXACTLY_ONCE,
                        true,
                        XA_DATA_SOURCE_CLASS_NAME,
                        MAX_COMMIT_ATTEMPTS,
                        TRANSACTION_TIMEOUT_SEC)
                .conditional(IS_EXACTLY_ONCE, false, MAX_RETRIES)
                .conditional(GENERATE_SINK_SQL, true, DATABASE)
                .conditional(GENERATE_SINK_SQL, false, QUERY)
                .conditional(DATA_SAVE_MODE, DataSaveMode.CUSTOM_PROCESSING, CUSTOM_SQL)
                .build();
    }
}
