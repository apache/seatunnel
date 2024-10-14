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

package org.apache.seatunnel.connectors.seatunnel.timeplus.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.timeplus.config.ReaderOption;
import org.apache.seatunnel.connectors.seatunnel.timeplus.state.TPAggCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.timeplus.state.TPCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.timeplus.state.TimeplusSinkState;

import com.google.auto.service.AutoService;

import java.util.Properties;

import static org.apache.seatunnel.api.sink.SinkCommonOptions.MULTI_TABLE_SINK_REPLICA;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_SCHEMA_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.BULK_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.DATA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.PRIMARY_KEY;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SAVE_MODE_CREATE_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SCHEMA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SHARDING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SPLIT_MODE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.SUPPORT_UPSERT;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.TIMEPLUS_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig.USERNAME;
import static org.icecream.IceCream.ic;

@AutoService(Factory.class)
public class TimeplusSinkFactory
        implements TableSinkFactory<
                SeaTunnelRow, TimeplusSinkState, TPCommitInfo, TPAggCommitInfo> {
    @Override
    public String factoryIdentifier() {
        return "Timeplus";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(TABLE)
                .optional(
                        HOST,
                        DATABASE,
                        TIMEPLUS_CONFIG,
                        BULK_SIZE,
                        SPLIT_MODE,
                        SHARDING_KEY,
                        PRIMARY_KEY,
                        SUPPORT_UPSERT,
                        SCHEMA_SAVE_MODE,
                        SAVE_MODE_CREATE_TEMPLATE,
                        DATA_SAVE_MODE,
                        MULTI_TABLE_SINK_REPLICA,
                        ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE)
                .bundled(USERNAME, PASSWORD)
                .build();
    }

    public static boolean isBlank(final String s) {
        if (s == null || s.isEmpty()) {
            return true;
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!Character.isWhitespace(c)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();

        String sinkTableName = config.get(TABLE);

        if (isBlank(sinkTableName)) {
            sinkTableName = catalogTable.getTableId().getTableName();
        }

        ic("sinkTableName", sinkTableName);

        // get source table relevant information
        TableIdentifier tableId = catalogTable.getTableId();
        String sourceDatabaseName = tableId.getDatabaseName();
        // String sourceSchemaName = tableId.getSchemaName();
        String sourceTableName = tableId.getTableName();
        // get sink table relevant information
        String sinkDatabaseName = config.get(DATABASE);

        ic("sourceTableName", sourceTableName);
        // to replace
        sinkDatabaseName =
                sinkDatabaseName.replace(
                        REPLACE_DATABASE_NAME_KEY,
                        sourceDatabaseName != null ? sourceDatabaseName : "");
        String finalTableName = this.replaceFullTableName(sinkTableName, tableId);
        ic("finalTableName", finalTableName);

        // rebuild TableIdentifier and catalogTable
        TableIdentifier newTableId =
                TableIdentifier.of(
                        tableId.getCatalogName(), sinkDatabaseName, null, finalTableName);
        catalogTable =
                CatalogTable.of(
                        newTableId,
                        catalogTable.getTableSchema(),
                        catalogTable.getOptions(),
                        catalogTable.getPartitionKeys(),
                        catalogTable.getCatalogName());

        Properties tpProperties = new Properties();
        if (config.getOptional(TIMEPLUS_CONFIG).isPresent()) {
            tpProperties.putAll(config.get(TIMEPLUS_CONFIG));
        }

        boolean supportUpsert = config.get(SUPPORT_UPSERT);
        boolean allowExperimentalLightweightDelete =
                config.get(ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE);
        ReaderOption readerOption =
                ReaderOption.builder()
                        .tableName(finalTableName)
                        .properties(tpProperties)
                        .bulkSize(config.get(BULK_SIZE))
                        .supportUpsert(supportUpsert)
                        .schemaSaveMode(config.get(SCHEMA_SAVE_MODE))
                        .dataSaveMode(config.get(DATA_SAVE_MODE))
                        .allowExperimentalLightweightDelete(allowExperimentalLightweightDelete)
                        .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                        .build();

        CatalogTable finalCatalogTable = catalogTable;
        ic("SAVE_MODE_CREATE_TEMPLATE", config.get(SAVE_MODE_CREATE_TEMPLATE));
        return () -> new TimeplusSink(finalCatalogTable, readerOption, config);
    }

    private String replaceFullTableName(String original, TableIdentifier tableId) {
        if (!isBlank(tableId.getDatabaseName())) {
            original = original.replace(REPLACE_DATABASE_NAME_KEY, tableId.getDatabaseName());
        }
        if (!isBlank(tableId.getSchemaName())) {
            original = original.replace(REPLACE_SCHEMA_NAME_KEY, tableId.getSchemaName());
        }
        if (!isBlank(tableId.getTableName())) {
            original = original.replace(REPLACE_TABLE_NAME_KEY, tableId.getTableName());
        }
        return original;
    }
}
