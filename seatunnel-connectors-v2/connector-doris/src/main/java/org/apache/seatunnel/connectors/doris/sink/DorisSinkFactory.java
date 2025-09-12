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

package org.apache.seatunnel.connectors.doris.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.doris.config.DorisSinkOptions;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitInfo;
import org.apache.seatunnel.connectors.doris.sink.writer.DorisSinkState;
import org.apache.seatunnel.connectors.doris.util.UnsupportedTypeConverterUtils;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import java.util.Arrays;
import java.util.List;

import static org.apache.seatunnel.connectors.doris.config.DorisBaseOptions.DATABASE;
import static org.apache.seatunnel.connectors.doris.config.DorisBaseOptions.TABLE;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.NEEDS_UNSUPPORTED_TYPE_CASTING;

@AutoService(Factory.class)
public class DorisSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return DorisSinkOptions.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DorisSinkOptions.FENODES,
                        DorisSinkOptions.USERNAME,
                        DorisSinkOptions.PASSWORD,
                        DorisSinkOptions.SINK_LABEL_PREFIX,
                        DorisSinkOptions.DORIS_SINK_CONFIG_PREFIX,
                        DorisSinkOptions.DATA_SAVE_MODE,
                        DorisSinkOptions.SCHEMA_SAVE_MODE)
                .optional(
                        DorisSinkOptions.DATABASE,
                        DorisSinkOptions.TABLE,
                        DorisSinkOptions.TABLE_IDENTIFIER,
                        DorisSinkOptions.QUERY_PORT,
                        DorisSinkOptions.DORIS_BATCH_SIZE,
                        DorisSinkOptions.SINK_ENABLE_2PC,
                        DorisSinkOptions.SINK_ENABLE_DELETE,
                        DorisSinkOptions.SAVE_MODE_CREATE_TEMPLATE,
                        DorisSinkOptions.NEEDS_UNSUPPORTED_TYPE_CASTING,
                        DorisSinkOptions.SINK_CHECK_INTERVAL,
                        DorisSinkOptions.SINK_MAX_RETRIES,
                        DorisSinkOptions.SINK_BUFFER_SIZE,
                        DorisSinkOptions.SINK_BUFFER_COUNT,
                        DorisSinkOptions.DEFAULT_DATABASE,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .conditional(
                        DorisSinkOptions.DATA_SAVE_MODE,
                        DataSaveMode.CUSTOM_PROCESSING,
                        DorisSinkOptions.CUSTOM_SQL)
                .build();
    }

    @Override
    public List<String> excludeTablePlaceholderReplaceKeys() {
        return Arrays.asList(DorisSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key());
    }

    @Override
    public TableSink<SeaTunnelRow, DorisSinkState, DorisCommitInfo, DorisCommitInfo> createSink(
            TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable =
                config.get(NEEDS_UNSUPPORTED_TYPE_CASTING)
                        ? UnsupportedTypeConverterUtils.convertCatalogTable(
                                context.getCatalogTable())
                        : context.getCatalogTable();
        final CatalogTable finalCatalogTable = this.renameCatalogTable(config, catalogTable);
        return () -> new DorisSink(config, finalCatalogTable);
    }

    private CatalogTable renameCatalogTable(ReadonlyConfig options, CatalogTable catalogTable) {
        TableIdentifier tableId = catalogTable.getTableId();
        String tableName;
        String databaseName;
        String tableIdentifier = options.get(DorisSinkOptions.TABLE_IDENTIFIER);
        if (StringUtils.isNotEmpty(tableIdentifier)) {
            tableName = tableIdentifier.split("\\.")[1];
            databaseName = tableIdentifier.split("\\.")[0];
        } else {
            if (StringUtils.isNotEmpty(options.get(TABLE))) {
                tableName = options.get(TABLE);
            } else {
                tableName = tableId.getTableName();
            }
            if (StringUtils.isNotEmpty(options.get(DATABASE))) {
                databaseName = options.get(DATABASE);
            } else {
                databaseName = tableId.getDatabaseName();
            }
        }
        TableIdentifier newTableId =
                TableIdentifier.of(tableId.getCatalogName(), databaseName, null, tableName);
        return CatalogTable.of(newTableId, catalogTable);
    }
}
