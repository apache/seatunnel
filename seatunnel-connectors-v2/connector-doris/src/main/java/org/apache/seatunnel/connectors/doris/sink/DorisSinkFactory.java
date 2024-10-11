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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.doris.config.DorisOptions;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitInfo;
import org.apache.seatunnel.connectors.doris.sink.writer.DorisSinkState;
import org.apache.seatunnel.connectors.doris.util.UnsupportedTypeConverterUtils;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import java.util.Arrays;
import java.util.List;

import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DATABASE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.NEEDS_UNSUPPORTED_TYPE_CASTING;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.TABLE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.TABLE_IDENTIFIER;

@AutoService(Factory.class)
public class DorisSinkFactory implements TableSinkFactory {

    public static final String IDENTIFIER = "Doris";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return DorisOptions.SINK_RULE.build();
    }

    @Override
    public List<String> excludeTablePlaceholderReplaceKeys() {
        return Arrays.asList(DorisOptions.SAVE_MODE_CREATE_TEMPLATE.key());
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
        String tableIdentifier = options.get(TABLE_IDENTIFIER);
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
