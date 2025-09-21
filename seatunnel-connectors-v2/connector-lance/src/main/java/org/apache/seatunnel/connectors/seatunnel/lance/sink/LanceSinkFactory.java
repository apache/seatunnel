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

package org.apache.seatunnel.connectors.seatunnel.lance.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceSinkOptions;

import com.google.auto.service.AutoService;
import org.apache.commons.lang3.StringUtils;

@AutoService(Factory.class)
public class LanceSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Lance";
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable =
            renameCatalogTable(new LanceSinkConfig(config), context.getCatalogTable());
        return () -> new LanceSink(config, catalogTable);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
            .required(
                LanceCommonOptions.KEY_DATASET_PATH,
                LanceCommonOptions.KEY_NAMESPACE_TYPE,
                LanceCommonOptions.KEY_NAMESPACE_IDS)
            .optional(
                LanceSinkOptions.WRITE_MAX_ROWS_PER_FILE,
                LanceSinkOptions.WRITE_MAX_ROWS_PER_GROUP,
                LanceSinkOptions.WRITE_MAX_BYTES_PER_FILE,
                LanceSinkOptions.WRITE_MODE,
                LanceSinkOptions.WRITE_ENABLE_STABLE_ROW_IDS,
                LanceSinkOptions.WRITE_STORAGE_OPTIONS)
            .build();
    }

    private CatalogTable renameCatalogTable(
        LanceSinkConfig sinkConfig, CatalogTable catalogTable) {
        TableIdentifier tableId = catalogTable.getTableId();
        String tableName;
        String namespace;
//        if (StringUtils.isNotEmpty(sinkConfig.get())) {
//            tableName = sinkConfig.getTable();
//        } else {
//            tableName = tableId.getTableName();
//        }
//
//        if (StringUtils.isNotEmpty(sinkConfig.getNamespace())) {
//            namespace = sinkConfig.getNamespace();
//        } else {
//            namespace = tableId.getSchemaName();
//        }

        TableIdentifier newTableId =
            TableIdentifier.of(
                tableId.getCatalogName(), namespace, tableId.getSchemaName(), tableName);

        return CatalogTable.of(newTableId, catalogTable);
    }
}
