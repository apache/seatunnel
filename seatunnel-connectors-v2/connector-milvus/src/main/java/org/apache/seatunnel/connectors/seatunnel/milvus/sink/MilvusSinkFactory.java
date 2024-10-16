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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class MilvusSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "Milvus";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(MilvusSinkConfig.URL, MilvusSinkConfig.TOKEN)
                .optional(
                        MilvusSinkConfig.ENABLE_UPSERT,
                        MilvusSinkConfig.ENABLE_DYNAMIC_FIELD,
                        MilvusSinkConfig.ENABLE_AUTO_ID,
                        MilvusSinkConfig.SCHEMA_SAVE_MODE,
                        MilvusSinkConfig.DATA_SAVE_MODE)
                .build();
    }

    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = renameCatalogTable(config, context.getCatalogTable());
        return () -> new MilvusSink(config, catalogTable);
    }

    private CatalogTable renameCatalogTable(
            ReadonlyConfig config, CatalogTable sourceCatalogTable) {
        TableIdentifier sourceTableId = sourceCatalogTable.getTableId();
        String databaseName;
        if (StringUtils.isNotEmpty(config.get(MilvusSinkConfig.DATABASE))) {
            databaseName = config.get(MilvusSinkConfig.DATABASE);
        } else {
            databaseName = sourceTableId.getDatabaseName();
        }

        TableIdentifier newTableId =
                TableIdentifier.of(
                        sourceTableId.getCatalogName(),
                        databaseName,
                        sourceTableId.getSchemaName(),
                        sourceTableId.getTableName());

        return CatalogTable.of(newTableId, sourceCatalogTable);
    }
}
