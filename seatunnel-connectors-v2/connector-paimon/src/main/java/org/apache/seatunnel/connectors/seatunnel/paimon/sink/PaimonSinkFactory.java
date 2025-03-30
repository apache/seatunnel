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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalogEnum;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class PaimonSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "Paimon";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(PaimonConfig.WAREHOUSE, PaimonConfig.DATABASE, PaimonConfig.TABLE)
                .optional(
                        PaimonConfig.HDFS_SITE_PATH,
                        PaimonConfig.HADOOP_CONF,
                        PaimonConfig.HADOOP_CONF_PATH,
                        PaimonConfig.CATALOG_TYPE,
                        PaimonSinkConfig.SCHEMA_SAVE_MODE,
                        PaimonSinkConfig.DATA_SAVE_MODE,
                        PaimonSinkConfig.PRIMARY_KEYS,
                        PaimonSinkConfig.PARTITION_KEYS,
                        PaimonSinkConfig.WRITE_PROPS,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .conditional(
                        PaimonConfig.CATALOG_TYPE, PaimonCatalogEnum.HIVE, PaimonConfig.CATALOG_URI)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable =
                renameCatalogTable(new PaimonSinkConfig(readonlyConfig), context.getCatalogTable());
        return () -> new PaimonSink(context.getOptions(), catalogTable);
    }

    private CatalogTable renameCatalogTable(
            PaimonSinkConfig paimonSinkConfig, CatalogTable catalogTable) {
        TableIdentifier tableId = catalogTable.getTableId();
        String tableName;
        String namespace;
        if (StringUtils.isNotEmpty(paimonSinkConfig.getTable())) {
            tableName = paimonSinkConfig.getTable();
        } else {
            tableName = tableId.getTableName();
        }

        if (StringUtils.isNotEmpty(paimonSinkConfig.getNamespace())) {
            namespace = paimonSinkConfig.getNamespace();
        } else {
            namespace = tableId.getSchemaName();
        }

        TableIdentifier newTableId =
                TableIdentifier.of(
                        tableId.getCatalogName(), namespace, tableId.getSchemaName(), tableName);

        return CatalogTable.of(newTableId, catalogTable);
    }
}
