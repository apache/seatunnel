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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink;

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
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkOptions;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class IcebergSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "Iceberg";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        IcebergCommonOptions.KEY_CATALOG_NAME,
                        IcebergSinkOptions.KEY_NAMESPACE,
                        IcebergSinkOptions.KEY_TABLE,
                        IcebergSinkOptions.CATALOG_PROPS)
                .optional(
                        IcebergSinkOptions.TABLE_PROPS,
                        IcebergSinkOptions.HADOOP_PROPS,
                        IcebergSinkOptions.HADOOP_CONF_PATH_PROP,
                        IcebergSinkOptions.KEY_CASE_SENSITIVE,
                        IcebergSinkOptions.KERBEROS_PRINCIPAL,
                        IcebergSinkOptions.KERBEROS_KEYTAB_PATH,
                        IcebergSinkOptions.KRB5_PATH,
                        IcebergSinkOptions.WRITE_PROPS,
                        IcebergSinkOptions.SCHEMA_SAVE_MODE,
                        IcebergSinkOptions.DATA_SAVE_MODE,
                        IcebergSinkOptions.AUTO_CREATE_PROPS,
                        IcebergSinkOptions.TABLE_PRIMARY_KEYS,
                        IcebergSinkOptions.TABLE_DEFAULT_PARTITION_KEYS,
                        IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP,
                        IcebergSinkOptions.TABLE_SCHEMA_EVOLUTION_ENABLED_PROP,
                        IcebergSinkOptions.TABLES_DEFAULT_COMMIT_BRANCH,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .conditional(
                        IcebergSinkOptions.DATA_SAVE_MODE,
                        DataSaveMode.CUSTOM_PROCESSING,
                        IcebergSinkOptions.DATA_SAVE_MODE_CUSTOM_SQL)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable =
                renameCatalogTable(new IcebergSinkConfig(config), context.getCatalogTable());
        return () -> new IcebergSink(config, catalogTable);
    }

    private CatalogTable renameCatalogTable(
            IcebergSinkConfig sinkConfig, CatalogTable catalogTable) {
        TableIdentifier tableId = catalogTable.getTableId();
        String tableName;
        String namespace;
        if (StringUtils.isNotEmpty(sinkConfig.getTable())) {
            tableName = sinkConfig.getTable();
        } else {
            tableName = tableId.getTableName();
        }

        if (StringUtils.isNotEmpty(sinkConfig.getNamespace())) {
            namespace = sinkConfig.getNamespace();
        } else {
            namespace = tableId.getSchemaName();
        }

        TableIdentifier newTableId =
                TableIdentifier.of(
                        tableId.getCatalogName(), namespace, tableId.getSchemaName(), tableName);

        return CatalogTable.of(newTableId, catalogTable);
    }
}
