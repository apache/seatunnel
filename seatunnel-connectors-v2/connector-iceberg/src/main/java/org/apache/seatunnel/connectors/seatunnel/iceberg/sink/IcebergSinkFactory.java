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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.HDFS_SITE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.HIVE_SITE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KERBEROS_KEYTAB_PATH;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KERBEROS_KRB5_CONF_PATH;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KERBEROS_PRINCIPAL;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_CATALOG_NAME;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_CATALOG_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_NAMESPACE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_TABLE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_URI;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_WAREHOUSE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HIVE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig.ENABLE_UPSERT;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig.PRIMARY_KEYS;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig.TARGET_FILE_SIZE_BYTES;

@AutoService(Factory.class)
@Slf4j
public class IcebergSinkFactory implements TableSinkFactory {

    public static final String REPLACE_TABLE_NAME_KEY = "${table_name}";

    public static final String REPLACE_SCHEMA_NAME_KEY = "${schema_name}";

    public static final String REPLACE_DATABASE_NAME_KEY = "${database_name}";

    @Override
    public String factoryIdentifier() {
        return "Iceberg";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(KEY_CATALOG_NAME, KEY_CATALOG_TYPE, KEY_WAREHOUSE)
                .conditional(KEY_CATALOG_TYPE, HIVE, KEY_URI)
                .optional(
                        KERBEROS_PRINCIPAL,
                        KERBEROS_KEYTAB_PATH,
                        KERBEROS_KRB5_CONF_PATH,
                        HDFS_SITE_PATH,
                        HIVE_SITE_PATH)
                .optional(KEY_NAMESPACE, KEY_TABLE)
                .optional(ENABLE_UPSERT)
                .optional(PRIMARY_KEYS)
                .optional(TARGET_FILE_SIZE_BYTES)
                .build();
    }

    @Override
    public TableSink createSink(TableFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        SinkConfig sinkConfig = new SinkConfig(config);
        CatalogTable catalogTable = renameCatalogTable(sinkConfig, context.getCatalogTable());
        return () -> new IcebergSink(catalogTable, config);
    }

    private CatalogTable renameCatalogTable(SinkConfig sinkConfig, CatalogTable catalogTable) {

        TableIdentifier tableId = catalogTable.getTableId();
        String tableName;
        String namespace;
        if (StringUtils.isNotEmpty(sinkConfig.getTable())) {
            tableName = replaceName(sinkConfig.getTable(), tableId);
        } else {
            tableName = tableId.getTableName();
        }

        if (StringUtils.isNotEmpty(sinkConfig.getNamespace())) {
            namespace = replaceName(sinkConfig.getNamespace(), tableId);
        } else {
            namespace = tableId.getSchemaName();
        }

        TableIdentifier newTableId =
                TableIdentifier.of(
                        tableId.getCatalogName(), namespace, tableId.getSchemaName(), tableName);

        return CatalogTable.of(newTableId, catalogTable);
    }

    private String replaceName(String original, TableIdentifier tableId) {
        if (tableId.getTableName() != null) {
            original = original.replace(REPLACE_TABLE_NAME_KEY, tableId.getTableName());
        }
        if (tableId.getSchemaName() != null) {
            original = original.replace(REPLACE_SCHEMA_NAME_KEY, tableId.getSchemaName());
        }
        if (tableId.getDatabaseName() != null) {
            original = original.replace(REPLACE_DATABASE_NAME_KEY, tableId.getDatabaseName());
        }
        return original;
    }
}
