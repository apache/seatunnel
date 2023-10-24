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

package org.apache.seatunnel.connectors.seatunnel.kudu.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient.KuduTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.kudu.util.KuduUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.shaded.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.ADMIN_OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.ENABLE_KERBEROS;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.KERBEROS_KEYTAB;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.KERBEROS_KRB5_CONF;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.KERBEROS_PRINCIPAL;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.MASTER;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.TABLE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.WORKER_COUNT;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class KuduCatalog implements Catalog {

    private final CommonConfig config;

    private KuduClient kuduClient;

    private final String defaultDatabase = "default_database";

    private final String catalogName;

    public KuduCatalog(String catalogName, CommonConfig config) {
        this.config = config;
        this.catalogName = catalogName;
    }

    @Override
    public void open() throws CatalogException {
        kuduClient = KuduUtil.getKuduClient(config);
    }

    @Override
    public void close() throws CatalogException {
        try {
            kuduClient.close();
        } catch (KuduException e) {
            throw new CatalogException("Failed close kudu client", e);
        }
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return listDatabases().contains(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return Lists.newArrayList(getDefaultDatabase());
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        try {
            return kuduClient.getTablesList().getTablesList();
        } catch (KuduException e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", this.catalogName), e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkNotNull(tablePath);
        try {
            return kuduClient.tableExists(tablePath.getFullName());
        } catch (KuduException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        checkNotNull(tablePath);

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        String tableName = tablePath.getFullName();

        try {
            KuduTable kuduTable = kuduClient.openTable(tableName);
            TableSchema.Builder builder = TableSchema.builder();
            Schema schema = kuduTable.getSchema();
            kuduTable.getPartitionSchema();
            List<ColumnSchema> columnSchemaList = schema.getColumns();
            Optional<PrimaryKey> primaryKey = getPrimaryKey(schema.getPrimaryKeyColumns());
            for (int i = 0; i < columnSchemaList.size(); i++) {
                SeaTunnelDataType<?> type = KuduTypeMapper.mapping(columnSchemaList, i);
                builder.column(
                        PhysicalColumn.of(
                                columnSchemaList.get(i).getName(),
                                type,
                                columnSchemaList.get(i).getTypeSize(),
                                columnSchemaList.get(i).isNullable(),
                                columnSchemaList.get(i).getDefaultValue(),
                                columnSchemaList.get(i).getComment()));
            }

            primaryKey.ifPresent(builder::primaryKey);

            TableIdentifier tableIdentifier =
                    TableIdentifier.of(
                            catalogName, tablePath.getDatabaseName(), tablePath.getTableName());

            return CatalogTable.of(
                    tableIdentifier,
                    builder.build(),
                    buildConnectorOptions(tablePath),
                    Collections.emptyList(),
                    tableName);
        } catch (Exception e) {
            throw new CatalogException("An exception occurred while obtaining the table", e);
        }
    }

    private Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "kudu");
        options.put(TABLE_NAME.key(), tablePath.getFullName());
        options.put(MASTER.key(), config.getMasters());
        options.put(WORKER_COUNT.key(), config.getWorkerCount().toString());
        options.put(OPERATION_TIMEOUT.key(), config.getOperationTimeout().toString());
        options.put(ADMIN_OPERATION_TIMEOUT.key(), config.getAdminOperationTimeout().toString());
        if (config.getEnableKerberos()) {
            options.put(KERBEROS_PRINCIPAL.key(), config.getPrincipal());
            options.put(KERBEROS_KEYTAB.key(), config.getKeytab());
            if (StringUtils.isNotBlank(config.getKrb5conf())) {
                options.put(KERBEROS_KRB5_CONF.key(), config.getKrb5conf());
            }
        }
        options.put(ENABLE_KERBEROS.key(), config.getEnableKerberos().toString());
        return options;
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        String tableName = tablePath.getFullName();
        try {
            if (tableExists(tablePath)) {
                kuduClient.deleteTable(tableName);
            } else if (!ignoreIfNotExists) {
                throw new TableNotExistException(catalogName, tablePath);
            }
        } catch (KuduException e) {
            throw new CatalogException("Could not delete table " + tableName, e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    protected Optional<PrimaryKey> getPrimaryKey(List<ColumnSchema> columnSchemaList) {
        List<String> pkFields =
                columnSchemaList.stream().map(ColumnSchema::getName).collect(Collectors.toList());
        if (!pkFields.isEmpty()) {
            String pkName = "pk_" + String.join("_", pkFields);
            return Optional.of(PrimaryKey.of(pkName, pkFields));
        }
        return Optional.empty();
    }
}
