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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.catalog;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PreviewResult;
import org.apache.seatunnel.api.table.catalog.SQLPreviewResult;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.datatype.MaxComputeTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeUtil;

import org.apache.commons.lang3.StringUtils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Projects;
import com.aliyun.odps.Table;
import com.aliyun.odps.Tables;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.type.TypeInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ACCESS_ID;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PARTITION_SPEC;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.SAVE_MODE_CREATE_TEMPLATE;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class MaxComputeCatalog implements Catalog {

    private final ReadonlyConfig readonlyConfig;
    private final String catalogName;

    private Account account;

    public MaxComputeCatalog(String catalogName, ReadonlyConfig options) {
        this.readonlyConfig = options;
        this.catalogName = catalogName;
    }

    @Override
    public void open() throws CatalogException {
        account = new AliyunAccount(readonlyConfig.get(ACCESS_ID), readonlyConfig.get(ACCESS_KEY));
    }

    @Override
    public void close() throws CatalogException {}

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return readonlyConfig.get(PROJECT);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            Odps odps = getOdps(readonlyConfig.get(PROJECT));
            Projects projects = odps.projects();
            return projects.exists(databaseName);
        } catch (OdpsException e) {
            throw new CatalogException("Check " + databaseName + " exist error", e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            // todo: how to get all projects
            String project = readonlyConfig.get(PROJECT);
            if (databaseExists(project)) {
                return Lists.newArrayList(project);
            }
            return Collections.emptyList();
        } catch (Exception e) {
            throw new CatalogException("listDatabases exist error", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        Odps odps = getOdps(databaseName);

        Tables tables = odps.tables();
        List<String> tableNames = new ArrayList<>();
        tables.forEach(
                table -> {
                    tableNames.add(table.getName());
                });
        return tableNames;
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            Odps odps = getOdps(tablePath.getDatabaseName());
            com.aliyun.odps.Tables tables = odps.tables();
            return tables.exists(tablePath.getTableName());
        } catch (OdpsException e) {
            throw new CatalogException("tableExists" + tablePath + " error", e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        Table odpsTable;
        com.aliyun.odps.TableSchema odpsSchema;
        boolean isPartitioned;
        try {
            Odps odps = getOdps(tablePath.getDatabaseName());
            odpsTable =
                    MaxcomputeUtil.parseTable(
                            odps, tablePath.getDatabaseName(), tablePath.getTableName());
            odpsSchema = odpsTable.getSchema();
            isPartitioned = odpsTable.isPartitioned();
        } catch (Exception ex) {
            throw new CatalogException(catalogName, ex);
        }
        List<String> partitionKeys = new ArrayList<>();
        TableSchema.Builder builder = TableSchema.builder();
        buildColumnsWithErrorCheck(
                tablePath,
                builder,
                odpsSchema.getColumns().iterator(),
                (column) -> {
                    BasicTypeDefine<TypeInfo> typeDefine =
                            BasicTypeDefine.<TypeInfo>builder()
                                    .name(column.getName())
                                    .nativeType(column.getTypeInfo())
                                    .columnType(column.getTypeInfo().getTypeName())
                                    .dataType(column.getTypeInfo().getTypeName())
                                    .nullable(column.isNullable())
                                    .comment(column.getComment())
                                    .build();
                    return MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
                });
        if (isPartitioned) {
            buildColumnsWithErrorCheck(
                    tablePath,
                    builder,
                    odpsSchema.getPartitionColumns().iterator(),
                    (column) -> {
                        BasicTypeDefine<TypeInfo> typeDefine =
                                BasicTypeDefine.<TypeInfo>builder()
                                        .name(column.getName())
                                        .nativeType(column.getTypeInfo())
                                        .columnType(column.getTypeInfo().getTypeName())
                                        .dataType(column.getTypeInfo().getTypeName())
                                        .nullable(column.isNullable())
                                        .comment(column.getComment())
                                        .build();
                        partitionKeys.add(column.getName());
                        return MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
                    });
        }
        TableSchema tableSchema = builder.build();
        TableIdentifier tableIdentifier = getTableIdentifier(tablePath);
        return CatalogTable.of(
                tableIdentifier,
                tableSchema,
                readonlyConfig.toMap(),
                partitionKeys,
                odpsTable.getComment(),
                catalogName);
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        try {
            Odps odps = getOdps(tablePath.getDatabaseName());
            SQLTask.run(
                            odps,
                            MaxComputeCatalogUtil.getCreateTableStatement(
                                    readonlyConfig.get(SAVE_MODE_CREATE_TEMPLATE),
                                    tablePath,
                                    table))
                    .waitForSuccess();
        } catch (OdpsException e) {
            throw new CatalogException("create table error", e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            Odps odps = getOdps(tablePath.getDatabaseName());
            SQLTask.run(odps, MaxComputeCatalogUtil.getDropTableQuery(tablePath, ignoreIfNotExists))
                    .waitForSuccess();
        } catch (OdpsException e) {
            throw new CatalogException("drop table error", e);
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

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            Odps odps = getOdps(tablePath.getDatabaseName());
            Table odpsTable = odps.tables().get(tablePath.getTableName());
            if (odpsTable.isPartitioned()
                    && StringUtils.isNotEmpty(readonlyConfig.get(PARTITION_SPEC))) {
                PartitionSpec partitionSpec = new PartitionSpec(readonlyConfig.get(PARTITION_SPEC));
                odpsTable.deletePartition(partitionSpec, ignoreIfNotExists);
                odpsTable.createPartition(partitionSpec, true);
            } else {
                odpsTable.truncate();
            }
        } catch (Exception e) {
            throw new CatalogException("truncate table error", e);
        }
    }

    public void createPartition(TablePath tablePath, PartitionSpec partitionSpec) {
        try {
            Odps odps = getOdps(tablePath.getDatabaseName());
            Table odpsTable = odps.tables().get(tablePath.getTableName());
            odpsTable.createPartition(partitionSpec, true);
        } catch (Exception e) {
            throw new CatalogException("create partition error", e);
        }
    }

    public void truncatePartition(TablePath tablePath, PartitionSpec partitionSpec) {
        try {
            Odps odps = getOdps(tablePath.getDatabaseName());
            Table odpsTable = odps.tables().get(tablePath.getTableName());
            odpsTable.deletePartition(partitionSpec, true);
            odpsTable.createPartition(partitionSpec, true);
        } catch (Exception e) {
            throw new CatalogException("create partition error", e);
        }
    }

    @Override
    public boolean isExistsData(TablePath tablePath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void executeSql(TablePath tablePath, String sql) {
        try {
            Odps odps = getOdps(tablePath.getDatabaseName());
            String[] sqls = sql.split(";");
            for (String s : sqls) {
                if (!s.trim().isEmpty()) {
                    if (!s.trim().endsWith(";")) {
                        s = s.trim() + ";";
                    }
                    SQLTask.run(odps, s).waitForSuccess();
                }
            }
        } catch (OdpsException e) {
            throw new CatalogException("execute sql error", e);
        }
    }

    @Override
    public PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
        if (actionType == ActionType.CREATE_TABLE) {
            checkArgument(catalogTable.isPresent(), "CatalogTable cannot be null");
            return new SQLPreviewResult(
                    MaxComputeCatalogUtil.getCreateTableStatement(
                            readonlyConfig.get(SAVE_MODE_CREATE_TEMPLATE),
                            tablePath,
                            catalogTable.get()));
        } else if (actionType == ActionType.DROP_TABLE) {
            return new SQLPreviewResult(MaxComputeCatalogUtil.getDropTableQuery(tablePath, true));
        } else {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        }
    }

    private Odps getOdps(String project) {
        Odps odps = new Odps(account);
        odps.setEndpoint(readonlyConfig.get(ENDPOINT));
        odps.setDefaultProject(project);
        return odps;
    }

    protected TableIdentifier getTableIdentifier(TablePath tablePath) {
        return TableIdentifier.of(
                catalogName,
                tablePath.getDatabaseName(),
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }
}
