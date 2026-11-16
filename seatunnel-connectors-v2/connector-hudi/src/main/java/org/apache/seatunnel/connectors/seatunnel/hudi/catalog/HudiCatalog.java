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

package org.apache.seatunnel.connectors.seatunnel.hudi.catalog;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiAuthenticationContext;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieCatalogException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkOptions.CDC_ENABLED;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkOptions.PRECOMBINE_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkOptions.RECORD_KEY_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkOptions.TABLE_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.sink.convert.AvroSchemaConverter.convertToSchema;
import static org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiCatalogUtil.inferTablePath;
import static org.apache.seatunnel.connectors.seatunnel.hudi.util.SchemaUtil.convertSeaTunnelType;

@Slf4j
public class HudiCatalog implements Catalog {

    private final String catalogName;
    private final org.apache.hadoop.conf.Configuration hadoopConf;
    private final String tableParentDfsPathStr;
    private final Path tableParentDfsPath;
    private FileSystem fs;
    private final String principal;
    private final String keytab;
    private final String krb5Path;
    // Owned between open/close. All *UnderIdentity methods require this context's execute scope.
    // Catalog operations and close must not run concurrently.
    private HudiAuthenticationContext authentication;

    public HudiCatalog(String catalogName, Configuration hadoopConf, String tableParentDfsPathStr) {
        this(catalogName, hadoopConf, tableParentDfsPathStr, null, null, null);
    }

    public HudiCatalog(
            String catalogName,
            Configuration hadoopConf,
            String tableParentDfsPathStr,
            String principal,
            String keytab,
            String krb5Path) {
        this.catalogName = catalogName;
        this.hadoopConf = hadoopConf;
        this.tableParentDfsPathStr = tableParentDfsPathStr;
        this.tableParentDfsPath = new Path(tableParentDfsPathStr);
        this.principal = principal;
        this.keytab = keytab;
        this.krb5Path = krb5Path;
    }

    @Override
    public void open() throws CatalogException {
        authentication = new HudiAuthenticationContext(hadoopConf, principal, keytab, krb5Path);
        try {
            execute(
                    "open",
                    null,
                    () -> {
                        openUnderIdentity();
                        return null;
                    });
        } catch (RuntimeException e) {
            try {
                close();
            } catch (RuntimeException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw e;
        }
    }

    private void openUnderIdentity() {
        fs = HadoopFSUtils.getFs(tableParentDfsPathStr, hadoopConf);
        try {
            if (!fs.exists(tableParentDfsPath)) {
                log.info("Table dfs path not exists, will be created");
                fs.mkdirs(tableParentDfsPath);
            }
        } catch (IOException e) {
            throw new CatalogException(
                    String.format(
                            "Checking catalog path %s exists exception.", tableParentDfsPathStr),
                    e);
        }
        if (!databaseExistsUnderIdentity(getDefaultDatabase())) {
            TablePath defaultDatabase = TablePath.of(getDefaultDatabase(), "default");
            createDatabaseUnderIdentity(defaultDatabase, true);
        }
    }

    @Override
    public void close() throws CatalogException {
        if (authentication != null) {
            authentication.close(
                    () -> {
                        if (fs != null) {
                            fs.close();
                        }
                        return null;
                    });
        }
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return "default";
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return execute(
                "check database",
                TablePath.of(databaseName, "<tables>"),
                () -> databaseExistsUnderIdentity(databaseName));
    }

    private boolean databaseExistsUnderIdentity(String databaseName) {
        if (StringUtils.isEmpty(databaseName)) {
            throw new CatalogException("Database name is null or empty.");
        }
        return listDatabasesUnderIdentity().contains(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return execute("list databases", null, this::listDatabasesUnderIdentity);
    }

    private List<String> listDatabasesUnderIdentity() {
        try {
            FileStatus[] fileStatuses = fs.listStatus(tableParentDfsPath);
            return Arrays.stream(fileStatuses)
                    .filter(FileStatus::isDirectory)
                    .map(fileStatus -> fileStatus.getPath().getName())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException("Listing database exception.", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        return execute(
                "list tables",
                TablePath.of(databaseName, "<tables>"),
                () -> listTablesUnderIdentity(databaseName));
    }

    private List<String> listTablesUnderIdentity(String databaseName) {
        if (!databaseExistsUnderIdentity(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }

        Path dbPath = new Path(tableParentDfsPath, databaseName);
        try {
            return Arrays.stream(fs.listStatus(dbPath))
                    .filter(FileStatus::isDirectory)
                    .map(fileStatus -> fileStatus.getPath().getName())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Listing table in database %s exception.", dbPath), e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        return execute("check table", tablePath, () -> tableExistsUnderIdentity(tablePath));
    }

    private boolean tableExistsUnderIdentity(TablePath tablePath) {
        String basePath = inferTablePath(tableParentDfsPathStr, tablePath);
        try {
            return fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
                    && fs.exists(
                            new Path(
                                    new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME),
                                    HoodieTableConfig.HOODIE_PROPERTIES_FILE));
        } catch (IOException e) {
            throw new CatalogException(
                    "Error while checking whether table exists under path:" + basePath, e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        return execute("get table", tablePath, () -> getTableUnderIdentity(tablePath));
    }

    private CatalogTable getTableUnderIdentity(TablePath tablePath) {
        if (!tableExistsUnderIdentity(tablePath)) {
            throw new TableNotExistException(name(), tablePath);
        }
        HoodieTableMetaClient hoodieTableMetaClient =
                HoodieTableMetaClient.builder()
                        .setBasePath(inferTablePath(tableParentDfsPathStr, tablePath))
                        .setConf(HadoopFSUtils.getStorageConfWithCopy(hadoopConf))
                        .build();
        HoodieTableType tableType = hoodieTableMetaClient.getTableType();
        HoodieTableConfig tableConfig = hoodieTableMetaClient.getTableConfig();
        TableSchema tableSchema = convertSchema(TableSchema.builder(), tableConfig);
        List<String> partitionFields = null;
        if (tableConfig.getPartitionFields().isPresent()) {
            partitionFields = Arrays.asList(tableConfig.getPartitionFields().get());
        }

        Map<String, String> options = new HashMap<>();
        if (tableConfig.getRecordKeyFields().isPresent()) {
            options.put(
                    RECORD_KEY_FIELDS.key(),
                    String.join(",", tableConfig.getRecordKeyFields().get()));
        }
        if (StringUtils.isNoneBlank(tableConfig.getPreCombineField())) {
            options.put(PRECOMBINE_FIELD.key(), tableConfig.getPreCombineField());
        }
        options.put(TABLE_TYPE.key(), tableType.name());
        options.put(CDC_ENABLED.key(), String.valueOf(tableConfig.isCDCEnabled()));
        return CatalogTable.of(
                TableIdentifier.of(
                        catalogName, tablePath.getDatabaseName(), tablePath.getTableName()),
                tableSchema,
                options,
                partitionFields,
                null);
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        execute(
                "create table",
                tablePath,
                () -> {
                    createTableUnderIdentity(tablePath, table);
                    return null;
                });
    }

    private void createTableUnderIdentity(TablePath tablePath, CatalogTable table) {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(table, "Table cannot be null");

        String tablePathStr = inferTablePath(tableParentDfsPathStr, tablePath);
        Path path = new Path(tablePathStr);
        try {
            if (!fs.exists(path)) {
                HoodieTableMetaClient.withPropertyBuilder()
                        .setTableType(table.getOptions().get(TABLE_TYPE.key()))
                        .setRecordKeyFields(table.getOptions().get(RECORD_KEY_FIELDS.key()))
                        .setTableCreateSchema(
                                convertToSchema(
                                                table.getSeaTunnelRowType(),
                                                AvroSchemaUtils.getAvroRecordQualifiedName(
                                                        table.getTableId().getTableName()))
                                        .toString())
                        .setTableName(tablePath.getTableName())
                        .setPartitionFields(String.join(",", table.getPartitionKeys()))
                        .setPayloadClassName(HoodieAvroPayload.class.getName())
                        .setCDCEnabled(
                                Boolean.parseBoolean(table.getOptions().get(CDC_ENABLED.key())))
                        .setPreCombineField(table.getOptions().get(PRECOMBINE_FIELD.key()))
                        .initTable(new HadoopStorageConfiguration(hadoopConf), tablePathStr);
            }
        } catch (IOException e) {
            throw new HoodieCatalogException(
                    String.format("Failed to create table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        execute(
                "drop table",
                tablePath,
                () -> {
                    dropTableUnderIdentity(tablePath, ignoreIfNotExists);
                    return null;
                });
    }

    private void dropTableUnderIdentity(TablePath tablePath, boolean ignoreIfNotExists) {
        if (!tableExistsUnderIdentity(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new TableNotExistException(catalogName, tablePath);
            }
        }

        Path path = new Path(inferTablePath(tableParentDfsPathStr, tablePath));
        try {
            this.fs.delete(path, true);
        } catch (IOException e) {
            throw new CatalogException(String.format("Dropping table %s exception.", tablePath), e);
        }
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Hudi catalog not support truncate table.");
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        execute(
                "create database",
                tablePath,
                () -> {
                    createDatabaseUnderIdentity(tablePath, ignoreIfExists);
                    return null;
                });
    }

    private void createDatabaseUnderIdentity(TablePath tablePath, boolean ignoreIfExists) {
        if (databaseExistsUnderIdentity(tablePath.getDatabaseName())) {
            if (ignoreIfExists) {
                return;
            } else {
                throw new DatabaseAlreadyExistException(catalogName, tablePath.getDatabaseName());
            }
        }

        Path dbPath = new Path(tableParentDfsPath, tablePath.getDatabaseName());
        try {
            fs.mkdirs(dbPath);
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Creating database %s exception.", tablePath.getDatabaseName()),
                    e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        execute(
                "drop database",
                tablePath,
                () -> {
                    dropDatabaseUnderIdentity(tablePath, ignoreIfNotExists);
                    return null;
                });
    }

    private void dropDatabaseUnderIdentity(TablePath tablePath, boolean ignoreIfNotExists) {
        // do nothing
        if (!databaseExistsUnderIdentity(tablePath.getDatabaseName())) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
            }
        }

        List<String> tables = listTablesUnderIdentity(tablePath.getDatabaseName());
        if (!tables.isEmpty()) {
            throw new CatalogException(
                    String.format(
                            "Database %s not empty, can't drop it.", tablePath.getDatabaseName()));
        }

        Path dbPath = new Path(tableParentDfsPath, tablePath.getDatabaseName());
        try {
            fs.delete(dbPath, true);
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Dropping database %s exception.", tablePath.getDatabaseName()),
                    e);
        }
    }

    private <T> T execute(
            String operation, TablePath tablePath, PrivilegedExceptionAction<T> action) {
        return authentication.execute(
                action,
                error -> {
                    String safeCatalog = safeIdentifier(catalogName);
                    TablePath safePath =
                            tablePath == null
                                    ? null
                                    : TablePath.of(
                                            safeIdentifier(tablePath.getDatabaseName()),
                                            safeIdentifier(tablePath.getSchemaName()),
                                            safeIdentifier(tablePath.getTableName()));
                    // Reconstruct domain errors rather than retaining arbitrary messages/causes
                    // from I/O.
                    if (safePath != null) {
                        if (error instanceof TableNotExistException) {
                            return new TableNotExistException(safeCatalog, safePath);
                        }
                        if (error instanceof TableAlreadyExistException) {
                            return new TableAlreadyExistException(safeCatalog, safePath);
                        }
                        if (error instanceof DatabaseNotExistException) {
                            return new DatabaseNotExistException(
                                    safeCatalog, safePath.getDatabaseName());
                        }
                        if (error instanceof DatabaseAlreadyExistException) {
                            return new DatabaseAlreadyExistException(
                                    safeCatalog, safePath.getDatabaseName());
                        }
                    }
                    return new CatalogException(
                            "Hudi catalog "
                                    + operation
                                    + " failed in "
                                    + safeCatalog
                                    + (safePath == null ? "" : " for " + safePath.getFullName())
                                    + " ("
                                    + HudiAuthenticationContext.failureCategory(error)
                                    + ")");
                });
    }

    private String safeIdentifier(String identifier) {
        if (identifier == null) {
            return null;
        }
        String safe = identifier;
        for (String credential : new String[] {principal, keytab, krb5Path}) {
            if (credential != null && !credential.isEmpty()) {
                safe = safe.replace(credential, "<redacted>");
            }
        }
        safe = safe.replaceAll("[\\p{Cntrl}]", "?");
        return safe.length() > 256 ? safe.substring(0, 256) + "..." : safe;
    }

    private TableSchema convertSchema(
            TableSchema.Builder tableSchemaBuilder, HoodieTableConfig tableConfig) {
        if (tableConfig.getTableCreateSchema().isPresent()) {
            Schema schema = tableConfig.getTableCreateSchema().get();
            List<Schema.Field> fields = schema.getFields();
            for (Schema.Field field : fields) {
                tableSchemaBuilder.column(
                        PhysicalColumn.of(
                                field.name(),
                                convertSeaTunnelType(field.name(), field.schema()),
                                (Long) null,
                                true,
                                null,
                                field.doc()));
            }
        }
        return tableSchemaBuilder.build();
    }
}
