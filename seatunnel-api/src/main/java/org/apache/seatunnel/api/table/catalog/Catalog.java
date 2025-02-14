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

package org.apache.seatunnel.api.table.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Interface for reading and writing table metadata from SeaTunnel. Each connector need to contain
 * the implementation of Catalog.
 */
public interface Catalog extends AutoCloseable {

    default Optional<Factory> getFactory() {
        return Optional.empty();
    }

    /**
     * Open the catalog. Used for any required preparation in initialization phase.
     *
     * @throws CatalogException in case of any runtime exception
     */
    void open() throws CatalogException;

    /**
     * Close the catalog when it is no longer needed and release any resource that it might be
     * holding.
     *
     * @throws CatalogException in case of any runtime exception
     */
    void close() throws CatalogException;

    /** Get the name of the catalog. */
    String name();

    // --------------------------------------------------------------------------------------------
    // database
    // --------------------------------------------------------------------------------------------

    /**
     * Get the name of the default database for this catalog. The default database will be the
     * current database for the catalog when user's session doesn't specify a current database. The
     * value probably comes from configuration, will not change for the life time of the catalog
     * instance.
     *
     * @return the name of the current database
     * @throws CatalogException in case of any runtime exception
     */
    String getDefaultDatabase() throws CatalogException;

    /**
     * Check if a database exists in this catalog.
     *
     * @param databaseName Name of the database
     * @return true if the given database exists in the catalog false otherwise
     * @throws CatalogException in case of any runtime exception
     */
    boolean databaseExists(String databaseName) throws CatalogException;

    /**
     * Get the names of all databases in this catalog.
     *
     * @return a list of the names of all databases
     * @throws CatalogException in case of any runtime exception
     */
    List<String> listDatabases() throws CatalogException;

    // --------------------------------------------------------------------------------------------
    // table
    // --------------------------------------------------------------------------------------------

    /**
     * Get names of all tables under this database. An empty list is returned if none exists.
     *
     * @return a list of the names of all tables in this database
     * @throws CatalogException in case of any runtime exception
     */
    List<String> listTables(String databaseName) throws CatalogException, DatabaseNotExistException;

    /**
     * Check if a table exist in this catalog.
     *
     * @param tablePath Path of the table
     * @return true if the given table exists in the catalog false otherwise
     * @throws CatalogException in case of any runtime exception
     */
    boolean tableExists(TablePath tablePath) throws CatalogException;

    /**
     * Return a {@link CatalogTable} identified by the given {@link TablePath}. The framework will
     * resolve the metadata objects when necessary.
     *
     * @param tablePath Path of the table
     * @return The requested table
     * @throws CatalogException in case of any runtime exception
     */
    CatalogTable getTable(TablePath tablePath) throws CatalogException, TableNotExistException;

    /**
     * Return a {@link CatalogTable} identified by the given {@link TablePath} and field names. The
     * framework will resolve the metadata objects when necessary.
     *
     * @param tablePath Path of the table
     * @param fieldNames The field names need read
     * @return The requested table
     * @throws CatalogException in case of any runtime exception
     */
    default CatalogTable getTable(TablePath tablePath, List<String> fieldNames)
            throws CatalogException, TableNotExistException {
        throw CommonError.unsupportedOperation(
                name(), "get table with tablePath " + tablePath + ", fieldNames: " + fieldNames);
    }

    default List<CatalogTable> getTables(ReadonlyConfig config) throws CatalogException {
        // Get the list of specified tables
        List<String> tableNames = config.get(ConnectorCommonOptions.TABLE_NAMES);
        if (tableNames != null && !tableNames.isEmpty()) {
            Iterator<TablePath> tablePaths =
                    tableNames.stream().map(TablePath::of).filter(this::tableExists).iterator();
            return buildCatalogTablesWithErrorCheck(tablePaths);
        }

        // Get the list of table pattern
        String tablePatternStr = config.get(ConnectorCommonOptions.TABLE_PATTERN);
        if (StringUtils.isBlank(tablePatternStr)) {
            return Collections.emptyList();
        }
        Pattern databasePattern =
                Pattern.compile(config.get(ConnectorCommonOptions.DATABASE_PATTERN));
        Pattern tablePattern = Pattern.compile(config.get(ConnectorCommonOptions.TABLE_PATTERN));
        List<String> allDatabase = this.listDatabases();
        allDatabase.removeIf(s -> !databasePattern.matcher(s).matches());
        List<TablePath> tablePaths = new ArrayList<>();
        for (String databaseName : allDatabase) {
            tableNames = this.listTables(databaseName);
            tableNames.forEach(
                    tableName -> {
                        if (tablePattern.matcher(databaseName + "." + tableName).matches()) {
                            tablePaths.add(TablePath.of(databaseName, tableName));
                        }
                    });
        }
        return buildCatalogTablesWithErrorCheck(tablePaths.iterator());
    }

    default List<CatalogTable> buildCatalogTablesWithErrorCheck(Iterator<TablePath> tablePaths) {
        Map<String, Map<String, String>> unsupportedTable = new LinkedHashMap<>();
        List<CatalogTable> catalogTables = new ArrayList<>();
        while (tablePaths.hasNext()) {
            try {
                catalogTables.add(getTable(tablePaths.next()));
            } catch (SeaTunnelRuntimeException e) {
                if (e.getSeaTunnelErrorCode()
                        .equals(CommonErrorCode.GET_CATALOG_TABLE_WITH_UNSUPPORTED_TYPE_ERROR)) {
                    unsupportedTable.put(
                            e.getParams().get("tableName"),
                            e.getParamsValueAsMap("fieldWithDataTypes"));
                } else {
                    throw e;
                }
            }
        }
        if (!unsupportedTable.isEmpty()) {
            throw CommonError.getCatalogTablesWithUnsupportedType(name(), unsupportedTable);
        }
        return catalogTables;
    }

    default <T> void buildColumnsWithErrorCheck(
            TablePath tablePath,
            TableSchema.Builder builder,
            Iterator<T> keys,
            Function<T, Column> getColumn) {
        Map<String, String> unsupported = new LinkedHashMap<>();
        while (keys.hasNext()) {
            try {
                builder.column(getColumn.apply(keys.next()));
            } catch (SeaTunnelRuntimeException e) {
                if (e.getSeaTunnelErrorCode()
                        .equals(CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE)) {
                    unsupported.put(e.getParams().get("field"), e.getParams().get("dataType"));
                } else {
                    throw e;
                }
            }
        }
        if (!unsupported.isEmpty()) {
            throw CommonError.getCatalogTableWithUnsupportedType(
                    name(), tablePath.getFullName(), unsupported);
        }
    }

    /**
     * Create a new table in this catalog.
     *
     * @param tablePath Path of the table
     * @param table The table definition
     * @param ignoreIfExists Flag to specify behavior when a table with the given name already exist
     * @throws TableAlreadyExistException thrown if the table already exists in the catalog and
     *     ignoreIfExists is false
     * @throws DatabaseNotExistException thrown if the database in tablePath doesn't exist in the
     *     catalog
     * @throws CatalogException in case of any runtime exception
     */
    void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException;

    /**
     * Create a new table in this catalog.
     *
     * @param tablePath Path of the table
     * @param table The table definition
     * @param ignoreIfExists Flag to specify behavior when a table with the given name already exist
     * @param createIndex If you want to create index or not
     * @throws TableAlreadyExistException thrown if the table already exists in the catalog and
     *     ignoreIfExists is false
     * @throws DatabaseNotExistException thrown if the database in tablePath doesn't exist in the
     *     catalog
     * @throws CatalogException in case of any runtime exception
     */
    default void createTable(
            TablePath tablePath, CatalogTable table, boolean ignoreIfExists, boolean createIndex)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        createTable(tablePath, table, ignoreIfExists);
    }

    /**
     * Drop an existing table in this catalog.
     *
     * @param tablePath Path of the table
     * @param ignoreIfNotExists Flag to specify behavior when a table with the given name doesn't
     *     exist
     * @throws TableNotExistException thrown if the table doesn't exist in the catalog and
     *     ignoreIfNotExists is false
     * @throws CatalogException in case of any runtime exception
     */
    void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException;

    void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException;

    void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException;

    /**
     * Truncate an existing table data in this catalog.
     *
     * @param tablePath Path of the table
     * @param ignoreIfNotExists Flag to specify behavior when a table with the given name doesn't
     *     exist
     * @throws TableNotExistException thrown if the table doesn't exist in the catalog and
     *     ignoreIfNotExists is false
     * @throws CatalogException in case of any runtime exception
     */
    default void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {}

    default boolean isExistsData(TablePath tablePath) {
        return false;
    }

    default void executeSql(TablePath tablePath, String sql) {}

    default PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
        throw new UnsupportedOperationException("Preview action is not supported");
    }

    enum ActionType {
        CREATE_TABLE,
        CREATE_DATABASE,
        DROP_TABLE,
        DROP_DATABASE,
        TRUNCATE_TABLE
    }

    // todo: Support for update table metadata

}
