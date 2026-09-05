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

package org.apache.seatunnel.connectors.bigquery.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.bigquery.client.BigQueryClientFactory;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;
import org.apache.seatunnel.connectors.bigquery.sink.BigQuerySinkBatchWriter;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryStreamWriter.CHANGE_TYPE;
import static org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryStreamWriter.SEQUENCE_NUM;

/**
 * Catalog implementation for Google Cloud BigQuery. This class provides standard catalog operations
 * such as listing databases/datasets, creating and deleting tables, checking table existence, and
 * reading metadata.
 */
@Slf4j
public class BigQueryCatalog implements Catalog {

    private final String catalogName;
    private final ReadonlyConfig config;
    private BigQuery bigquery;

    /**
     * Constructs a new BigQueryCatalog.
     *
     * @param catalogName the name of this catalog
     * @param config the readonly configuration options containing BigQuery connection info
     */
    public BigQueryCatalog(String catalogName, ReadonlyConfig config) {
        this.catalogName = catalogName;
        this.config = config;
    }

    /**
     * Opens the catalog and initializes the BigQuery client.
     *
     * @throws CatalogException if the BigQuery client fails to initialize
     */
    @Override
    public void open() throws CatalogException {
        try {
            this.bigquery = BigQueryClientFactory.getBigQuery(config);
            log.info("BigQueryCatalog '{}' opened successfully.", catalogName);
        } catch (Exception e) {
            throw new CatalogException("Failed to open BigQueryCatalog", e);
        }
    }

    /**
     * Closes the catalog.
     *
     * @throws CatalogException if any resources fail to close
     */
    @Override
    public void close() throws CatalogException {
        // BigQuery service client doesn't hold open TCP sockets directly; it's a stateless HTTP
        // wrapper.
        log.info("BigQueryCatalog '{}' closed successfully.", catalogName);
    }

    /**
     * Returns the catalog name.
     *
     * @return the name of this catalog
     */
    @Override
    public String name() {
        return catalogName;
    }

    private String getDatasetName(TablePath tablePath) {
        String db = tablePath.getDatabaseName();
        if (db == null || db.trim().isEmpty() || "default".equalsIgnoreCase(db)) {
            return config.get(BigQuerySinkOptions.DATASET_ID);
        }
        return db;
    }

    /**
     * Returns the default database (dataset ID) configured for the BigQuery connector.
     *
     * @return the configured default dataset ID
     * @throws CatalogException if default dataset info cannot be retrieved
     */
    @Override
    public String getDefaultDatabase() throws CatalogException {
        return config.get(BigQuerySinkOptions.DATASET_ID);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        if (databaseName == null || databaseName.trim().isEmpty()) {
            databaseName = config.get(BigQuerySinkOptions.DATASET_ID);
        }
        return bigquery.getDataset(databaseName) != null;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> databases = new ArrayList<>();
        try {
            Page<Dataset> datasets = bigquery.listDatasets();
            for (Dataset dataset : datasets.iterateAll()) {
                databases.add(dataset.getDatasetId().getDataset());
            }
        } catch (Exception e) {
            throw new CatalogException("Failed to list BigQuery databases (datasets)", e);
        }
        return databases;
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (databaseName == null || databaseName.trim().isEmpty()) {
            databaseName = config.get(BigQuerySinkOptions.DATASET_ID);
        }
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }
        List<String> tables = new ArrayList<>();
        try {
            Page<Table> bqTables = bigquery.listTables(databaseName);
            for (Table table : bqTables.iterateAll()) {
                tables.add(table.getTableId().getTable());
            }
        } catch (Exception e) {
            throw new CatalogException("Failed to list tables in dataset: " + databaseName, e);
        }
        return tables;
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        TableId tableId = TableId.of(getDatasetName(tablePath), tablePath.getTableName());
        return bigquery.getTable(tableId) != null;
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        TableId tableId = TableId.of(getDatasetName(tablePath), tablePath.getTableName());
        Table table = bigquery.getTable(tableId);
        if (table == null) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        Schema bqSchema = table.getDefinition().getSchema();
        TableSchema.Builder schemaBuilder = TableSchema.builder();

        if (bqSchema != null && bqSchema.getFields() != null) {
            for (Field field : bqSchema.getFields()) {
                // Skip change capture metadata columns to keep source/target comparison clean
                if (CHANGE_TYPE.equals(field.getName()) || SEQUENCE_NUM.equals(field.getName())) {
                    continue;
                }
                SeaTunnelDataType<?> type = mapToSeaTunnelType(field);
                Column column =
                        PhysicalColumn.of(
                                field.getName(),
                                type,
                                0,
                                field.getMode() == Field.Mode.NULLABLE || field.getMode() == null,
                                null,
                                null);
                schemaBuilder.column(column);
            }
        }

        return CatalogTable.of(
                TableIdentifier.of(catalogName, tablePath),
                schemaBuilder.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "BigQuery Catalog Table");
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(getDatasetName(tablePath))) {
            throw new DatabaseNotExistException(catalogName, getDatasetName(tablePath));
        }
        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return;
            }
            throw new TableAlreadyExistException(catalogName, tablePath);
        }

        List<Field> fields = new ArrayList<>();
        for (Column column : table.getTableSchema().getColumns()) {
            fields.add(convertColumn(column));
        }

        // Add stream change capture tracking fields if running in streaming mode
        boolean isBatch =
                BigQuerySinkBatchWriter.BATCH.equals(config.get(BigQuerySinkOptions.WRITE_MODE));
        if (!isBatch) {
            org.apache.seatunnel.api.table.catalog.PrimaryKey seaTunnelPrimaryKey =
                    table.getTableSchema().getPrimaryKey();
            if (seaTunnelPrimaryKey == null || seaTunnelPrimaryKey.getColumnNames().isEmpty()) {
                throw new CatalogException(
                        "Streaming mode requires a Primary Key in the schema of table: "
                                + tablePath.getFullName());
            }
        }

        Schema bqSchema = Schema.of(fields);
        TableId tableId = TableId.of(getDatasetName(tablePath), tablePath.getTableName());

        TableDefinition tableDefinition;
        org.apache.seatunnel.api.table.catalog.PrimaryKey seaTunnelPrimaryKey =
                table.getTableSchema().getPrimaryKey();

        if (seaTunnelPrimaryKey != null && !seaTunnelPrimaryKey.getColumnNames().isEmpty()) {
            com.google.cloud.bigquery.PrimaryKey bqPrimaryKey =
                    com.google.cloud.bigquery.PrimaryKey.newBuilder()
                            .setColumns(seaTunnelPrimaryKey.getColumnNames())
                            .build();

            com.google.cloud.bigquery.TableConstraints constraints =
                    com.google.cloud.bigquery.TableConstraints.newBuilder()
                            .setPrimaryKey(bqPrimaryKey)
                            .build();

            tableDefinition =
                    StandardTableDefinition.newBuilder()
                            .setSchema(bqSchema)
                            .setTableConstraints(constraints)
                            .build();
        } else {
            tableDefinition = StandardTableDefinition.of(bqSchema);
        }

        try {
            bigquery.create(TableInfo.of(tableId, tableDefinition));
            log.info("BigQuery Table '{}' created successfully.", tablePath.getFullName());
        } catch (Exception e) {
            throw new CatalogException(
                    "Failed to create BigQuery table: " + tablePath.getFullName(), e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        TableId tableId = TableId.of(getDatasetName(tablePath), tablePath.getTableName());
        try {
            boolean deleted = bigquery.delete(tableId);
            if (!deleted && !ignoreIfNotExists) {
                throw new TableNotExistException(catalogName, tablePath);
            }
            log.info("BigQuery Table '{}' dropped successfully.", tablePath.getFullName());
        } catch (TableNotExistException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    "Failed to drop BigQuery table: " + tablePath.getFullName(), e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        String databaseName = getDatasetName(tablePath);
        if (databaseExists(databaseName)) {
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(catalogName, databaseName);
        }
        try {
            bigquery.create(DatasetInfo.newBuilder(databaseName).build());
            log.info("BigQuery Dataset (database) '{}' created successfully.", databaseName);
        } catch (Exception e) {
            throw new CatalogException("Failed to create BigQuery dataset: " + databaseName, e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        String databaseName = getDatasetName(tablePath);
        if (!databaseExists(databaseName)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException(catalogName, databaseName);
        }
        try {
            bigquery.delete(databaseName, BigQuery.DatasetDeleteOption.deleteContents());
            log.info("BigQuery Dataset (database) '{}' dropped successfully.", databaseName);
        } catch (Exception e) {
            throw new CatalogException("Failed to drop BigQuery dataset: " + databaseName, e);
        }
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(catalogName, tablePath);
        }
        String query =
                String.format(
                        "TRUNCATE TABLE `%s.%s` ;",
                        getDatasetName(tablePath), tablePath.getTableName());
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        try {
            bigquery.query(queryConfig);
            log.info("BigQuery Table '{}' truncated successfully.", tablePath.getFullName());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CatalogException("Truncate table interrupted", e);
        } catch (Exception e) {
            throw new CatalogException(
                    "Failed to truncate BigQuery table: " + tablePath.getFullName(), e);
        }
    }

    @Override
    public boolean isExistsData(TablePath tablePath) {
        if (!tableExists(tablePath)) {
            return false;
        }
        String query =
                String.format(
                        "SELECT 1 FROM `%s.%s` LIMIT 1 ;",
                        getDatasetName(tablePath), tablePath.getTableName());
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        try {
            return bigquery.query(queryConfig).iterateAll().iterator().hasNext();
        } catch (Exception e) {
            log.warn(
                    "Failed to check if table has data via query, falling back to metadata: {}",
                    e.getMessage());
            TableId tableId = TableId.of(getDatasetName(tablePath), tablePath.getTableName());
            Table table = bigquery.getTable(tableId);
            return table != null && table.getNumRows().longValue() > 0;
        }
    }

    @Override
    public void executeSql(TablePath tablePath, String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            log.warn("No custom SQL query provided for table {}, skipping execution.", tablePath);
            return;
        }
        log.info("Executing custom SQL in dataset {}: {}", getDatasetName(tablePath), sql);
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
        try {
            bigquery.query(queryConfig);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CatalogException("Custom SQL execution interrupted", e);
        } catch (Exception e) {
            throw new CatalogException("Failed to execute custom SQL", e);
        }
    }

    private Field convertColumn(Column column) {
        StandardSQLTypeName bqType = mapToBigQueryType(column.getDataType().getSqlType());
        Field.Builder fieldBuilder = Field.newBuilder(column.getName(), bqType);

        if (column.isNullable()) {
            fieldBuilder.setMode(Field.Mode.NULLABLE);
        } else {
            fieldBuilder.setMode(Field.Mode.REQUIRED);
        }

        if (column.getDataType().getSqlType() == SqlType.ROW) {
            SeaTunnelRowType rowType = (SeaTunnelRowType) column.getDataType();
            List<Field> subFields = new ArrayList<>();
            for (int i = 0; i < rowType.getFieldNames().length; i++) {
                Column subColumn =
                        PhysicalColumn.of(
                                rowType.getFieldNames()[i],
                                rowType.getFieldType(i),
                                0,
                                true,
                                null,
                                null);
                subFields.add(convertColumn(subColumn));
            }
            fieldBuilder.setType(bqType, FieldList.of(subFields));
        }

        return fieldBuilder.build();
    }

    private StandardSQLTypeName mapToBigQueryType(SqlType sqlType) {
        switch (sqlType) {
            case BOOLEAN:
                return StandardSQLTypeName.BOOL;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return StandardSQLTypeName.INT64;
            case FLOAT:
            case DOUBLE:
                return StandardSQLTypeName.FLOAT64;
            case DECIMAL:
                return StandardSQLTypeName.NUMERIC;
            case DATE:
                return StandardSQLTypeName.DATE;
            case TIME:
                return StandardSQLTypeName.TIME;
            case TIMESTAMP:
                return StandardSQLTypeName.TIMESTAMP;
            case BYTES:
                return StandardSQLTypeName.BYTES;
            case ROW:
                return StandardSQLTypeName.STRUCT;
            case STRING:
            default:
                return StandardSQLTypeName.STRING;
        }
    }

    private SeaTunnelDataType<?> mapToSeaTunnelType(Field field) {
        StandardSQLTypeName standardType = field.getType().getStandardType();
        switch (standardType) {
            case BOOL:
                return BasicType.BOOLEAN_TYPE;
            case INT64:
                return BasicType.LONG_TYPE;
            case FLOAT64:
                return BasicType.DOUBLE_TYPE;
            case NUMERIC:
            case BIGNUMERIC:
                return new DecimalType(38, 9);
            case BYTES:
                return PrimitiveByteArrayType.INSTANCE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case DATETIME:
            case TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case STRUCT:
                FieldList subFields = field.getSubFields();
                String[] fieldNames = new String[subFields.size()];
                SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType<?>[subFields.size()];
                for (int i = 0; i < subFields.size(); i++) {
                    Field subField = subFields.get(i);
                    fieldNames[i] = subField.getName();
                    fieldTypes[i] = mapToSeaTunnelType(subField);
                }
                return new SeaTunnelRowType(fieldNames, fieldTypes);
            case STRING:
            default:
                return BasicType.STRING_TYPE;
        }
    }
}
