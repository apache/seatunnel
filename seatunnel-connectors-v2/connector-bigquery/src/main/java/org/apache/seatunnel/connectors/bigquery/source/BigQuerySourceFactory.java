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

package org.apache.seatunnel.connectors.bigquery.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.bigquery.client.BigQueryClientFactory;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySourceOptions;
import org.apache.seatunnel.connectors.bigquery.schema.BigQueryTypeConverter;

import com.google.auto.service.AutoService;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.TimePartitioning;

import java.util.Collections;

@AutoService(Factory.class)
public class BigQuerySourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return BigQuerySourceOptions.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        BigQuerySourceOptions.PROJECT_ID,
                        Conditions.notBlank(BigQuerySourceOptions.PROJECT_ID))
                .optional(
                        BigQuerySourceOptions.QUERY,
                        BigQuerySourceOptions.DATASET_ID,
                        BigQuerySourceOptions.TABLE_ID,
                        BigQuerySourceOptions.SERVICE_ACCOUNT_KEY_PATH,
                        BigQuerySourceOptions.SERVICE_ACCOUNT_KEY_JSON,
                        BigQuerySourceOptions.MAX_RESULTS,
                        BigQuerySourceOptions.FETCH_SIZE,
                        BigQuerySourceOptions.LOCATION,
                        BigQuerySourceOptions.USE_QUERY_CACHE,
                        BigQuerySourceOptions.DRY_RUN,
                        BigQuerySourceOptions.MAXIMUM_BYTES_BILLED,
                        BigQuerySourceOptions.PARTITION_FILTER,
                        BigQuerySourceOptions.EMULATOR_HOST,
                        BigQuerySourceOptions.UNIVERSE_DOMAIN)
                .build();
    }

    @Override
    public TableSource<SeaTunnelRow, BigQuerySourceSplit, BigQuerySourceState> createSource(
            TableSourceFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        String query = resolveQuery(config);
        CatalogTable catalogTable = resolveCatalogTable(config);
        return () -> new BigQuerySource(config, catalogTable, query);
    }

    @Override
    public Class<? extends org.apache.seatunnel.api.source.SeaTunnelSource> getSourceClass() {
        return BigQuerySource.class;
    }

    public String resolveQuery(ReadonlyConfig config) {
        String query = config.get(BigQuerySourceOptions.QUERY);
        String datasetId = config.get(BigQuerySourceOptions.DATASET_ID);
        String tableId = config.get(BigQuerySourceOptions.TABLE_ID);
        String projectId = config.get(BigQuerySourceOptions.PROJECT_ID);
        String partitionFilter = config.get(BigQuerySourceOptions.PARTITION_FILTER);

        boolean hasQuery = query != null && !query.trim().isEmpty();
        boolean hasTable = datasetId != null && tableId != null;
        boolean hasPartitionFilter = partitionFilter != null && !partitionFilter.trim().isEmpty();

        if (hasQuery && hasTable) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.CONFIG_INVALID,
                    "Provide either 'query' or 'dataset_id' + 'table_id', not both.");
        }
        if (hasQuery) {
            if (hasPartitionFilter) {
                throw new BigQueryConnectorException(
                        BigQueryConnectorErrorCode.CONFIG_INVALID,
                        "'partition_filter' is only valid with 'dataset_id' + 'table_id' mode, not with 'query'.");
            }
            return query;
        }
        if (hasTable) {
            String baseQuery =
                    String.format("SELECT * FROM `%s.%s.%s`", projectId, datasetId, tableId);
            if (hasPartitionFilter) {
                return baseQuery + " WHERE " + partitionFilter;
            }
            return baseQuery;
        }
        throw new BigQueryConnectorException(
                BigQueryConnectorErrorCode.CONFIG_INVALID,
                "Either 'query' or 'dataset_id' + 'table_id' must be provided.");
    }

    private CatalogTable resolveCatalogTable(ReadonlyConfig config) {
        String datasetId = config.get(BigQuerySourceOptions.DATASET_ID);
        String tableId = config.get(BigQuerySourceOptions.TABLE_ID);
        if (datasetId != null && tableId != null) {
            return resolveFromTableMetadata(config, datasetId, tableId);
        }

        return resolveFromQuery(config);
    }

    private CatalogTable resolveFromTableMetadata(
            ReadonlyConfig config, String datasetId, String tableId) {
        String projectId = config.get(BigQuerySourceOptions.PROJECT_ID);
        String partitionFilter = config.get(BigQuerySourceOptions.PARTITION_FILTER);
        String fullTableName = projectId + "." + datasetId + "." + tableId;

        BigQuery bigQuery = BigQueryClientFactory.getBigQuery(config);
        Table table;
        try {
            TableId tableIdObj = TableId.of(projectId, datasetId, tableId);
            table = bigQuery.getTable(tableIdObj);
        } catch (BigQueryException e) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.READ_FAILED,
                    "Failed to get table metadata for: " + fullTableName,
                    e);
        }
        if (table == null) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.READ_FAILED, "Table not found: " + fullTableName);
        }

        checkRequirePartitionFilter(table, partitionFilter, fullTableName);

        Schema bqSchema = table.getDefinition().getSchema();
        TableSchema tableSchema = buildTableSchema(bqSchema);
        return CatalogTable.of(
                TableIdentifier.of("BigQuery", TablePath.of(datasetId, tableId)),
                tableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "BigQuery Source Table");
    }

    /**
     * Validates that a partition filter is supplied when the table requires one.
     *
     * @param table the BigQuery table metadata, or null if not available
     * @param partitionFilter the user-supplied partition filter, or null
     * @param fullTableName the fully qualified table name for error messages
     */
    public static void checkRequirePartitionFilter(
            Table table, String partitionFilter, String fullTableName) {
        if (table == null || !(table.getDefinition() instanceof StandardTableDefinition)) {
            return;
        }
        StandardTableDefinition definition = (StandardTableDefinition) table.getDefinition();
        TimePartitioning timePartitioning = definition.getTimePartitioning();
        if (timePartitioning != null
                && Boolean.TRUE.equals(timePartitioning.getRequirePartitionFilter())) {
            if (partitionFilter == null || partitionFilter.trim().isEmpty()) {
                throw new BigQueryConnectorException(
                        BigQueryConnectorErrorCode.CONFIG_INVALID,
                        "Table '"
                                + fullTableName
                                + "' has requirePartitionFilter enabled. "
                                + "Provide 'partition_filter' to filter partitions.");
            }
        }
    }

    private CatalogTable resolveFromQuery(ReadonlyConfig config) {
        String query = config.get(BigQuerySourceOptions.QUERY);
        BigQuery bigQuery = BigQueryClientFactory.getBigQuery(config);
        try {
            QueryJobConfiguration.Builder configBuilder =
                    QueryJobConfiguration.newBuilder(query)
                            .setMaxResults(1L)
                            .setUseQueryCache(config.get(BigQuerySourceOptions.USE_QUERY_CACHE));
            if (config.get(BigQuerySourceOptions.DATASET_ID) != null) {
                configBuilder.setDefaultDataset(
                        DatasetId.of(
                                config.get(BigQuerySourceOptions.PROJECT_ID),
                                config.get(BigQuerySourceOptions.DATASET_ID)));
            }
            QueryJobConfiguration queryConfig = configBuilder.build();
            String location = config.get(BigQuerySourceOptions.LOCATION);
            TableResult result;
            if (location != null) {
                JobId jobId = JobId.newBuilder().setLocation(location).build();
                result = bigQuery.query(queryConfig, jobId);
            } else {
                result = bigQuery.query(queryConfig);
            }
            Schema bqSchema = result.getSchema();
            TableSchema tableSchema = buildTableSchema(bqSchema);
            return CatalogTable.of(
                    TableIdentifier.of("BigQuery", TablePath.of("query_result", "query_result")),
                    tableSchema,
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    "BigQuery Source Table (inferred from query)");
        } catch (BigQueryException e) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.READ_FAILED,
                    "Failed to infer schema from query: " + query,
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.READ_FAILED,
                    "Failed to infer schema from query: " + query,
                    e);
        }
    }

    private TableSchema buildTableSchema(Schema bqSchema) {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        if (bqSchema != null && bqSchema.getFields() != null) {
            FieldList fields = bqSchema.getFields();
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                SeaTunnelDataType<?> type = BigQueryTypeConverter.toSeaTunnelType(field);
                boolean nullable =
                        field.getMode() == Field.Mode.NULLABLE || field.getMode() == null;
                Column column = PhysicalColumn.of(field.getName(), type, 0, nullable, null, null);
                schemaBuilder.column(column);
            }
        }
        return schemaBuilder.build();
    }
}
