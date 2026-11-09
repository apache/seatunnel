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
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.TimePartitioning;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

@AutoService(Factory.class)
@Slf4j
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

    /**
     * Resolves the SQL query from configuration. Supports two mutually exclusive modes: custom
     * query ({@code query} option) or table read ({@code dataset_id} + {@code table_id}). In
     * table-read mode, {@code partition_filter} is appended as a WHERE clause; it is rejected in
     * query mode to avoid predicate injection.
     *
     * @param config the source configuration
     * @return the resolved SQL query string
     * @throws BigQueryConnectorException if both or neither mode is provided, or if
     *     partition_filter is combined with query mode
     */
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
        BigQuery bigQuery = BigQueryClientFactory.getBigQuery(config);
        if (datasetId != null && tableId != null) {
            return resolveFromTableMetadata(config, datasetId, tableId, bigQuery);
        }

        return resolveFromQuery(config, bigQuery);
    }

    private CatalogTable resolveFromTableMetadata(
            ReadonlyConfig config, String datasetId, String tableId, BigQuery bigQuery) {
        String projectId = config.get(BigQuerySourceOptions.PROJECT_ID);
        String partitionFilter = config.get(BigQuerySourceOptions.PARTITION_FILTER);
        String fullTableName = projectId + "." + datasetId + "." + tableId;

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
     * Validates that a partition filter is supplied when the table requires one. Reads the stable
     * {@code TableInfo.getRequirePartitionFilter()} field first (covers range partitioning and
     * top-level-only metadata); falls back to the legacy {@code TimePartitioning} nested field for
     * compatibility with older SDK responses.
     *
     * @param table the BigQuery table metadata, or null if not available
     * @param partitionFilter the user-supplied partition filter, or null
     * @param fullTableName the fully qualified table name for error messages
     */
    public static void checkRequirePartitionFilter(
            Table table, String partitionFilter, String fullTableName) {
        if (table == null) {
            return;
        }
        Boolean requireFilter = table.getRequirePartitionFilter();
        if (requireFilter == null && table.getDefinition() instanceof StandardTableDefinition) {
            StandardTableDefinition definition = (StandardTableDefinition) table.getDefinition();
            TimePartitioning timePartitioning = definition.getTimePartitioning();
            if (timePartitioning != null) {
                requireFilter = timePartitioning.getRequirePartitionFilter();
            }
        }
        if (Boolean.TRUE.equals(requireFilter)
                && (partitionFilter == null || partitionFilter.trim().isEmpty())) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.CONFIG_INVALID,
                    "Table '"
                            + fullTableName
                            + "' has requirePartitionFilter enabled. "
                            + "Provide 'partition_filter' to filter partitions.");
        }
    }

    /**
     * Infers the catalog schema from a custom query. Always performs a zero-cost dry run via {@code
     * bigQuery.create(JobInfo)} with {@code dryRun=true}: the query is validated and its result
     * schema is extracted from {@link JobStatistics.QueryStatistics#getSchema()} without executing
     * the query or billing any scanned bytes. The {@code dry_run} option does not affect this
     * planning step; it only controls whether the reader executes the query at runtime. If the
     * dry-run response does not include a schema and {@code emulator_host} is configured (the
     * BigQuery emulator does not return a dry-run schema), falls back to a {@code maxResults=1}
     * query that executes but returns at most one row, still providing the schema for downstream
     * transforms. Without {@code emulator_host}, planning fails fast instead: a missing dry-run
     * schema must never cause planning to execute the query on real BigQuery.
     *
     * @param config the source configuration
     * @param bigQuery the BigQuery client used for schema discovery
     * @return a CatalogTable with the inferred schema
     */
    CatalogTable resolveFromQuery(ReadonlyConfig config, BigQuery bigQuery) {
        String query = config.get(BigQuerySourceOptions.QUERY);

        Schema schema = discoverSchemaViaDryRun(bigQuery, config, query);
        if (schema == null) {
            // The fallback executes the query at planning, so it is emulator-only: on real
            // BigQuery a missing dry-run schema (e.g. multi-statement scripts) must fail
            // fast instead of letting planning execute and bill the query.
            if (config.get(BigQuerySourceOptions.EMULATOR_HOST) == null) {
                throw new BigQueryConnectorException(
                        BigQueryConnectorErrorCode.READ_FAILED,
                        "Query schema could not be determined by dry run (BigQuery omits it "
                                + "for some valid inputs such as multi-statement scripts); the "
                                + "maxResults=1 fallback would execute the query at planning "
                                + "and is only supported with emulator_host. Query: "
                                + query);
            }
            log.warn(
                    "Dry run did not return schema and emulator_host is configured, "
                            + "falling back to maxResults=1 query for schema inference");
            try {
                schema = inferSchemaFromQuery(bigQuery, config, query);
            } catch (BigQueryException e) {
                throw new BigQueryConnectorException(
                        BigQueryConnectorErrorCode.READ_FAILED,
                        "Failed to infer schema from query (maxResults=1 fallback): " + query,
                        e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new BigQueryConnectorException(
                        BigQueryConnectorErrorCode.READ_FAILED,
                        "Failed to infer schema from query (maxResults=1 fallback): " + query,
                        e);
            }
        }
        if (schema == null || schema.getFields() == null || schema.getFields().isEmpty()) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.READ_FAILED,
                    "Query returned no schema, cannot infer table schema: " + query);
        }
        TableSchema tableSchema = buildTableSchema(schema);
        return CatalogTable.of(
                TableIdentifier.of("BigQuery", TablePath.of("query_result", "query_result")),
                tableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "BigQuery Source Table (schema inferred via dry run)");
    }

    /**
     * Executes the query with {@code maxResults=1} and returns the result schema. Used only as the
     * emulator fallback, gated by {@code emulator_host} in the caller, when a dry run does not
     * return a schema: the query is executed but at most one row is returned. Applies the same
     * {@code maximumBytesBilled}, {@code defaultDataset}, and {@code location} configuration as the
     * reader's execution path so that planning and runtime cannot disagree about billing or region.
     *
     * @param bigQuery the BigQuery client
     * @param config the source configuration
     * @param query the SQL query to execute
     * @return the BigQuery result schema, or null if the response has no schema
     * @throws InterruptedException if the query is interrupted
     */
    private Schema inferSchemaFromQuery(BigQuery bigQuery, ReadonlyConfig config, String query)
            throws InterruptedException {
        QueryJobConfiguration queryConfig =
                newQueryJobConfigBuilder(query, config).setMaxResults(1L).build();
        String location = config.get(BigQuerySourceOptions.LOCATION);
        TableResult result;
        if (location != null) {
            JobId jobId = JobId.newBuilder().setLocation(location).build();
            result = bigQuery.query(queryConfig, jobId);
        } else {
            result = bigQuery.query(queryConfig);
        }
        return result.getSchema();
    }

    /**
     * Discovers the query result schema via a zero-cost dry run without executing the query.
     * BigQuery populates {@link JobStatistics.QueryStatistics#getSchema()} only for dry-run jobs.
     * Applies the same location, default-dataset, and cost-cap configuration as the reader's
     * execution path so that planning and runtime cannot disagree about billing or region.
     *
     * @param bigQuery the BigQuery client
     * @param config the source configuration
     * @param query the SQL query to validate
     * @return the schema from the dry-run statistics, or null if the response has no schema (e.g.
     *     when using the BigQuery emulator)
     */
    private Schema discoverSchemaViaDryRun(BigQuery bigQuery, ReadonlyConfig config, String query) {
        QueryJobConfiguration queryConfig =
                newQueryJobConfigBuilder(query, config).setDryRun(true).build();
        try {
            JobInfo jobInfo;
            String location = config.get(BigQuerySourceOptions.LOCATION);
            if (location != null) {
                JobId jobId = JobId.newBuilder().setLocation(location).build();
                jobInfo = JobInfo.of(jobId, queryConfig);
            } else {
                jobInfo = JobInfo.of(queryConfig);
            }
            Job job = bigQuery.create(jobInfo);
            JobStatistics.QueryStatistics stats = job.getStatistics();
            if (stats != null) {
                Long estimatedBytes = stats.getEstimatedBytesProcessed();
                if (estimatedBytes != null) {
                    log.info(
                            "Dry run for query validation. Estimated bytes processed: {}",
                            estimatedBytes);
                }
                Long cap = config.get(BigQuerySourceOptions.MAXIMUM_BYTES_BILLED);
                if (cap != null && estimatedBytes != null) {
                    if (estimatedBytes > cap) {
                        log.warn(
                                "Estimated bytes {} exceeds maximum_bytes_billed {}",
                                estimatedBytes,
                                cap);
                    }
                }
                Schema schema = stats.getSchema();
                if (schema != null && schema.getFields() != null) {
                    log.info("Dry run returned schema with {} fields", schema.getFields().size());
                }
                return schema;
            }
            return null;
        } catch (BigQueryException e) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.READ_FAILED,
                    "Failed to validate query via dry run: " + query,
                    e);
        }
    }

    /**
     * Creates a {@link QueryJobConfiguration.Builder} pre-populated with the options shared by
     * schema discovery (planning) and query execution (runtime): {@code useQueryCache}, {@code
     * maximumBytesBilled} (if configured) and {@code defaultDataset} (if configured). Both stages
     * start from this builder so they cannot disagree about billing, dataset or cache semantics;
     * callers add stage-specific settings (dryRun, maxResults) on top.
     *
     * @param query the SQL query
     * @param config the source configuration
     * @return a builder with the shared options applied
     */
    static QueryJobConfiguration.Builder newQueryJobConfigBuilder(
            String query, ReadonlyConfig config) {
        QueryJobConfiguration.Builder builder =
                QueryJobConfiguration.newBuilder(query)
                        .setUseQueryCache(config.get(BigQuerySourceOptions.USE_QUERY_CACHE));
        if (config.get(BigQuerySourceOptions.MAXIMUM_BYTES_BILLED) != null) {
            builder.setMaximumBytesBilled(config.get(BigQuerySourceOptions.MAXIMUM_BYTES_BILLED));
        }
        if (config.get(BigQuerySourceOptions.DATASET_ID) != null) {
            builder.setDefaultDataset(
                    DatasetId.of(
                            config.get(BigQuerySourceOptions.PROJECT_ID),
                            config.get(BigQuerySourceOptions.DATASET_ID)));
        }
        return builder;
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
