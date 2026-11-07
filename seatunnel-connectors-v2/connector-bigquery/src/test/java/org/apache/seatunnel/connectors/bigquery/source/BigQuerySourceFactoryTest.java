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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySourceOptions;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.TimePartitioning;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BigQuerySourceFactoryTest {

    @Test
    public void testFactoryIdentifier() {
        BigQuerySourceFactory factory = new BigQuerySourceFactory();
        assertEquals(BigQuerySourceOptions.IDENTIFIER, factory.factoryIdentifier());
        assertEquals("BigQuery", factory.factoryIdentifier());
    }

    @Test
    public void testRequiredProjectIdAccepted() {
        Config config =
                ConfigFactory.parseString(
                        BigQuerySourceOptions.PROJECT_ID.key()
                                + " = \"test-project\"\n"
                                + BigQuerySourceOptions.QUERY.key()
                                + " = \"SELECT 1\"\n");

        BigQuerySourceFactory factory = new BigQuerySourceFactory();

        assertDoesNotThrow(
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromConfig(config))
                                .validate(factory.optionRule()));
    }

    @Test
    public void testBlankProjectIdRejectedByOptionRule() {
        Config config =
                ConfigFactory.parseString(
                        BigQuerySourceOptions.PROJECT_ID.key()
                                + " = \" \"\n"
                                + BigQuerySourceOptions.QUERY.key()
                                + " = \"SELECT 1\"\n");

        BigQuerySourceFactory factory = new BigQuerySourceFactory();

        assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromConfig(config))
                                .validate(factory.optionRule()));
    }

    @Test
    public void testMissingProjectIdRejectedByOptionRule() {
        Config config =
                ConfigFactory.parseString(BigQuerySourceOptions.QUERY.key() + " = \"SELECT 1\"\n");

        BigQuerySourceFactory factory = new BigQuerySourceFactory();

        assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromConfig(config))
                                .validate(factory.optionRule()));
    }

    @Test
    public void testAllOptionalOptionsAccepted() {
        Config config =
                ConfigFactory.parseString(
                        BigQuerySourceOptions.PROJECT_ID.key()
                                + " = \"test-project\"\n"
                                + BigQuerySourceOptions.QUERY.key()
                                + " = \"SELECT * FROM `proj.ds.tbl`\"\n"
                                + BigQuerySourceOptions.DATASET_ID.key()
                                + " = \"ds\"\n"
                                + BigQuerySourceOptions.TABLE_ID.key()
                                + " = \"tbl\"\n"
                                + BigQuerySourceOptions.SERVICE_ACCOUNT_KEY_PATH.key()
                                + " = \"/tmp/key.json\"\n"
                                + BigQuerySourceOptions.MAX_RESULTS.key()
                                + " = 200\n"
                                + BigQuerySourceOptions.FETCH_SIZE.key()
                                + " = 50\n"
                                + BigQuerySourceOptions.LOCATION.key()
                                + " = \"us-central1\"\n"
                                + BigQuerySourceOptions.USE_QUERY_CACHE.key()
                                + " = false\n"
                                + BigQuerySourceOptions.DRY_RUN.key()
                                + " = true\n"
                                + BigQuerySourceOptions.MAXIMUM_BYTES_BILLED.key()
                                + " = 1000000000\n"
                                + BigQuerySourceOptions.PARTITION_FILTER.key()
                                + " = \"_PARTITIONDATE >= '2026-01-01'\"\n");

        BigQuerySourceFactory factory = new BigQuerySourceFactory();
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);

        assertDoesNotThrow(() -> ConfigValidator.of(readonlyConfig).validate(factory.optionRule()));
        assertEquals(200L, readonlyConfig.get(BigQuerySourceOptions.MAX_RESULTS));
        assertEquals(50, readonlyConfig.get(BigQuerySourceOptions.FETCH_SIZE));
        assertEquals("us-central1", readonlyConfig.get(BigQuerySourceOptions.LOCATION));
        assertEquals(false, readonlyConfig.get(BigQuerySourceOptions.USE_QUERY_CACHE));
        assertEquals(true, readonlyConfig.get(BigQuerySourceOptions.DRY_RUN));
        assertEquals(1000000000L, readonlyConfig.get(BigQuerySourceOptions.MAXIMUM_BYTES_BILLED));
        assertEquals(
                "_PARTITIONDATE >= '2026-01-01'",
                readonlyConfig.get(BigQuerySourceOptions.PARTITION_FILTER));
    }

    @Test
    public void testDefaultValuesApplied() {
        Config config =
                ConfigFactory.parseString(
                        BigQuerySourceOptions.PROJECT_ID.key()
                                + " = \"test-project\"\n"
                                + BigQuerySourceOptions.QUERY.key()
                                + " = \"SELECT 1\"\n");

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);

        assertEquals(100L, readonlyConfig.get(BigQuerySourceOptions.MAX_RESULTS));
        assertEquals(0, readonlyConfig.get(BigQuerySourceOptions.FETCH_SIZE));
        assertEquals(true, readonlyConfig.get(BigQuerySourceOptions.USE_QUERY_CACHE));
        assertEquals(false, readonlyConfig.get(BigQuerySourceOptions.DRY_RUN));
    }

    @Test
    public void testBothQueryAndTableIdRejected() {
        Config config =
                ConfigFactory.parseString(
                        BigQuerySourceOptions.PROJECT_ID.key()
                                + " = \"test-project\"\n"
                                + BigQuerySourceOptions.QUERY.key()
                                + " = \"SELECT 1\"\n"
                                + BigQuerySourceOptions.DATASET_ID.key()
                                + " = \"ds\"\n"
                                + BigQuerySourceOptions.TABLE_ID.key()
                                + " = \"tbl\"\n");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);
        TableSourceFactoryContext context = mock(TableSourceFactoryContext.class);
        when(context.getOptions()).thenReturn(readonlyConfig);

        BigQuerySourceFactory factory = new BigQuerySourceFactory();
        BigQueryConnectorException ex =
                assertThrows(BigQueryConnectorException.class, () -> factory.createSource(context));
        assertTrue(ex.getMessage().contains("not both"));
    }

    @Test
    public void testNeitherQueryNorTableIdRejected() {
        Config config =
                ConfigFactory.parseString(
                        BigQuerySourceOptions.PROJECT_ID.key() + " = \"test-project\"\n");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);
        TableSourceFactoryContext context = mock(TableSourceFactoryContext.class);
        when(context.getOptions()).thenReturn(readonlyConfig);

        BigQuerySourceFactory factory = new BigQuerySourceFactory();
        BigQueryConnectorException ex =
                assertThrows(BigQueryConnectorException.class, () -> factory.createSource(context));
        assertTrue(ex.getMessage().contains("must be provided"));
    }

    @Test
    public void testPartitionFilterWithQueryRejected() {
        Config config =
                ConfigFactory.parseString(
                        BigQuerySourceOptions.PROJECT_ID.key()
                                + " = \"test-project\"\n"
                                + BigQuerySourceOptions.QUERY.key()
                                + " = \"SELECT 1\"\n"
                                + BigQuerySourceOptions.PARTITION_FILTER.key()
                                + " = \"_PARTITIONDATE >= '2026-01-01'\"\n");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);
        TableSourceFactoryContext context = mock(TableSourceFactoryContext.class);
        when(context.getOptions()).thenReturn(readonlyConfig);

        BigQuerySourceFactory factory = new BigQuerySourceFactory();
        BigQueryConnectorException ex =
                assertThrows(BigQueryConnectorException.class, () -> factory.createSource(context));
        assertTrue(ex.getMessage().contains("only valid"));
    }

    @Test
    public void testPartitionFilterAppendedToTableQuery() {
        Config config =
                ConfigFactory.parseString(
                        BigQuerySourceOptions.PROJECT_ID.key()
                                + " = \"proj\"\n"
                                + BigQuerySourceOptions.DATASET_ID.key()
                                + " = \"ds\"\n"
                                + BigQuerySourceOptions.TABLE_ID.key()
                                + " = \"tbl\"\n"
                                + BigQuerySourceOptions.PARTITION_FILTER.key()
                                + " = \"_PARTITIONDATE >= '2026-01-01'\"\n");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);

        BigQuerySourceFactory factory = new BigQuerySourceFactory();
        String query = factory.resolveQuery(readonlyConfig);
        assertTrue(query.contains("WHERE"));
        assertTrue(query.contains("_PARTITIONDATE >= '2026-01-01'"));
    }

    @Test
    public void testTableQueryWithoutPartitionFilterHasNoWhereClause() {
        Config config =
                ConfigFactory.parseString(
                        BigQuerySourceOptions.PROJECT_ID.key()
                                + " = \"proj\"\n"
                                + BigQuerySourceOptions.DATASET_ID.key()
                                + " = \"ds\"\n"
                                + BigQuerySourceOptions.TABLE_ID.key()
                                + " = \"tbl\"\n");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);

        BigQuerySourceFactory factory = new BigQuerySourceFactory();
        String query = factory.resolveQuery(readonlyConfig);
        assertTrue(!query.contains("WHERE"));
    }

    @Test
    public void testRequirePartitionFilterMissing() {
        Table table = mock(Table.class);
        when(table.getRequirePartitionFilter()).thenReturn(true);

        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () ->
                                BigQuerySourceFactory.checkRequirePartitionFilter(
                                        table, null, "proj.ds.tbl"));
        assertTrue(ex.getMessage().contains("requirePartitionFilter"));
    }

    @Test
    public void testRequirePartitionFilterSatisfied() {
        Table table = mock(Table.class);
        when(table.getRequirePartitionFilter()).thenReturn(true);

        assertDoesNotThrow(
                () ->
                        BigQuerySourceFactory.checkRequirePartitionFilter(
                                table, "_PARTITIONDATE >= '2026-01-01'", "proj.ds.tbl"));
    }

    @Test
    public void testRequirePartitionFilterNotSetSkipsCheck() {
        Table table = mock(Table.class);
        when(table.getRequirePartitionFilter()).thenReturn(null);
        StandardTableDefinition definition = mock(StandardTableDefinition.class);
        when(table.getDefinition()).thenReturn(definition);
        when(definition.getTimePartitioning()).thenReturn(null);

        assertDoesNotThrow(
                () ->
                        BigQuerySourceFactory.checkRequirePartitionFilter(
                                table, null, "proj.ds.tbl"));
    }

    @Test
    public void testRequirePartitionFilterNullTableSkipsCheck() {
        assertDoesNotThrow(
                () -> BigQuerySourceFactory.checkRequirePartitionFilter(null, null, "proj.ds.tbl"));
    }

    @Test
    public void testRequirePartitionFilterFromTableInfoOnly() {
        // Table has requirePartitionFilter on TableInfo but no TimePartitioning (e.g. range
        // partitioning). The top-level flag must be honored without the legacy fallback.
        Table table = mock(Table.class);
        when(table.getRequirePartitionFilter()).thenReturn(true);
        StandardTableDefinition definition = mock(StandardTableDefinition.class);
        when(table.getDefinition()).thenReturn(definition);
        when(definition.getTimePartitioning()).thenReturn(null);

        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () ->
                                BigQuerySourceFactory.checkRequirePartitionFilter(
                                        table, null, "proj.ds.tbl"));
        assertTrue(ex.getMessage().contains("requirePartitionFilter"));
    }

    @Test
    public void testRequirePartitionFilterLegacyFallback() {
        // Table has requirePartitionFilter only on the legacy TimePartitioning field
        // (TableInfo returns null). The fallback must catch it.
        Table table = mock(Table.class);
        when(table.getRequirePartitionFilter()).thenReturn(null);
        StandardTableDefinition definition = mock(StandardTableDefinition.class);
        when(table.getDefinition()).thenReturn(definition);
        TimePartitioning partitioning = mock(TimePartitioning.class);
        when(definition.getTimePartitioning()).thenReturn(partitioning);
        when(partitioning.getRequirePartitionFilter()).thenReturn(true);

        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () ->
                                BigQuerySourceFactory.checkRequirePartitionFilter(
                                        table, null, "proj.ds.tbl"));
        assertTrue(ex.getMessage().contains("requirePartitionFilter"));
    }

    @Test
    public void testResolveFromQueryExtractsSchemaFromDryRunStatistics()
            throws BigQueryException, InterruptedException {
        // dry_run is NOT set (default false): planning must still discover the schema via a
        // zero-cost dry run, so the option only affects runtime execution, never planning.
        Config config =
                ConfigFactory.parseString(
                        BigQuerySourceOptions.PROJECT_ID.key()
                                + " = \"test-project\"\n"
                                + BigQuerySourceOptions.QUERY.key()
                                + " = \"SELECT id, name FROM `proj.ds.tbl`\"\n");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);

        Schema schema =
                Schema.of(
                        Field.of("id", StandardSQLTypeName.INT64),
                        Field.of("name", StandardSQLTypeName.STRING));

        BigQuery bigQuery = mock(BigQuery.class);
        Job job = mock(Job.class);
        JobStatistics.QueryStatistics stats = mock(JobStatistics.QueryStatistics.class);
        when(bigQuery.create(any(JobInfo.class))).thenReturn(job);
        when(job.getStatistics()).thenReturn(stats);
        when(stats.getEstimatedBytesProcessed()).thenReturn(1000L);
        when(stats.getSchema()).thenReturn(schema);

        BigQuerySourceFactory factory = new BigQuerySourceFactory();
        CatalogTable catalogTable = factory.resolveFromQuery(readonlyConfig, bigQuery);

        // Schema from dry-run statistics should be used in the catalog table
        assertEquals(2, catalogTable.getTableSchema().getColumns().size());
        assertEquals("id", catalogTable.getTableSchema().getColumns().get(0).getName());
        assertEquals("name", catalogTable.getTableSchema().getColumns().get(1).getName());

        // Planning must create a dry-run job and never execute a real query. Creating a job with
        // a non-dry-run configuration would also execute the query server-side, so the submitted
        // configuration itself must be asserted.
        org.mockito.ArgumentCaptor<JobInfo> jobInfoCaptor =
                org.mockito.ArgumentCaptor.forClass(JobInfo.class);
        verify(bigQuery).create(jobInfoCaptor.capture());
        QueryJobConfiguration capturedConfig =
                (QueryJobConfiguration) jobInfoCaptor.getValue().getConfiguration();
        assertTrue(capturedConfig.dryRun());
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testResolveFromQueryFallsBackToMaxResultsOneWhenNoSchema()
            throws BigQueryException, InterruptedException {
        // dry_run=true must go through the same unified path: option value cannot change planning
        Config config =
                ConfigFactory.parseString(
                        BigQuerySourceOptions.PROJECT_ID.key()
                                + " = \"test-project\"\n"
                                + BigQuerySourceOptions.QUERY.key()
                                + " = \"SELECT id FROM `proj.ds.tbl`\"\n"
                                + BigQuerySourceOptions.DRY_RUN.key()
                                + " = true\n");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);

        // Emulator scenario: dry run returns no schema
        Schema fallbackSchema = Schema.of(Field.of("id", StandardSQLTypeName.INT64));

        BigQuery bigQuery = mock(BigQuery.class);
        Job job = mock(Job.class);
        JobStatistics.QueryStatistics stats = mock(JobStatistics.QueryStatistics.class);
        TableResult tableResult = mock(TableResult.class);
        when(bigQuery.create(any(JobInfo.class))).thenReturn(job);
        when(job.getStatistics()).thenReturn(stats);
        when(stats.getEstimatedBytesProcessed()).thenReturn(1000L);
        when(stats.getSchema()).thenReturn(null);
        when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(tableResult);
        when(tableResult.getSchema()).thenReturn(fallbackSchema);

        BigQuerySourceFactory factory = new BigQuerySourceFactory();
        CatalogTable catalogTable = factory.resolveFromQuery(readonlyConfig, bigQuery);

        // Fallback query should have been called with maxResults=1
        org.mockito.ArgumentCaptor<QueryJobConfiguration> captor =
                org.mockito.ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(bigQuery).create(any(JobInfo.class));
        verify(bigQuery).query(captor.capture());
        assertEquals(1L, captor.getValue().getMaxResults().longValue());

        // Schema from fallback query should be used
        assertEquals(1, catalogTable.getTableSchema().getColumns().size());
        assertEquals("id", catalogTable.getTableSchema().getColumns().get(0).getName());
    }

    @Test
    public void testResolveFromQueryAppliesMaximumBytesBilledToDryRun()
            throws BigQueryException, InterruptedException {
        // The cost cap must guard the planning dry run in both modes, so the query is never
        // estimated, let alone executed, above the configured cap.
        Config config =
                ConfigFactory.parseString(
                        BigQuerySourceOptions.PROJECT_ID.key()
                                + " = \"test-project\"\n"
                                + BigQuerySourceOptions.QUERY.key()
                                + " = \"SELECT * FROM `proj.ds.tbl`\"\n"
                                + BigQuerySourceOptions.MAXIMUM_BYTES_BILLED.key()
                                + " = 1000000000\n");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);

        Schema schema = Schema.of(Field.of("id", StandardSQLTypeName.INT64));

        BigQuery bigQuery = mock(BigQuery.class);
        Job job = mock(Job.class);
        JobStatistics.QueryStatistics stats = mock(JobStatistics.QueryStatistics.class);
        when(bigQuery.create(any(JobInfo.class))).thenReturn(job);
        when(job.getStatistics()).thenReturn(stats);
        when(stats.getEstimatedBytesProcessed()).thenReturn(1000L);
        when(stats.getSchema()).thenReturn(schema);

        BigQuerySourceFactory factory = new BigQuerySourceFactory();
        factory.resolveFromQuery(readonlyConfig, bigQuery);

        org.mockito.ArgumentCaptor<JobInfo> jobInfoCaptor =
                org.mockito.ArgumentCaptor.forClass(JobInfo.class);
        verify(bigQuery).create(jobInfoCaptor.capture());
        QueryJobConfiguration capturedConfig =
                (QueryJobConfiguration) jobInfoCaptor.getValue().getConfiguration();
        assertTrue(capturedConfig.dryRun());
        assertEquals(1000000000L, capturedConfig.getMaximumBytesBilled().longValue());
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testResolveFromQueryFailsFastWhenNoSchemaAtAnyStage()
            throws BigQueryException, InterruptedException {
        // Both discovery stages returning no schema must fail planning instead of silently
        // producing a zero-column catalog table that only breaks far downstream.
        Config config =
                ConfigFactory.parseString(
                        BigQuerySourceOptions.PROJECT_ID.key()
                                + " = \"test-project\"\n"
                                + BigQuerySourceOptions.QUERY.key()
                                + " = \"SELECT id FROM `proj.ds.tbl`\"\n");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);

        BigQuery bigQuery = mock(BigQuery.class);
        Job job = mock(Job.class);
        JobStatistics.QueryStatistics stats = mock(JobStatistics.QueryStatistics.class);
        TableResult tableResult = mock(TableResult.class);
        when(bigQuery.create(any(JobInfo.class))).thenReturn(job);
        when(job.getStatistics()).thenReturn(stats);
        when(stats.getEstimatedBytesProcessed()).thenReturn(null);
        when(stats.getSchema()).thenReturn(null);
        when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(tableResult);
        when(tableResult.getSchema()).thenReturn(null);

        BigQuerySourceFactory factory = new BigQuerySourceFactory();
        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () -> factory.resolveFromQuery(readonlyConfig, bigQuery));

        assertTrue(ex.getMessage().contains("no schema"));
        // The fallback must have been attempted before failing
        verify(bigQuery).create(any(JobInfo.class));
        verify(bigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testResolveFromQueryFailsFastWhenSchemaHasNoFields()
            throws BigQueryException, InterruptedException {
        // A dry-run schema with an empty field list is as unusable as a missing schema
        // and must fail planning rather than produce a zero-column table.
        Config config =
                ConfigFactory.parseString(
                        BigQuerySourceOptions.PROJECT_ID.key()
                                + " = \"test-project\"\n"
                                + BigQuerySourceOptions.QUERY.key()
                                + " = \"SELECT id FROM `proj.ds.tbl`\"\n");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);

        BigQuery bigQuery = mock(BigQuery.class);
        Job job = mock(Job.class);
        JobStatistics.QueryStatistics stats = mock(JobStatistics.QueryStatistics.class);
        when(bigQuery.create(any(JobInfo.class))).thenReturn(job);
        when(job.getStatistics()).thenReturn(stats);
        when(stats.getEstimatedBytesProcessed()).thenReturn(1000L);
        when(stats.getSchema()).thenReturn(Schema.of());

        BigQuerySourceFactory factory = new BigQuerySourceFactory();
        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () -> factory.resolveFromQuery(readonlyConfig, bigQuery));

        assertTrue(ex.getMessage().contains("no schema"));
        // The fallback must NOT run: the dry run returned a (fieldless) schema
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }
}
