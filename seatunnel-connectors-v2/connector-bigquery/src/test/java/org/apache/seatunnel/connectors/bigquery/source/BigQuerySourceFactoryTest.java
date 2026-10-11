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
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySourceOptions;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TimePartitioning;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
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
        StandardTableDefinition definition = mock(StandardTableDefinition.class);
        TimePartitioning partitioning = mock(TimePartitioning.class);
        when(table.getDefinition()).thenReturn(definition);
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
    public void testRequirePartitionFilterSatisfied() {
        Table table = mock(Table.class);
        StandardTableDefinition definition = mock(StandardTableDefinition.class);
        TimePartitioning partitioning = mock(TimePartitioning.class);
        when(table.getDefinition()).thenReturn(definition);
        when(definition.getTimePartitioning()).thenReturn(partitioning);
        when(partitioning.getRequirePartitionFilter()).thenReturn(true);

        assertDoesNotThrow(
                () ->
                        BigQuerySourceFactory.checkRequirePartitionFilter(
                                table, "_PARTITIONDATE >= '2026-01-01'", "proj.ds.tbl"));
    }

    @Test
    public void testRequirePartitionFilterNotSetSkipsCheck() {
        Table table = mock(Table.class);
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
}
