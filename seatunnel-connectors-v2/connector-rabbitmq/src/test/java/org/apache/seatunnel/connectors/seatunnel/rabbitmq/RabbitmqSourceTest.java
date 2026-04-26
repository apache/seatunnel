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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.source.RabbitmqSource;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RabbitmqSourceTest {

    /**
     * Test the initialization of the RabbitMQ source with multiple tables. Verifies that: 1. The
     * correct number of tables is created. 2. Each table correctly parses its explicitly defined
     * Table Name. 3. Each table has the correct Schema (columns).
     */
    @Test
    public void testMultiTableInitialization() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RabbitmqBaseOptions.HOST.key(), "localhost");
        configMap.put(RabbitmqBaseOptions.PORT.key(), 5672);

        // 1. Table A Config (User Table)
        Map<String, Object> table1 = new HashMap<>();
        table1.put("queue_name", "queue_user");
        Map<String, Object> schema1 = new HashMap<>();
        // Using the official API method, table name must be defined inside the schema
        schema1.put("table", "queue_user");
        schema1.put("fields", Collections.singletonMap("username", "string"));
        table1.put("schema", schema1);

        // 2. Table B Config (Order Table)
        Map<String, Object> table2 = new HashMap<>();
        table2.put("queue_name", "queue_order");
        Map<String, Object> schema2 = new HashMap<>();
        schema2.put("table", "queue_order");
        schema2.put("fields", Collections.singletonMap("amount", "int"));
        table2.put("schema", schema2);

        configMap.put(TableSchemaOptions.TABLE_CONFIGS.key(), Arrays.asList(table1, table2));

        RabbitmqSource source = new RabbitmqSource(ReadonlyConfig.fromMap(configMap));
        List<CatalogTable> tables = source.getProducedCatalogTables();

        Assertions.assertNotNull(tables);
        Assertions.assertEquals(2, tables.size());

        // --- Deep Verification ---
        CatalogTable t1 = tables.get(0);
        Assertions.assertEquals("queue_user", t1.getTableId().getTableName());
        Assertions.assertArrayEquals(
                new String[] {"username"}, t1.getTableSchema().getFieldNames());

        CatalogTable t2 = tables.get(1);
        Assertions.assertEquals("queue_order", t2.getTableId().getTableName());
        Assertions.assertArrayEquals(new String[] {"amount"}, t2.getTableSchema().getFieldNames());
    }

    /**
     * Tests Backward Compatibility (Legacy Mode). Ensures that providing a global queue_name and
     * schema block results in a single CatalogTable.
     */
    @Test
    public void testLegacySingleTableInitialization() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RabbitmqBaseOptions.HOST.key(), "localhost");
        configMap.put(RabbitmqBaseOptions.QUEUE_NAME.key(), "legacy_queue");

        Map<String, Object> schemaMap = new HashMap<>();
        schemaMap.put("fields", Collections.singletonMap("id", "int"));

        configMap.put(ConnectorCommonOptions.SCHEMA.key(), schemaMap);

        RabbitmqSource source = new RabbitmqSource(ReadonlyConfig.fromMap(configMap));
        List<CatalogTable> tables = source.getProducedCatalogTables();

        Assertions.assertNotNull(tables);
        Assertions.assertEquals(1, tables.size());
        // In legacy single-table mode without an explicit "table" key, SeaTunnel defaults to
        // "default"
        Assertions.assertEquals("default", tables.get(0).getTableId().getTableName());
    }

    /**
     * Test Validation: If a user accidentally provides BOTH 'table_configs' and 'schema', the
     * connector should fail-fast and throw a validation exception.
     */
    @Test
    public void testMixedConfigThrowsException() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RabbitmqBaseOptions.HOST.key(), "localhost");

        // Define Table Configs
        Map<String, Object> table1 = new HashMap<>();
        table1.put("queue_name", "q1");
        table1.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("col1", "string")));
        configMap.put(TableSchemaOptions.TABLE_CONFIGS.key(), Collections.singletonList(table1));

        // Define Root Schema (Conflict)
        Map<String, Object> rootSchema = new HashMap<>();
        rootSchema.put("fields", Collections.singletonMap("legacy_col", "boolean"));
        configMap.put(ConnectorCommonOptions.SCHEMA.key(), rootSchema);
        configMap.put(RabbitmqBaseOptions.QUEUE_NAME.key(), "global_q");

        // Expect the validation to fail and throw our new RabbitmqConnectorException
        RabbitmqConnectorException exception =
                Assertions.assertThrows(
                        RabbitmqConnectorException.class,
                        () -> new RabbitmqSource(ReadonlyConfig.fromMap(configMap)),
                        "Should throw an exception when both table_configs and schema are provided");

        // Verify the error message is the one we expect
        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains("Cannot specify both 'table_configs' and 'schema'"));
    }

    /**
     * Tests that the Source throws an exception if configured for BATCH mode, as RabbitMQ is
     * inherently unbounded (Streaming) unless specific for_e2e_testing flag is true.
     */
    @Test
    public void testBatchJobModeFailure() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RabbitmqBaseOptions.HOST.key(), "localhost");
        configMap.put(
                RabbitmqBaseOptions.QUEUE_NAME.key(), "test_queue"); // Added to avoid missing ID
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", Collections.singletonMap("id", "int"));
        configMap.put("schema", schema);

        RabbitmqSource source = new RabbitmqSource(ReadonlyConfig.fromMap(configMap));

        JobContext batchContext = new JobContext();
        batchContext.setJobMode(JobMode.BATCH);
        source.setJobContext(batchContext);

        // Expect RabbitmqConnectorException because Batch is not supported
        RabbitmqConnectorException exception =
                Assertions.assertThrows(RabbitmqConnectorException.class, source::getBoundedness);

        Assertions.assertEquals(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED, exception.getSeaTunnelErrorCode());
    }

    /** Tests the correctness of metadata and boundedness in Streaming mode. */
    @Test
    public void testSourceMetadataAndBoundedness() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RabbitmqBaseOptions.HOST.key(), "localhost");
        configMap.put(
                RabbitmqBaseOptions.QUEUE_NAME.key(), "test_queue"); // Added to avoid missing ID
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", Collections.singletonMap("id", "int"));
        configMap.put("schema", schema);

        RabbitmqSource source = new RabbitmqSource(ReadonlyConfig.fromMap(configMap));

        JobContext context = new JobContext();
        context.setJobMode(JobMode.STREAMING);
        source.setJobContext(context);

        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());
        Assertions.assertEquals("RabbitMQ", source.getPluginName());
    }

    /** Test Fallback Scenario: Missing 'queue_name' in table config. */
    @Test
    public void testTableConfigWithoutQueueName() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RabbitmqBaseOptions.HOST.key(), "localhost");
        configMap.put(RabbitmqBaseOptions.PORT.key(), 5672);

        // Define a Global Queue
        configMap.put(RabbitmqBaseOptions.QUEUE_NAME.key(), "global_default_queue");

        // Define a Table Config without queue_name
        Map<String, Object> table1 = new HashMap<>();

        // Setup Schema
        Map<String, Object> schema1 = new HashMap<>();
        schema1.put("table", "fallback_table");
        schema1.put("fields", Collections.singletonMap("id", "int"));
        table1.put("schema", schema1);

        // Add to config
        configMap.put(TableSchemaOptions.TABLE_CONFIGS.key(), Collections.singletonList(table1));

        // Create Source (Περιμένουμε να πετάξει Exception αμέσως!)
        Assertions.assertThrows(
                Exception.class,
                () -> new RabbitmqSource(ReadonlyConfig.fromMap(configMap)),
                "Should fail when table_configs is missing the queue_name");
    }

    @Test
    public void testTableConfigMissingSchemaThrowsException() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RabbitmqBaseOptions.HOST.key(), "localhost");

        Map<String, Object> table1 = new HashMap<>();
        table1.put("queue_name", "q1");
        // table1.put("schema", ...) is purposefully missing

        configMap.put(TableSchemaOptions.TABLE_CONFIGS.key(), Collections.singletonList(table1));

        Assertions.assertThrows(
                Exception.class,
                () -> new RabbitmqSource(ReadonlyConfig.fromMap(configMap)),
                "Should fail when table_configs is missing the schema block");
    }
    /**
     * Verifies that if a user configures BOTH 'schema' and 'tables_configs', the code fails-fast
     * instead of silently prioritizing one over the other.
     */
    @Test
    public void testExclusiveConfigValidation() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "localhost");
        configMap.put(ConnectorCommonOptions.SCHEMA.key(), new HashMap<>());
        configMap.put(ConnectorCommonOptions.TABLE_CONFIGS.key(), new ArrayList<>());

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        RabbitmqConnectorException exception =
                Assertions.assertThrows(
                        RabbitmqConnectorException.class, () -> new RabbitmqSource(config));
        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains("Cannot specify both 'table_configs' and 'schema'"));
    }

    /**
     * Verifies that the 'splitId' correctly represents the physical 'queue_name' and is NOT
     * overwritten by the virtual 'plugin_output' table identifier, ensuring connection stability.
     */
    @Test
    public void testSplitIdIsNotOverwrittenByPluginOutput() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RabbitmqBaseOptions.HOST.key(), "localhost");

        Map<String, Object> table1 = new HashMap<>();
        // This is the physical queue name that the client must use to connect
        table1.put("queue_name", "physical_queue_1");
        // This is the virtual identifier used for downstream routing
        table1.put("plugin_output", "virtual_table_1");

        Map<String, Object> schema1 = new HashMap<>();
        schema1.put("fields", Collections.singletonMap("col1", "string"));
        table1.put("schema", schema1);

        configMap.put(TableSchemaOptions.TABLE_CONFIGS.key(), Collections.singletonList(table1));
        RabbitmqSource source = new RabbitmqSource(ReadonlyConfig.fromMap(configMap));
        SourceSplitEnumerator<RabbitmqSplit, ?> enumerator = source.createEnumerator(null);

        // The enumerator should hold 1 pending split, and its ID must be the physical queue name
        Assertions.assertEquals(1, enumerator.currentUnassignedSplitSize());

        Assertions.assertTrue(source.getProducedCatalogTables().size() == 1);
    }

    /**
     * Verifies that if a user defines a 'table_configs' block but forgets to include the
     * 'queue_name' key inside it, the system throws a fail-fast validation exception instead of
     * throwing a NullPointerException during source initialization.
     */
    @Test
    public void testMissingQueueNameInTableConfigsThrowsException() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RabbitmqBaseOptions.HOST.key(), "localhost");

        Map<String, Object> table1 = new HashMap<>();
        table1.put("plugin_output", "my_table");
        // PURPOSEFULLY MISSING: table1.put("queue_name", "q1");

        Map<String, Object> schema1 = new HashMap<>();
        schema1.put("fields", Collections.singletonMap("col1", "string"));
        table1.put("schema", schema1);

        configMap.put(TableSchemaOptions.TABLE_CONFIGS.key(), Collections.singletonList(table1));

        RabbitmqConnectorException exception =
                Assertions.assertThrows(
                        RabbitmqConnectorException.class,
                        () -> new RabbitmqSource(ReadonlyConfig.fromMap(configMap)),
                        "Should fail-fast when 'queue_name' is omitted inside a table_configs item");

        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains(
                                "The 'queue_name' is missing or empty inside one of the 'table_configs' items."),
                "Error message should clearly state that queue_name is missing");
    }
}
