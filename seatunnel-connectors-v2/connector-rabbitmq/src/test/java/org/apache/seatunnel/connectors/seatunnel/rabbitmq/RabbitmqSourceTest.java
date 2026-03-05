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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.source.RabbitmqSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RabbitmqSourceTest {

    /**
     * Test the initialization of the RabbitMQ source with multiple tables. Verifies that: 1. The
     * correct number of tables is created. 2. Each table has the correct Table Name (based on our
     * new logic, it should match queue_name). 3. Each table has the correct Schema (columns).
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
        schema1.put("fields", Collections.singletonMap("username", "string"));
        table1.put("schema", schema1);

        // 2. Table B Config (Order Table)
        Map<String, Object> table2 = new HashMap<>();
        table2.put("queue_name", "queue_order");
        Map<String, Object> schema2 = new HashMap<>();
        schema2.put("fields", Collections.singletonMap("amount", "int"));
        table2.put("schema", schema2);

        configMap.put(TableSchemaOptions.TABLE_CONFIGS.key(), Arrays.asList(table1, table2));

        RabbitmqSource source = new RabbitmqSource(ReadonlyConfig.fromMap(configMap));
        List<CatalogTable> tables = source.getProducedCatalogTables();

        Assertions.assertNotNull(tables);
        Assertions.assertEquals(2, tables.size());

        // --- Deep Verification ---
        // Based on our NEW logic, the TableName should equal the queue_name!
        // Check Table 1
        CatalogTable t1 = tables.get(0);
        Assertions.assertEquals("queue_user", t1.getTableId().getTableName());
        Assertions.assertArrayEquals(
                new String[] {"username"}, t1.getTableSchema().getFieldNames());

        // Check Table 2
        CatalogTable t2 = tables.get(1);
        Assertions.assertEquals("queue_order", t2.getTableId().getTableName());
        Assertions.assertArrayEquals(new String[] {"amount"}, t2.getTableSchema().getFieldNames());
    }

    /**
     * Tests Backward Compatibility (Legacy Mode). Ensures that providing a global queue_name and
     * schema block results in a single CatalogTable with the correct TableName.
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
        // Based on our NEW logic, the TableName should match the global queue_name
        Assertions.assertEquals("legacy_queue", tables.get(0).getTableId().getTableName());
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
        configMap.put(TableSchemaOptions.TABLE_CONFIGS.key(), Arrays.asList(table1));

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

    /**
     * Test Fallback Scenario: Missing 'queue_name' in table config. This test verifies that if a
     * user forgets to specify a 'queue_name' inside 'table_configs', the Source correctly falls
     * back to the global queue_name to build the CatalogTable.
     */
    @Test
    public void testTableConfigWithoutQueueName() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RabbitmqBaseOptions.HOST.key(), "localhost");
        configMap.put(RabbitmqBaseOptions.PORT.key(), 5672);

        // Define a Global Queue (This acts as the fallback)
        configMap.put(RabbitmqBaseOptions.QUEUE_NAME.key(), "global_default_queue");

        // Define a Table Config that relies on the global queue (NO 'queue_name' key here)
        Map<String, Object> table1 = new HashMap<>();

        // Setup Schema
        Map<String, Object> schema1 = new HashMap<>();
        schema1.put("fields", Collections.singletonMap("id", "int"));
        table1.put("schema", schema1);

        // Add to config
        configMap.put(TableSchemaOptions.TABLE_CONFIGS.key(), Collections.singletonList(table1));

        // Create Source
        RabbitmqSource source = new RabbitmqSource(ReadonlyConfig.fromMap(configMap));
        List<CatalogTable> tables = source.getProducedCatalogTables();

        // Verifications
        Assertions.assertNotNull(tables);
        Assertions.assertEquals(1, tables.size());

        CatalogTable t1 = tables.get(0);
        // It should have fallen back to the global queue name!
        Assertions.assertEquals("global_default_queue", t1.getTableId().getTableName());
    }

    @Test
    public void testTableConfigMissingSchemaThrowsException() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RabbitmqBaseOptions.HOST.key(), "localhost");

        Map<String, Object> table1 = new HashMap<>();
        table1.put("queue_name", "q1");
        // table1.put("schema", ...)

        configMap.put(TableSchemaOptions.TABLE_CONFIGS.key(), Collections.singletonList(table1));

        Assertions.assertThrows(
                Exception.class,
                () -> new RabbitmqSource(ReadonlyConfig.fromMap(configMap)),
                "Should fail when table_configs is missing the schema block");
    }
}
