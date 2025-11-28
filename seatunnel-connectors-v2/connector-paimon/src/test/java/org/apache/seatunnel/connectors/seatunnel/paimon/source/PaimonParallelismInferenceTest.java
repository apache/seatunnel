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

package org.apache.seatunnel.connectors.seatunnel.paimon.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonBaseOptions;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PaimonParallelismInferenceTest {

    @TempDir private Path tempDir;

    private PaimonCatalog paimonCatalog;
    private String warehouse;
    private static final String CATALOG_NAME = "test_catalog";
    private static final String DATABASE = "test_db";

    @BeforeEach
    public void setUp() throws Exception {
        warehouse = new File(tempDir.toFile(), UUID.randomUUID().toString()).toString();
        paimonCatalog = createPaimonCatalog();
        paimonCatalog.open();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (paimonCatalog != null) {
            paimonCatalog.close();
        }
    }

    @Test
    public void testInferParallelismWithNonPartitionedTable() throws Exception {
        String table = "non_partitioned_table";
        int bucketCount = 4;
        createPaimonTable(table, bucketCount, null);
        writeTestDataToBuckets(table, bucketCount, 100);
        PaimonSource source = createPaimonSource(table);
        int parallelism = source.inferParallelism();
        Assertions.assertEquals(1, parallelism);
    }

    @Test
    public void testInferParallelismWithPartitionedTable() throws Exception {
        String table = "partitioned_table";
        int bucketCount = 2;
        createPaimonTable(table, bucketCount, "dt");
        writeTestDataToPartition(table, bucketCount, "2024-01-01", 50);
        writeTestDataToPartition(table, bucketCount, "2024-01-02", 50);
        writeTestDataToPartition(table, bucketCount, "2024-01-03", 50);

        PaimonSource source = createPaimonSource(table);
        int parallelism = source.inferParallelism();
        Assertions.assertEquals(3, parallelism);
    }

    @Test
    public void testInferParallelismWithEmptyTable() throws Exception {
        String table = "empty_table";
        createPaimonTable(table, 4, "dt");
        PaimonSource source = createPaimonSource(table);
        int parallelism = source.inferParallelism();
        Assertions.assertEquals(1, parallelism);
    }

    @Test
    public void testInferParallelismWithMultipleTables() throws Exception {
        // Create first table with 2 partitions
        String table1 = "multi_table_1";
        int bucketCount1 = 2;
        createPaimonTable(table1, bucketCount1, "dt");
        writeTestDataToPartition(table1, bucketCount1, "2024-01-01", 50);
        writeTestDataToPartition(table1, bucketCount1, "2024-01-02", 50);

        // Create second table with 3 partitions
        String table2 = "multi_table_2";
        int bucketCount2 = 2;
        createPaimonTable(table2, bucketCount2, "dt");
        writeTestDataToPartition(table2, bucketCount2, "2024-02-01", 50);
        writeTestDataToPartition(table2, bucketCount2, "2024-02-02", 50);
        writeTestDataToPartition(table2, bucketCount2, "2024-02-03", 50);

        // Create third table with 1 partition
        String table3 = "multi_table_3";
        int bucketCount3 = 2;
        createPaimonTable(table3, bucketCount3, "dt");
        writeTestDataToPartition(table3, bucketCount3, "2024-03-01", 50);

        PaimonSource source = createPaimonSourceWithMultipleTables(table1, table2, table3);
        int parallelism = source.inferParallelism();

        // Expected: 2 (table1) + 3 (table2) + 1 (table3) = 6
        Assertions.assertEquals(6, parallelism);
    }

    private PaimonCatalog createPaimonCatalog() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(PaimonBaseOptions.WAREHOUSE.key(), warehouse);
        properties.put(PaimonBaseOptions.DATABASE.key(), DATABASE);
        properties.put(PaimonBaseOptions.CATALOG_NAME.key(), CATALOG_NAME);
        return new PaimonCatalog(CATALOG_NAME, ReadonlyConfig.fromMap(properties));
    }

    private void createPaimonTable(String table, int bucketCount, String partitionKey)
            throws Exception {
        Identifier identifier = Identifier.create(DATABASE, table);
        Catalog catalog = (Catalog) ReflectionUtils.getField(paimonCatalog, "catalog").get();

        // Use Paimon native API to create table with partition
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("id", DataTypes.INT().notNull());
        schemaBuilder.column("name", DataTypes.STRING());
        schemaBuilder.column("age", DataTypes.INT());

        if (partitionKey != null) {
            schemaBuilder.column(partitionKey, DataTypes.STRING());
            schemaBuilder.partitionKeys(partitionKey);
            schemaBuilder.primaryKey("id", partitionKey);
        } else {
            schemaBuilder.primaryKey("id");
        }

        schemaBuilder.option("bucket", String.valueOf(bucketCount));
        schemaBuilder.option("bucket-key", "id");

        catalog.createDatabase(DATABASE, true);
        catalog.createTable(identifier, schemaBuilder.build(), false);
    }

    private void writeTestDataToBuckets(String table, int bucketCount, int rowCount)
            throws Exception {
        Identifier identifier = Identifier.create(DATABASE, table);
        Catalog catalog = (Catalog) ReflectionUtils.getField(paimonCatalog, "catalog").get();
        Table paimonTable = catalog.getTable(identifier);

        BatchWriteBuilder writeBuilder = paimonTable.newBatchWriteBuilder();
        BatchTableWrite writer = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();

        List<CommitMessage> messages = new ArrayList<>();

        try {
            int rowsPerBucket = Math.max(1, rowCount / bucketCount);
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                for (int i = 0; i < rowsPerBucket; i++) {
                    int id = bucket * rowsPerBucket + i;
                    GenericRow row =
                            GenericRow.of(
                                    id, BinaryString.fromString("user_" + id), 20 + (id % 50));
                    writer.write(row, bucket);
                }
            }
            messages.addAll(writer.prepareCommit());
            commit.commit(messages);
        } finally {
            writer.close();
            commit.close();
        }
    }

    private void writeTestDataToPartition(
            String table, int bucketCount, String partitionValue, int rowCount) throws Exception {
        Identifier identifier = Identifier.create(DATABASE, table);
        Catalog catalog = (Catalog) ReflectionUtils.getField(paimonCatalog, "catalog").get();
        Table paimonTable = catalog.getTable(identifier);

        // Create a new write session for each partition
        BatchWriteBuilder writeBuilder = paimonTable.newBatchWriteBuilder();
        BatchTableWrite writer = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();

        try {
            int rowsPerBucket = Math.max(1, rowCount / bucketCount);
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                for (int i = 0; i < rowsPerBucket; i++) {
                    int id =
                            bucket * rowsPerBucket
                                    + i
                                    + Math.abs(partitionValue.hashCode() % 10000);
                    GenericRow row =
                            GenericRow.of(
                                    id,
                                    BinaryString.fromString("user_" + id),
                                    20 + (id % 50),
                                    BinaryString.fromString(partitionValue));
                    writer.write(row, bucket);
                }
            }
            List<CommitMessage> messages = writer.prepareCommit();
            commit.commit(messages);
        } finally {
            writer.close();
            commit.close();
        }
    }

    private PaimonSource createPaimonSource(String table) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(PaimonBaseOptions.WAREHOUSE.key(), warehouse);
        configMap.put(PaimonBaseOptions.DATABASE.key(), DATABASE);
        configMap.put(PaimonBaseOptions.TABLE.key(), table);
        configMap.put(PaimonBaseOptions.CATALOG_NAME.key(), CATALOG_NAME);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        return new PaimonSource(config, paimonCatalog);
    }

    private PaimonSource createPaimonSourceWithMultipleTables(String... tables) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(PaimonBaseOptions.WAREHOUSE.key(), warehouse);
        configMap.put(PaimonBaseOptions.CATALOG_NAME.key(), CATALOG_NAME);

        List<Map<String, Object>> tableList = new ArrayList<>();
        for (String table : tables) {
            Map<String, Object> tableConfig = new HashMap<>();
            tableConfig.put(PaimonBaseOptions.DATABASE.key(), DATABASE);
            tableConfig.put(PaimonBaseOptions.TABLE.key(), table);
            tableList.add(tableConfig);
        }
        configMap.put("table_list", tableList);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        return new PaimonSource(config, paimonCatalog);
    }
}
