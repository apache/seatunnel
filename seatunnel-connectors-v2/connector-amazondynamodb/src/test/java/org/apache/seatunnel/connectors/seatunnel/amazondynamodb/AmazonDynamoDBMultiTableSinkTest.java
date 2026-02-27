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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBConfig;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.sink.AmazonDynamoDBSink;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.sink.AmazonDynamoDBWriter;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.sink.DynamoDbSinkClient;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AmazonDynamoDBMultiTableSinkTest {

    private AmazonDynamoDBConfig config;
    private CatalogTable catalogTable;
    private DynamoDbClient mockDynamoDbClient;

    @BeforeEach
    public void setup() {
        config = createTestConfig("default_table");
        catalogTable = createTestCatalogTable("default_table");
        mockDynamoDbClient = mock(DynamoDbClient.class);

        when(mockDynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(
                        BatchWriteItemResponse.builder()
                                .unprocessedItems(Collections.emptyMap())
                                .build());
    }

    @Test
    public void testSinkImplementsMultiTableSinkInterface() {
        Assertions.assertTrue(
                SupportMultiTableSink.class.isAssignableFrom(AmazonDynamoDBSink.class),
                "AmazonDynamoDBSink must implement SupportMultiTableSink");
    }

    @Test
    public void testWriterImplementsMultiTableSinkWriterInterface() {
        Assertions.assertTrue(
                SupportMultiTableSinkWriter.class.isAssignableFrom(AmazonDynamoDBWriter.class),
                "AmazonDynamoDBWriter must implement SupportMultiTableSinkWriter");
    }

    @Test
    public void testRowWithTableIdIsRoutedToCorrectTable() throws Exception {
        DynamoDbSinkClient sinkClient = new DynamoDbSinkClient(config, mockDynamoDbClient);
        AmazonDynamoDBWriter writer = new AmazonDynamoDBWriter(config, catalogTable, sinkClient);

        SeaTunnelRow row = createTestRow(1, "alice");
        row.setTableId("users");

        writer.write(row);
        writer.prepareCommit();

        ArgumentCaptor<BatchWriteItemRequest> captor =
                ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        verify(mockDynamoDbClient, atLeastOnce()).batchWriteItem(captor.capture());
        Assertions.assertTrue(captor.getValue().requestItems().containsKey("users"));
    }

    @Test
    public void testRowWithEmptyTableIdFallsBackToConfigTable() throws Exception {
        DynamoDbSinkClient sinkClient = new DynamoDbSinkClient(config, mockDynamoDbClient);
        AmazonDynamoDBWriter writer = new AmazonDynamoDBWriter(config, catalogTable, sinkClient);

        SeaTunnelRow row = createTestRow(1, "alice");
        row.setTableId("");

        writer.write(row);
        writer.prepareCommit();

        ArgumentCaptor<BatchWriteItemRequest> captor =
                ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        verify(mockDynamoDbClient, atLeastOnce()).batchWriteItem(captor.capture());
        Assertions.assertTrue(captor.getValue().requestItems().containsKey("default_table"));
    }

    @Test
    public void testRowWithNullTableIdFallsBackToConfigTable() throws Exception {
        DynamoDbSinkClient sinkClient = new DynamoDbSinkClient(config, mockDynamoDbClient);
        AmazonDynamoDBWriter writer = new AmazonDynamoDBWriter(config, catalogTable, sinkClient);

        SeaTunnelRow row = createTestRow(1, "alice");
        row.setTableId(null);

        writer.write(row);
        writer.prepareCommit();

        ArgumentCaptor<BatchWriteItemRequest> captor =
                ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        verify(mockDynamoDbClient, atLeastOnce()).batchWriteItem(captor.capture());
        Assertions.assertTrue(captor.getValue().requestItems().containsKey("default_table"));
    }

    @Test
    public void testRowsAreGroupedByTableBeforeWriting() throws Exception {
        DynamoDbSinkClient sinkClient = new DynamoDbSinkClient(config, mockDynamoDbClient);
        AmazonDynamoDBWriter writer = new AmazonDynamoDBWriter(config, catalogTable, sinkClient);

        SeaTunnelRow row1 = createTestRow(1, "alice");
        row1.setTableId("users");

        SeaTunnelRow row2 = createTestRow(2, "order-1");
        row2.setTableId("orders");

        SeaTunnelRow row3 = createTestRow(3, "bob");
        row3.setTableId("users");

        writer.write(row1);
        writer.write(row2);
        writer.write(row3);
        writer.prepareCommit();

        ArgumentCaptor<BatchWriteItemRequest> captor =
                ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        verify(mockDynamoDbClient, atLeastOnce()).batchWriteItem(captor.capture());

        // Count total rows written per table across all batch calls
        Map<String, Integer> rowCountByTable = new HashMap<>();
        for (BatchWriteItemRequest request : captor.getAllValues()) {
            for (Map.Entry<String, List<WriteRequest>> entry : request.requestItems().entrySet()) {
                rowCountByTable.merge(entry.getKey(), entry.getValue().size(), Integer::sum);
            }
        }

        Assertions.assertEquals(2, rowCountByTable.getOrDefault("users", 0));
        Assertions.assertEquals(1, rowCountByTable.getOrDefault("orders", 0));
    }

    @Test
    public void testUnprocessedItemsAreRetriedUntilSuccess() throws Exception {
        DynamoDbSinkClient client = new DynamoDbSinkClient(config, mockDynamoDbClient);

        client.write(
                PutItemRequest.builder().tableName("test_table").item(createTestItem()).build(),
                "test_table");

        Map<String, List<WriteRequest>> unprocessed = new HashMap<>();
        unprocessed.put(
                "test_table",
                Collections.singletonList(
                        WriteRequest.builder().putRequest(b -> b.item(createTestItem())).build()));

        // First call returns unprocessed items, second call succeeds
        when(mockDynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(BatchWriteItemResponse.builder().unprocessedItems(unprocessed).build())
                .thenReturn(
                        BatchWriteItemResponse.builder()
                                .unprocessedItems(Collections.emptyMap())
                                .build());

        client.close();

        verify(mockDynamoDbClient, times(2)).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    public void testExceptionIsThrownWhenMaxRetriesExceeded() throws Exception {
        DynamoDbSinkClient client = new DynamoDbSinkClient(config, mockDynamoDbClient);

        client.write(
                PutItemRequest.builder().tableName("test_table").item(createTestItem()).build(),
                "test_table");

        Map<String, List<WriteRequest>> unprocessed = new HashMap<>();
        unprocessed.put(
                "test_table",
                Collections.singletonList(
                        WriteRequest.builder().putRequest(b -> b.item(createTestItem())).build()));

        // Always fail — retries will be exhausted
        when(mockDynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(BatchWriteItemResponse.builder().unprocessedItems(unprocessed).build());

        Assertions.assertThrows(RuntimeException.class, client::close);
    }

    private AmazonDynamoDBConfig createTestConfig(String tableName) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(AmazonDynamoDBSinkOptions.URL.key(), "http://localhost:8000");
        configMap.put(AmazonDynamoDBSinkOptions.REGION.key(), "us-east-1");
        configMap.put(AmazonDynamoDBSinkOptions.ACCESS_KEY_ID.key(), "test-key");
        configMap.put(AmazonDynamoDBSinkOptions.SECRET_ACCESS_KEY.key(), "test-secret");
        configMap.put(AmazonDynamoDBSinkOptions.TABLE.key(), tableName);
        configMap.put(AmazonDynamoDBSinkOptions.BATCH_SIZE.key(), 25);
        configMap.put(AmazonDynamoDBSinkOptions.MAX_RETRIES.key(), 2);
        configMap.put(AmazonDynamoDBSinkOptions.RETRY_BASE_DELAY_MS.key(), 1L);
        return new AmazonDynamoDBConfig(ReadonlyConfig.fromMap(configMap));
    }

    private CatalogTable createTestCatalogTable(String tableName) {

        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, (Long) null, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "name",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        null))
                        .primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")))
                        .build();

        return CatalogTable.of(
                TableIdentifier.of("catalog", TablePath.of("default", tableName)),
                schema,
                new HashMap<>(),
                new ArrayList<>(),
                "Test table");
    }

    private SeaTunnelRow createTestRow(int id, String name) {
        return new SeaTunnelRow(new Object[] {id, name});
    }

    private Map<String, AttributeValue> createTestItem() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().n("1").build());
        item.put("name", AttributeValue.builder().s("test").build());
        return item;
    }
}
