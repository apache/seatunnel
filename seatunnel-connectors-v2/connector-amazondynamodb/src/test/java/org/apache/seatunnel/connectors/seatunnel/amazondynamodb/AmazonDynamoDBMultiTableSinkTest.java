/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.sink;

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

        // Mock successful batch write by default
        BatchWriteItemResponse mockResponse =
                BatchWriteItemResponse.builder().unprocessedItems(Collections.emptyMap()).build();
        when(mockDynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(mockResponse);
    }

    @Test
    public void testSinkImplementsMultiTableInterface() {
        Assertions.assertTrue(
                SupportMultiTableSink.class.isAssignableFrom(AmazonDynamoDBSink.class));
    }

    @Test
    public void testWriterImplementsMultiTableInterface() {
        Assertions.assertTrue(
                SupportMultiTableSinkWriter.class.isAssignableFrom(AmazonDynamoDBWriter.class));
    }

    @Test
    public void testEmptyTableIdFallback() throws Exception {
        // Inject mock client using protected constructor
        DynamoDbSinkClient sinkClient = new DynamoDbSinkClient(config, mockDynamoDbClient);
        AmazonDynamoDBWriter writer = new AmazonDynamoDBWriter(config, catalogTable, sinkClient);

        SeaTunnelRow row = createTestRow(1, "test");
        row.setTableId("");

        writer.write(row);
        writer.prepareCommit(); // Triggers flush()

        ArgumentCaptor<BatchWriteItemRequest> captor =
                ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        verify(mockDynamoDbClient, atLeastOnce()).batchWriteItem(captor.capture());

        BatchWriteItemRequest request = captor.getValue();
        Assertions.assertTrue(request.requestItems().containsKey("default_table"));
    }

    @Test
    public void testNullTableIdFallback() throws Exception {
        DynamoDbSinkClient sinkClient = new DynamoDbSinkClient(config, mockDynamoDbClient);
        AmazonDynamoDBWriter writer = new AmazonDynamoDBWriter(config, catalogTable, sinkClient);

        SeaTunnelRow row = createTestRow(1, "test");
        row.setTableId(null);

        writer.write(row);
        writer.prepareCommit();

        ArgumentCaptor<BatchWriteItemRequest> captor =
                ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        verify(mockDynamoDbClient, atLeastOnce()).batchWriteItem(captor.capture());

        BatchWriteItemRequest request = captor.getValue();
        Assertions.assertTrue(request.requestItems().containsKey("default_table"));
    }

    @Test
    public void testMultiTableWrite() throws Exception {
        DynamoDbSinkClient sinkClient = new DynamoDbSinkClient(config, mockDynamoDbClient);
        AmazonDynamoDBWriter writer = new AmazonDynamoDBWriter(config, catalogTable, sinkClient);

        SeaTunnelRow row1 = createTestRow(1, "user1");
        row1.setTableId("users");

        SeaTunnelRow row2 = createTestRow(2, "order1");
        row2.setTableId("orders");

        SeaTunnelRow row3 = createTestRow(3, "user2");
        row3.setTableId("users");

        writer.write(row1);
        writer.write(row2);
        writer.write(row3);
        writer.prepareCommit();

        ArgumentCaptor<BatchWriteItemRequest> captor =
                ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        verify(mockDynamoDbClient, atLeastOnce()).batchWriteItem(captor.capture());

        List<BatchWriteItemRequest> requests = captor.getAllValues();
        Map<String, Integer> tableCounts = new HashMap<>();

        for (BatchWriteItemRequest request : requests) {
            for (Map.Entry<String, List<WriteRequest>> entry : request.requestItems().entrySet()) {
                tableCounts.put(
                        entry.getKey(),
                        tableCounts.getOrDefault(entry.getKey(), 0) + entry.getValue().size());
            }
        }

        Assertions.assertEquals(2, tableCounts.getOrDefault("users", 0));
        Assertions.assertEquals(1, tableCounts.getOrDefault("orders", 0));
    }

    @Test
    public void testUnprocessedKeysRetry() throws Exception {
        // Test Client directly without Writer to isolate retry logic
        DynamoDbSinkClient client = new DynamoDbSinkClient(config, mockDynamoDbClient);

        PutItemRequest putRequest =
                PutItemRequest.builder().tableName("test_table").item(createTestItem()).build();
        client.write(putRequest, "test_table");

        WriteRequest unprocessedRequest =
                WriteRequest.builder()
                        .putRequest(builder -> builder.item(createTestItem()))
                        .build();

        Map<String, List<WriteRequest>> unprocessedItems = new HashMap<>();
        unprocessedItems.put("test_table", Collections.singletonList(unprocessedRequest));

        BatchWriteItemResponse firstResponse =
                BatchWriteItemResponse.builder().unprocessedItems(unprocessedItems).build();

        BatchWriteItemResponse secondResponse =
                BatchWriteItemResponse.builder().unprocessedItems(Collections.emptyMap()).build();

        // First call returns unprocessed items, second call succeeds
        when(mockDynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(firstResponse)
                .thenReturn(secondResponse);

        client.close(); // calls flush()

        // Verify batchWriteItem was called twice (initial + 1 retry)
        verify(mockDynamoDbClient, times(2)).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    public void testUnprocessedKeysMaxRetriesExceeded() throws Exception {
        DynamoDbSinkClient client = new DynamoDbSinkClient(config, mockDynamoDbClient);

        PutItemRequest putRequest =
                PutItemRequest.builder().tableName("test_table").item(createTestItem()).build();
        client.write(putRequest, "test_table");

        WriteRequest unprocessedRequest =
                WriteRequest.builder()
                        .putRequest(builder -> builder.item(createTestItem()))
                        .build();

        Map<String, List<WriteRequest>> unprocessedItems = new HashMap<>();
        unprocessedItems.put("test_table", Collections.singletonList(unprocessedRequest));

        BatchWriteItemResponse response =
                BatchWriteItemResponse.builder().unprocessedItems(unprocessedItems).build();

        // Always return unprocessed items
        when(mockDynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(response);

        // Expect RuntimeException after retries exhausted
        Assertions.assertThrows(
                RuntimeException.class, client::close // calls flush()
                );
    }

    private AmazonDynamoDBConfig createTestConfig(String tableName) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(AmazonDynamoDBSinkOptions.URL.key(), "http://localhost:8000");
        configMap.put(AmazonDynamoDBSinkOptions.REGION.key(), "us-east-1");
        configMap.put(AmazonDynamoDBSinkOptions.ACCESS_KEY_ID.key(), "test");
        configMap.put(AmazonDynamoDBSinkOptions.SECRET_ACCESS_KEY.key(), "test");
        configMap.put(AmazonDynamoDBSinkOptions.TABLE.key(), tableName);
        configMap.put(AmazonDynamoDBSinkOptions.BATCH_SIZE.key(), 25);
        configMap.put(AmazonDynamoDBSinkOptions.MAX_RETRIES.key(), 2); // Low retry count for tests
        configMap.put(AmazonDynamoDBSinkOptions.RETRY_BASE_DELAY_MS.key(), 1L); // Fast retry

        return new AmazonDynamoDBConfig(ReadonlyConfig.fromMap(configMap));
    }

    private CatalogTable createTestCatalogTable(String tableName) {
        TablePath tablePath = TablePath.of("default", tableName);

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
                        .primaryKey(PrimaryKey.of("pk", List.of("id")))
                        .build();

        return CatalogTable.of(
                TableIdentifier.of("catalog", tablePath),
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
