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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.sink;

import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBConfig;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DynamoDbSinkClient {
    private final AmazonDynamoDBConfig amazondynamodbConfig;
    private volatile boolean initialize;
    private DynamoDbClient dynamoDbClient;
    private final Map<String, List<WriteRequest>> batchListByTable;
    private final Object lock = new Object();

    public DynamoDbSinkClient(
            AmazonDynamoDBConfig amazondynamodbConfig, DynamoDbClient dynamoDbClient) {
        this.amazondynamodbConfig = amazondynamodbConfig;
        this.dynamoDbClient = dynamoDbClient;
        this.batchListByTable = new HashMap<>();
        this.initialize = true;
    }

    public DynamoDbSinkClient(AmazonDynamoDBConfig amazondynamodbConfig) {
        this.amazondynamodbConfig = amazondynamodbConfig;
        this.batchListByTable = new HashMap<>();
    }

    private void tryInit() {
        if (initialize) {
            return;
        }
        dynamoDbClient =
                DynamoDbClient.builder()
                        .endpointOverride(URI.create(amazondynamodbConfig.getUrl()))
                        // The region is meaningless for local DynamoDb but required for client
                        // builder validation
                        .region(Region.of(amazondynamodbConfig.getRegion()))
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create(
                                                amazondynamodbConfig.getAccessKeyId(),
                                                amazondynamodbConfig.getSecretAccessKey())))
                        .build();
        initialize = true;
    }

    public void write(PutItemRequest putItemRequest, String tableName) {
        List<WriteRequest> toFlush = null;

        synchronized (lock) {
            tryInit();

            batchListByTable.computeIfAbsent(tableName, k -> new ArrayList<>());
            batchListByTable
                    .get(tableName)
                    .add(
                            WriteRequest.builder()
                                    .putRequest(
                                            PutRequest.builder()
                                                    .item(putItemRequest.item())
                                                    .build())
                                    .build());

            if (amazondynamodbConfig.getBatchSize() > 0
                    && batchListByTable.get(tableName).size()
                            >= amazondynamodbConfig.getBatchSize()) {
                // Copy batch and remove from map inside lock (fast)
                toFlush = new ArrayList<>(batchListByTable.get(tableName));
                batchListByTable.remove(tableName);
            }
        }

        // Execute network I/O outside lock (other threads can continue)
        if (toFlush != null) {
            flushTable(tableName, toFlush);
        }
    }

    public void close() {
        flush();
        synchronized (lock) {
            if (dynamoDbClient != null) {
                dynamoDbClient.close();
            }
        }
    }

    void flush() {
        Map<String, List<WriteRequest>> batchToFlush = new HashMap<>();

        synchronized (lock) {
            if (dynamoDbClient == null || batchListByTable.isEmpty()) {
                return;
            }
            batchToFlush.putAll(batchListByTable);
            batchListByTable.clear();
        }

        for (Map.Entry<String, List<WriteRequest>> entry : batchToFlush.entrySet()) {
            flushTable(entry.getKey(), entry.getValue());
        }
    }

    private void flushTable(String tableName, List<WriteRequest> requests) {
        if (!requests.isEmpty()) {
            flushWithRetry(tableName, requests);
        }
    }

    private void flushWithRetry(String tableName, List<WriteRequest> requests) {
        List<WriteRequest> pendingRequests = new ArrayList<>(requests);

        int maxRetries = amazondynamodbConfig.getMaxRetries();
        long baseDelayMs = amazondynamodbConfig.getRetryBaseDelayMs();
        long maxDelayMs = amazondynamodbConfig.getRetryMaxDelayMs();

        int retryCount = 0;

        while (!pendingRequests.isEmpty() && retryCount <= maxRetries) {
            Map<String, List<WriteRequest>> requestItems = new HashMap<>(1);
            requestItems.put(tableName, pendingRequests);

            BatchWriteItemResponse response =
                    dynamoDbClient.batchWriteItem(
                            BatchWriteItemRequest.builder().requestItems(requestItems).build());

            Map<String, List<WriteRequest>> unprocessedKeys = response.unprocessedItems();
            pendingRequests = unprocessedKeys.getOrDefault(tableName, new ArrayList<>());

            if (!pendingRequests.isEmpty()) {
                retryCount++;

                long delay = Math.min(baseDelayMs * (1L << retryCount), maxDelayMs);

                long jitter = (long) (delay * Math.random() * 0.5);
                delay += jitter;

                log.warn(
                        "Retrying batch write to table '{}': attempt {}/{}, "
                                + "{} unprocessed items remaining, retrying in {} ms",
                        tableName,
                        retryCount,
                        maxRetries,
                        pendingRequests.size(),
                        delay);

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry", e);
                }
            }
        }

        if (!pendingRequests.isEmpty()) {
            log.error(
                    "Failed to write {} items to table '{}' after {} retries",
                    pendingRequests.size(),
                    tableName,
                    maxRetries);

            throw new RuntimeException(
                    String.format(
                            "Failed to write %d items to table %s after %d retries",
                            pendingRequests.size(), tableName, maxRetries));
        }
    }
}
