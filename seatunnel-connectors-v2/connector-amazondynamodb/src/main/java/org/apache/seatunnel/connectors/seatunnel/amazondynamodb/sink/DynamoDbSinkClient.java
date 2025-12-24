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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDbSinkClient {
    private final AmazonDynamoDBConfig amazondynamodbConfig;
    private volatile boolean initialize;
    private DynamoDbClient dynamoDbClient;
    private final List<WriteRequest> batchList;

    public DynamoDbSinkClient(AmazonDynamoDBConfig amazondynamodbConfig) {
        this.amazondynamodbConfig = amazondynamodbConfig;
        this.batchList = new ArrayList<>();
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

    public synchronized void write(PutItemRequest putItemRequest) {
        tryInit();
        batchList.add(
                WriteRequest.builder()
                        .putRequest(PutRequest.builder().item(putItemRequest.item()).build())
                        .build());
        if (amazondynamodbConfig.getBatchSize() > 0
                && batchList.size() >= amazondynamodbConfig.getBatchSize()) {
            flush();
        }
    }

    public synchronized void close() {
        if (dynamoDbClient != null) {
            flush();
            dynamoDbClient.close();
        }
    }

    synchronized void flush() {
        if (batchList.isEmpty()) {
            return;
        }
        Map<String, List<WriteRequest>> requestItems = new HashMap<>(1);
        requestItems.put(amazondynamodbConfig.getTable(), batchList);
        dynamoDbClient.batchWriteItem(
                BatchWriteItemRequest.builder().requestItems(requestItems).build());

        batchList.clear();
    }
}
