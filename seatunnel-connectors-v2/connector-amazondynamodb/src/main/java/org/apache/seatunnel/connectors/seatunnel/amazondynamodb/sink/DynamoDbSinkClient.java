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

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.exception.AmazonDynamoDBConnectorException;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.serialize.DefaultSeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.serialize.SeaTunnelRowDeserializer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDbSinkClient {
    private final AmazonDynamoDBSourceOptions amazondynamodbSourceOptions;
    private volatile boolean initialize;
    private volatile Exception flushException;
    private DynamoDbClient dynamoDbClient;
    private final List<WriteRequest> batchList;
    protected SeaTunnelRowDeserializer seaTunnelRowDeserializer;

    public DynamoDbSinkClient(
            AmazonDynamoDBSourceOptions amazondynamodbSourceOptions, SeaTunnelRowType typeInfo) {
        this.amazondynamodbSourceOptions = amazondynamodbSourceOptions;
        this.batchList = new ArrayList<>();
        this.seaTunnelRowDeserializer = new DefaultSeaTunnelRowDeserializer(typeInfo);
    }

    private void tryInit() {
        if (initialize) {
            return;
        }
        dynamoDbClient =
                DynamoDbClient.builder()
                        .endpointOverride(URI.create(amazondynamodbSourceOptions.getUrl()))
                        // The region is meaningless for local DynamoDb but required for client
                        // builder validation
                        .region(Region.of(amazondynamodbSourceOptions.getRegion()))
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create(
                                                amazondynamodbSourceOptions.getAccessKeyId(),
                                                amazondynamodbSourceOptions.getSecretAccessKey())))
                        .build();
        initialize = true;
    }

    public synchronized void write(PutItemRequest putItemRequest) throws IOException {
        tryInit();
        checkFlushException();
        batchList.add(
                WriteRequest.builder()
                        .putRequest(PutRequest.builder().item(putItemRequest.item()).build())
                        .build());
        if (amazondynamodbSourceOptions.getBatchSize() > 0
                && batchList.size() >= amazondynamodbSourceOptions.getBatchSize()) {
            flush();
        }
    }

    public synchronized void close() throws IOException {
        if (dynamoDbClient != null) {
            flush();
            dynamoDbClient.close();
        }
    }

    synchronized void flush() {
        checkFlushException();
        if (batchList.isEmpty()) {
            return;
        }
        Map<String, List<WriteRequest>> requestItems = new HashMap<>(1);
        requestItems.put(amazondynamodbSourceOptions.getTable(), batchList);
        dynamoDbClient.batchWriteItem(
                BatchWriteItemRequest.builder().requestItems(requestItems).build());

        batchList.clear();
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new AmazonDynamoDBConnectorException(
                    CommonErrorCode.FLUSH_DATA_FAILED,
                    "Flush data to AmazonDynamoDB failed.",
                    flushException);
        }
    }
}
