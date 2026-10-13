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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class AmazonDynamoDBSourceReaderTest {

    @Test
    @Timeout(30)
    void readsSegmentOnceBeforeNoMoreSplitsArrives() throws Exception {
        AmazonDynamoDBSourceReader reader = reader();
        AtomicInteger scans = new AtomicInteger();
        DynamoDbClient client = Mockito.mock(DynamoDbClient.class);
        Mockito.when(client.scan(Mockito.any(ScanRequest.class)))
                .thenAnswer(
                        invocation -> {
                            // The enumerator's no-more-splits signal arrives late.
                            if (scans.incrementAndGet() >= 3) {
                                reader.handleNoMoreSplits();
                            }
                            return ScanResponse.builder()
                                    .items(Arrays.asList(item("o1"), item("o2")))
                                    .build();
                        });

        List<SeaTunnelRow> rows = read(reader, client);

        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(1, scans.get());
    }

    @Test
    @Timeout(30)
    void readsEveryPageOfSegment() throws Exception {
        AmazonDynamoDBSourceReader reader = reader();
        List<ScanRequest> requests = new ArrayList<>();
        DynamoDbClient client = Mockito.mock(DynamoDbClient.class);
        Mockito.when(client.scan(Mockito.any(ScanRequest.class)))
                .thenAnswer(
                        invocation -> {
                            ScanRequest request = invocation.getArgument(0, ScanRequest.class);
                            requests.add(request);
                            if (!request.hasExclusiveStartKey()) {
                                return ScanResponse.builder()
                                        .items(Arrays.asList(item("o1"), item("o2")))
                                        .lastEvaluatedKey(item("o2"))
                                        .build();
                            }
                            return ScanResponse.builder()
                                    .items(Collections.singletonList(item("o3")))
                                    .build();
                        });

        List<SeaTunnelRow> rows = read(reader, client);

        Assertions.assertEquals(
                Arrays.asList("o1", "o2", "o3"),
                rows.stream().map(row -> row.getField(0)).collect(Collectors.toList()));
        Assertions.assertEquals(2, requests.size());
        Assertions.assertEquals(item("o2"), requests.get(1).exclusiveStartKey());
    }

    private static AmazonDynamoDBSourceReader reader() {
        Map<String, Object> options = new HashMap<>();
        options.put("url", "http://localhost:8000");
        options.put("region", "us-east-1");
        options.put("access_key_id", "id");
        options.put("secret_access_key", "key");
        options.put("table", "orders");
        AmazonDynamoDBConfig config = new AmazonDynamoDBConfig(ReadonlyConfig.fromMap(options));
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        return new AmazonDynamoDBSourceReader(
                Mockito.mock(SourceReader.Context.class), config, rowType);
    }

    private static List<SeaTunnelRow> read(AmazonDynamoDBSourceReader reader, DynamoDbClient client)
            throws Exception {
        Mockito.when(client.scanPaginator(Mockito.any(ScanRequest.class)))
                .thenAnswer(
                        invocation ->
                                new ScanIterable(
                                        client, invocation.getArgument(0, ScanRequest.class)));
        reader.dynamoDbClient = client;
        reader.addSplits(Collections.singletonList(new AmazonDynamoDBSourceSplit(0, 1, 10)));
        List<SeaTunnelRow> rows = new ArrayList<>();
        reader.pollNext(
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        rows.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return this;
                    }
                });
        return rows;
    }

    private static Map<String, AttributeValue> item(String id) {
        return Collections.singletonMap("id", AttributeValue.builder().s(id).build());
    }
}
