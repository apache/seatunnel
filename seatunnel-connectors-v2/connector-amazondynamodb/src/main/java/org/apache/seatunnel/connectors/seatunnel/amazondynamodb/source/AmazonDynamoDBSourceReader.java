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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.serialize.DefaultSeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.serialize.SeaTunnelRowDeserializer;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class AmazonDynamoDBSourceReader
        implements SourceReader<SeaTunnelRow, AmazonDynamoDBSourceSplit> {

    protected DynamoDbClient dynamoDbClient;
    protected SourceReader.Context context;
    protected AmazonDynamoDBSourceOptions amazondynamodbSourceOptions;
    protected SeaTunnelRowDeserializer seaTunnelRowDeserializer;
    Queue<AmazonDynamoDBSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();

    private volatile boolean noMoreSplit;

    public AmazonDynamoDBSourceReader(
            SourceReader.Context context,
            AmazonDynamoDBSourceOptions amazondynamodbSourceOptions,
            SeaTunnelRowType typeInfo) {
        this.context = context;
        this.amazondynamodbSourceOptions = amazondynamodbSourceOptions;
        this.seaTunnelRowDeserializer = new DefaultSeaTunnelRowDeserializer(typeInfo);
    }

    @Override
    public void open() throws Exception {
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
    }

    @Override
    public void close() throws IOException {
        dynamoDbClient.close();
    }

    @Override
    @SuppressWarnings("magicnumber")
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        while (!pendingSplits.isEmpty()) {
            synchronized (output.getCheckpointLock()) {
                AmazonDynamoDBSourceSplit split = pendingSplits.poll();

                read(split, output);
            }
        }
        if (pendingSplits.isEmpty() && noMoreSplit) {
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<AmazonDynamoDBSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<AmazonDynamoDBSourceSplit> splits) {
        this.pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader received noMoreSplit event.");
        noMoreSplit = true;
    }

    private void read(AmazonDynamoDBSourceSplit split, Collector<SeaTunnelRow> output)
            throws Exception {
        Map<String, AttributeValue> lastKeyEvaluated = null;
        ScanIterable scan;
        ScanRequest scanRequest =
                ScanRequest.builder()
                        .tableName(amazondynamodbSourceOptions.getTable())
                        .limit(split.getItemCount())
                        .segment(split.getSplitId())
                        .totalSegments(split.getTotalSegments())
                        .build();
        scan = dynamoDbClient.scanPaginator(scanRequest);
        do {

            scan.items()
                    .forEach(
                            item -> {
                                output.collect(seaTunnelRowDeserializer.deserialize(item));
                            });

        } while (scan.iterator().hasNext() && !noMoreSplit);

        if (noMoreSplit && pendingSplits.isEmpty()) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded amazonDynamodb source");
            context.signalNoMoreElement();
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
