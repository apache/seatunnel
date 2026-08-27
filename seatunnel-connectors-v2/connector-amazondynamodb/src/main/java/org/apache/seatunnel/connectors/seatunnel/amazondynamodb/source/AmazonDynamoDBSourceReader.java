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
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBConfig;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.serialize.DefaultSeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.serialize.SeaTunnelRowDeserializer;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class AmazonDynamoDBSourceReader
        implements SourceReader<SeaTunnelRow, AmazonDynamoDBSourceSplit> {

    protected DynamoDbClient dynamoDbClient;
    protected SourceReader.Context context;
    protected AmazonDynamoDBConfig amazondynamodbConfig;
    protected SeaTunnelRowDeserializer seaTunnelRowDeserializer;
    Queue<AmazonDynamoDBSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();

    private volatile boolean noMoreSplit;

    public AmazonDynamoDBSourceReader(
            SourceReader.Context context,
            AmazonDynamoDBConfig amazondynamodbConfig,
            SeaTunnelRowType typeInfo) {
        this.context = context;
        this.amazondynamodbConfig = amazondynamodbConfig;
        this.seaTunnelRowDeserializer = new DefaultSeaTunnelRowDeserializer(typeInfo);
    }

    @Override
    public void open() {
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
    }

    @Override
    public void close() {
        dynamoDbClient.close();
    }

    @Override
    @SuppressWarnings("magicnumber")
    public void pollNext(Collector<SeaTunnelRow> output) throws InterruptedException {
        synchronized (output.getCheckpointLock()) {
            AmazonDynamoDBSourceSplit split = pendingSplits.poll();
            if (split == null) {
                log.info(
                        "AmazonDynamoDB Source Reader [{}] waiting for splits",
                        context.getIndexOfSubtask());
                if (noMoreSplit) {
                    // signal to the source that we have reached the end of the data.
                    log.info("Closed the bounded amazonDynamodb source");
                    context.signalNoMoreElement();
                    Thread.sleep(2000L);
                }
            }
            if (Objects.nonNull(split)) {
                read(split, output);
            }
        }
    }

    @Override
    public List<AmazonDynamoDBSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<AmazonDynamoDBSourceSplit> splits) {
        this.pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader [{}] received noMoreSplit event.", context.getIndexOfSubtask());
        noMoreSplit = true;
    }

    private void read(AmazonDynamoDBSourceSplit split, Collector<SeaTunnelRow> output) {
        ScanIterable scan;
        ScanRequest scanRequest =
                ScanRequest.builder()
                        .tableName(amazondynamodbConfig.getTable())
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
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}
}
