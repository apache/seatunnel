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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazondynamodbSourceOptions;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
public class AmazondynamodbSourceReader implements SourceReader<SeaTunnelRow, AmazondynamodbSourceSplit> {

    protected DynamoDbClient dynamoDbClient;
    protected SourceReader.Context context;
    protected AmazondynamodbSourceOptions amazondynamodbSourceOptions;
    protected Deque<AmazondynamodbSourceSplit> splits = new LinkedList<>();
    protected boolean noMoreSplit;
    protected SeaTunnelRowType typeInfo;

    public AmazondynamodbSourceReader(SourceReader.Context context,
                                      AmazondynamodbSourceOptions amazondynamodbSourceOptions,
                                      SeaTunnelRowType typeInfo) {
        this.context = context;
        this.amazondynamodbSourceOptions = amazondynamodbSourceOptions;
        this.typeInfo = typeInfo;
    }

    @Override
    public void open() throws Exception {
        dynamoDbClient = DynamoDbClient.builder()
            .endpointOverride(URI.create(amazondynamodbSourceOptions.getUrl()))
            // The region is meaningless for local DynamoDb but required for client builder validation
            .region(Region.of(amazondynamodbSourceOptions.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(amazondynamodbSourceOptions.getAccessKeyId(), amazondynamodbSourceOptions.getSecretAccessKey())))
            .build();
    }

    @Override
    public void close() throws IOException {
        dynamoDbClient.close();
    }

    @Override
    @SuppressWarnings("magicnumber")
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            AmazondynamodbSourceSplit split = splits.poll();
            if (null != split) {
                QueryResponse query = dynamoDbClient.query(QueryRequest.builder().build());
                if (query.hasItems()) {
                    query.items().forEach(item -> {
                        output.collect(converterToRow(item, typeInfo));
                    });
                }
            } else if (noMoreSplit) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded amazondynamodb source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(1000L);
            }
        }
    }

    @Override
    public List<AmazondynamodbSourceSplit> snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void addSplits(List<AmazondynamodbSourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

    private SeaTunnelRow converterToRow(Map<String, AttributeValue> item, SeaTunnelRowType typeInfo) {
        List<Object> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = typeInfo.getFieldTypes();
        String[] fieldNames = typeInfo.getFieldNames();
        for (int i = 1; i <= seaTunnelDataTypes.length; i++) {
            Object seatunnelField;
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[i - 1];
            AttributeValue attributeValue = item.get(fieldNames[i]);
            if (attributeValue.nul()) {
                seatunnelField = null;
            } else if (BasicType.BOOLEAN_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = attributeValue.bool();
            } else if (BasicType.BYTE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = attributeValue.s().getBytes(StandardCharsets.UTF_8);
            } else {
                throw new IllegalStateException("Unexpected value: " + seaTunnelDataType);
            }

            fields.add(seatunnelField);
        }

        return new SeaTunnelRow(fields.toArray());
    }
}
