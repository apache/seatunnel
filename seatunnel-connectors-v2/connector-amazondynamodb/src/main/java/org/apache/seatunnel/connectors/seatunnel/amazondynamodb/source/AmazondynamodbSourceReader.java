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
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazondynamodbSourceOptions;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementRequest;

import java.io.IOException;
import java.util.List;

public class AmazondynamodbSourceReader implements SourceReader<SeaTunnelRow, AmazondynamodbSourceSplit> {

    DynamoDbClient dynamoDbClient;
    SourceReader.Context context;
    ExecuteStatementRequest executeStatement;
    AmazondynamodbSourceOptions amazondynamodbSourceOptions;

    public AmazondynamodbSourceReader(DynamoDbClient dynamoDbClient, SourceReader.Context context, AmazondynamodbSourceOptions amazondynamodbSourceOptions) {
        this.dynamoDbClient = dynamoDbClient;
        this.context = context;
        this.amazondynamodbSourceOptions = amazondynamodbSourceOptions;
    }

    @Override
    public void open() throws Exception {
        executeStatement = ExecuteStatementRequest.builder().statement(amazondynamodbSourceOptions.getQuery()).build();
    }

    @Override
    public void close() throws IOException {
        dynamoDbClient.close();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
    }

    @Override
    public List<AmazondynamodbSourceSplit> snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void addSplits(List<AmazondynamodbSourceSplit> splits) {

    }

    @Override
    public void handleNoMoreSplits() {

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
