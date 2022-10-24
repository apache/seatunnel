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

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazondynamodbSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.state.AmazonDynamodbSourceState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.protocol.MarshallingType;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbResponseMetadata;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementResponse;

import java.net.URI;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class AmazondynamodbSource implements SeaTunnelSource<SeaTunnelRow, AmazondynamodbSourceSplit, AmazonDynamodbSourceState> {

    private DynamoDbClient dynamoDbClient;

    private AmazondynamodbSourceOptions amazondynamodbSourceOptions;

    @Override
    public String getPluginName() {
        return "Amazondynamodb";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        amazondynamodbSourceOptions = new AmazondynamodbSourceOptions(pluginConfig);
        dynamoDbClient = DynamoDbClient.builder()
            .endpointOverride(URI.create(amazondynamodbSourceOptions.getUrl()))
            // The region is meaningless for local DynamoDb but required for client builder validation
            .region(Region.of(amazondynamodbSourceOptions.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(amazondynamodbSourceOptions.getAccessKeyId(), amazondynamodbSourceOptions.getSecretAccessKey())))
            .build();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        ExecuteStatementResponse executeStatementResponse = dynamoDbClient.executeStatement(ExecuteStatementRequest.builder().statement(amazondynamodbSourceOptions.getQuery()).build());

        return null;
    }

    @Override
    public SourceReader<SeaTunnelRow, AmazondynamodbSourceSplit> createReader(SourceReader.Context readerContext) {
        return new AmazondynamodbSourceReader(dynamoDbClient, readerContext, amazondynamodbSourceOptions);
    }

    @Override
    public SourceSplitEnumerator<AmazondynamodbSourceSplit, AmazonDynamodbSourceState> createEnumerator(SourceSplitEnumerator.Context<AmazondynamodbSourceSplit> enumeratorContext) throws Exception {
        return null;
    }

    @Override
    public SourceSplitEnumerator<AmazondynamodbSourceSplit, AmazonDynamodbSourceState> restoreEnumerator(SourceSplitEnumerator.Context<AmazondynamodbSourceSplit> enumeratorContext, AmazonDynamodbSourceState checkpointState) throws Exception {
        return null;
    }
}
