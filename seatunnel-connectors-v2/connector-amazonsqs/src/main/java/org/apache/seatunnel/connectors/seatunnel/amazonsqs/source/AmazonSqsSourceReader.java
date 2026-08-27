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

package org.apache.seatunnel.connectors.seatunnel.amazonsqs.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.deserialize.AmazonSqsDeserializer;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.deserialize.SeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.io.IOException;
import java.net.URI;
import java.util.List;

@Slf4j
public class AmazonSqsSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    protected SqsClient sqsClient;
    protected SingleSplitReaderContext context;
    protected AmazonSqsSourceConfig amazonSqsSourceConfig;
    private final SeaTunnelRowDeserializer seaTunnelRowDeserializer;

    public AmazonSqsSourceReader(
            SingleSplitReaderContext context,
            AmazonSqsSourceConfig amazonSqsSourceConfig,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            SeaTunnelRowType seaTunnelRowType) {
        this.context = context;
        this.amazonSqsSourceConfig = amazonSqsSourceConfig;
        this.seaTunnelRowDeserializer = new AmazonSqsDeserializer(deserializationSchema);
    }

    @Override
    public void open() throws Exception {
        if (amazonSqsSourceConfig.getAccessKeyId() != null
                & amazonSqsSourceConfig.getSecretAccessKey() != null) {
            sqsClient =
                    SqsClient.builder()
                            .endpointOverride(URI.create(amazonSqsSourceConfig.getUrl()))
                            // The region is meaningless for local Sqs but required for client
                            // builder validation
                            .region(Region.of(amazonSqsSourceConfig.getRegion()))
                            .credentialsProvider(
                                    StaticCredentialsProvider.create(
                                            AwsBasicCredentials.create(
                                                    amazonSqsSourceConfig.getAccessKeyId(),
                                                    amazonSqsSourceConfig.getSecretAccessKey())))
                            .build();
        } else {
            sqsClient =
                    SqsClient.builder()
                            .endpointOverride(URI.create(amazonSqsSourceConfig.getUrl()))
                            .region(Region.of(amazonSqsSourceConfig.getRegion()))
                            .credentialsProvider(DefaultCredentialsProvider.create())
                            .build();
        }
    }

    @Override
    public void close() throws IOException {
        sqsClient.close();
    }

    @Override
    @SuppressWarnings("magicnumber")
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        ReceiveMessageRequest receiveMessageRequest =
                ReceiveMessageRequest.builder()
                        .queueUrl(amazonSqsSourceConfig.getUrl())
                        .maxNumberOfMessages(10) // Adjust the batch size as needed
                        .waitTimeSeconds(10) // Adjust the wait time as needed
                        .build();

        ReceiveMessageResponse response = sqsClient.receiveMessage(receiveMessageRequest);
        List<Message> messages = response.messages();

        for (Message message : messages) {
            String messageBody = message.body();
            SeaTunnelRow seaTunnelRow = this.seaTunnelRowDeserializer.deserializeRow(messageBody);
            output.collect(seaTunnelRow);

            // Delete the processed message
            if (amazonSqsSourceConfig.isDeleteMessage()) {
                DeleteMessageRequest deleteMessageRequest =
                        DeleteMessageRequest.builder()
                                .queueUrl(amazonSqsSourceConfig.getUrl())
                                .receiptHandle(message.receiptHandle())
                                .build();
                sqsClient.deleteMessage(deleteMessageRequest);
            }
        }
        this.context.signalNoMoreElement();
    }
}
