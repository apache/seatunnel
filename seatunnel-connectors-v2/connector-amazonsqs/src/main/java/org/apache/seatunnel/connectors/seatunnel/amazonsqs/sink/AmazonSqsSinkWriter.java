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

package org.apache.seatunnel.connectors.seatunnel.amazonsqs.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.json.canal.CanalJsonSerializationSchema;
import org.apache.seatunnel.format.json.debezium.DebeziumJsonSerializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsConfig.DEFAULT_FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsConfig.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsConfig.FORMAT;

public class AmazonSqsSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    protected SqsClient sqsClient;

    private final AmazonSqsSourceOptions amazonSqsSourceOptions;

    private final SerializationSchema serializationSchema;

    public AmazonSqsSinkWriter(
            AmazonSqsSourceOptions amazonSqsSourceOptions,
            SeaTunnelRowType seaTunnelRowType,
            ReadonlyConfig pluginConfig) {
        if (amazonSqsSourceOptions.getAccessKeyId() != null
                & amazonSqsSourceOptions.getSecretAccessKey() != null) {
            sqsClient =
                    SqsClient.builder()
                            .endpointOverride(URI.create(amazonSqsSourceOptions.getUrl()))
                            // The region is meaningless for local Sqs but required for client
                            // builder validation
                            .region(Region.of(amazonSqsSourceOptions.getRegion()))
                            .credentialsProvider(
                                    StaticCredentialsProvider.create(
                                            AwsBasicCredentials.create(
                                                    amazonSqsSourceOptions.getAccessKeyId(),
                                                    amazonSqsSourceOptions.getSecretAccessKey())))
                            .build();
        } else {
            sqsClient =
                    SqsClient.builder()
                            .endpointOverride(URI.create(amazonSqsSourceOptions.getUrl()))
                            .region(Region.of(amazonSqsSourceOptions.getRegion()))
                            .credentialsProvider(DefaultCredentialsProvider.create())
                            .build();
        }
        this.amazonSqsSourceOptions = amazonSqsSourceOptions;
        this.serializationSchema = createSerializationSchema(seaTunnelRowType, pluginConfig);
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        byte[] serializedBody = serializationSchema.serialize(row);

        SendMessageRequest sendMessageRequest =
                SendMessageRequest.builder()
                        .queueUrl(amazonSqsSourceOptions.getUrl())
                        .messageBody(Arrays.toString(serializedBody))
                        .build();

        sqsClient.sendMessage(sendMessageRequest);
    }

    @Override
    public void close() throws IOException {
        sqsClient.close();
    }

    private static SerializationSchema createSerializationSchema(
            SeaTunnelRowType rowType, ReadonlyConfig config) {
        MessageFormat format = config.get(FORMAT);
        switch (format) {
            case JSON:
                return new JsonSerializationSchema(rowType);
            case TEXT:
                String delimiter = DEFAULT_FIELD_DELIMITER;
                if (config.get(FIELD_DELIMITER) != null) {
                    delimiter = config.get(FIELD_DELIMITER);
                }
                return TextSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(delimiter)
                        .build();
            case CANAL_JSON:
                return new CanalJsonSerializationSchema(rowType);
            case DEBEZIUM_JSON:
                return new DebeziumJsonSerializationSchema(rowType);
            default:
                throw new SeaTunnelJsonFormatException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported format: " + format);
        }
    }
}
