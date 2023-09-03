package org.apache.seatunnel.connectors.seatunnel.amazonsqs.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.net.URI;

public class AmazonSqsSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    protected SqsClient sqsClient;
    protected SeaTunnelRowSerializer seaTunnelRowSerializer;

    public AmazonSqsSinkWriter(
            AmazonSqsSourceOptions amazonSqsSourceOptions, SeaTunnelRowType seaTunnelRowType) {
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
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        SendMessageRequest sendMessageRequest = seaTunnelRowSerializer.serialize(row);
        sqsClient.sendMessage(sendMessageRequest);
    }

    @Override
    public void close() throws IOException {
        sqsClient.close();
    }
}
