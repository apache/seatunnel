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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.client;

import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBConfig;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.net.URI;

public class DynamoDbClientProvider {

    public static DynamoDbClient createDynamoDBClient(AmazonDynamoDBConfig config) {
        DynamoDbClientBuilder builder = DynamoDbClient.builder();

        if (config.getUrl() != null) {
            builder.endpointOverride(URI.create(config.getUrl()));
        }

        if (config.getRegion() != null) {
            builder.region(Region.of(config.getRegion()));
        }

        if (config.getAccessKeyId() != null && config.getSecretAccessKey() != null) {
            // Set up AWS credentials with accessKeyId and secretAccessKey
            builder.credentialsProvider(
                    StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(
                                    config.getAccessKeyId(), config.getSecretAccessKey())));
        } else {
            // If accessKeyId and secretAccessKey are not provided, use container credentials.
            builder.credentialsProvider(ContainerCredentialsProvider.builder().build());
        }
        return builder.build();
    }
}
