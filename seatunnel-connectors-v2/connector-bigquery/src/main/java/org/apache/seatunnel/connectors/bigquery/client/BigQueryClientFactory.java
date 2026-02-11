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

package org.apache.seatunnel.connectors.bigquery.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
public class BigQueryClientFactory {
    private BigQueryClientFactory() {}

    public static BigQueryWriteClient getWriteClient(ReadonlyConfig config) {
        try {
            if (config.get(BigQuerySinkOptions.EMULATOR_HOST) != null) {
                log.info(
                        "Using BigQuery Emulator at {}",
                        config.get(BigQuerySinkOptions.EMULATOR_HOST));
                String emulatorHost = config.get(BigQuerySinkOptions.EMULATOR_HOST);

                BigQueryWriteSettings settings =
                        BigQueryWriteSettings.newBuilder()
                                .setEndpoint(emulatorHost)
                                .setTransportChannelProvider(
                                        BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                                                .setChannelConfigurator(
                                                        ManagedChannelBuilder::usePlaintext)
                                                .build())
                                .setCredentialsProvider(
                                        com.google.api.gax.core.NoCredentialsProvider.create())
                                .build();

                BigQueryWriteClient bigQueryWriteClient = BigQueryWriteClient.create(settings);
                log.info("Created BigQueryWriteClient for emulator at {}", emulatorHost);

                return bigQueryWriteClient;
            }

            GoogleCredentials credentials = getCredentials(config);

            BigQueryWriteSettings settings =
                    BigQueryWriteSettings.newBuilder()
                            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                            .build();
            return BigQueryWriteClient.create(settings);
        } catch (IOException e) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.CLIENT_CREATE_FAILED,
                    "Failed to create BigQueryWriteClient",
                    e);
        }
    }

    public static BigQuery getBigQuery(ReadonlyConfig config) {
        String projectId = config.get(BigQuerySinkOptions.PROJECT_ID);
        if (config.get(BigQuerySinkOptions.EMULATOR_HOST) != null) {
            return BigQueryOptions.newBuilder()
                    .setHost("http://" + config.get(BigQuerySinkOptions.EMULATOR_HOST))
                    .setProjectId(projectId)
                    .setCredentials(NoCredentials.getInstance())
                    .build()
                    .getService();
        }

        GoogleCredentials credentials = getCredentials(config);

        return BigQueryOptions.newBuilder()
                .setProjectId(projectId)
                .setCredentials(credentials)
                .build()
                .getService();
    }

    public static GoogleCredentials getCredentials(ReadonlyConfig config) {
        try {
            GoogleCredentials credentials;
            if (config.get(BigQuerySinkOptions.SERVICE_ACCOUNT_KEY_PATH) != null) {
                try (InputStream is =
                        Files.newInputStream(
                                Paths.get(
                                        config.get(
                                                BigQuerySinkOptions.SERVICE_ACCOUNT_KEY_PATH)))) {
                    credentials = ServiceAccountCredentials.fromStream(is);
                }
            } else if (config.get(BigQuerySinkOptions.SERVICE_ACCOUNT_KEY_JSON) != null) {
                credentials =
                        ServiceAccountCredentials.fromStream(
                                new ByteArrayInputStream(
                                        config.get(BigQuerySinkOptions.SERVICE_ACCOUNT_KEY_JSON)
                                                .getBytes()));
            } else {
                credentials = GoogleCredentials.getApplicationDefault();
            }
            return credentials;
        } catch (IOException e) {
            throw new BigQueryConnectorException(BigQueryConnectorErrorCode.BAD_CREDENTIALS, e);
        }
    }
}
