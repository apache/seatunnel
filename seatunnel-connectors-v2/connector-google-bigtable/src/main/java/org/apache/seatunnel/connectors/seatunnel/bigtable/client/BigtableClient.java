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

package org.apache.seatunnel.connectors.seatunnel.bigtable.client;

import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorException;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Wrapper around the native Google Cloud Bigtable Java client.
 *
 * <p>Provides basic CRUD operations and batched mutation support used by both the Sink and Source
 * connectors.
 */
@Slf4j
public class BigtableClient implements Serializable, AutoCloseable {

    private transient BigtableDataClient dataClient;
    private final BigtableParameters parameters;

    private BigtableClient(BigtableDataClient dataClient, BigtableParameters parameters) {
        this.dataClient = dataClient;
        this.parameters = parameters;
    }

    /**
     * Creates a new BigtableClient instance using the provided parameters.
     *
     * @param parameters Bigtable connection parameters
     * @return a connected BigtableClient
     */
    public static BigtableClient createInstance(BigtableParameters parameters) {
        try {
            BigtableDataSettings.Builder settingsBuilder =
                    BigtableDataSettings.newBuilder()
                            .setProjectId(parameters.getProjectId())
                            .setInstanceId(parameters.getInstanceId());

            if (parameters.getCredentialsPath() != null
                    && !parameters.getCredentialsPath().isEmpty()) {
                try (FileInputStream credStream =
                        new FileInputStream(parameters.getCredentialsPath())) {
                    GoogleCredentials credentials =
                            ServiceAccountCredentials.fromStream(credStream);
                    settingsBuilder.stubSettings().setCredentialsProvider(() -> credentials);
                }
            }

            BigtableDataClient client = BigtableDataClient.create(settingsBuilder.build());
            return new BigtableClient(client, parameters);
        } catch (IOException e) {
            throw new BigtableConnectorException(
                    BigtableConnectorErrorCode.CONNECTION_FAILED,
                    "Failed to create Bigtable client for project="
                            + parameters.getProjectId()
                            + ", instance="
                            + parameters.getInstanceId(),
                    e);
        }
    }

    /**
     * Applies a single row mutation to Bigtable.
     *
     * @param rowMutation the row mutation to apply
     */
    public void mutateRow(RowMutation rowMutation) {
        try {
            dataClient.mutateRow(rowMutation);
        } catch (Exception e) {
            throw new BigtableConnectorException(
                    BigtableConnectorErrorCode.WRITE_FAILED,
                    "Failed to mutate row in table " + parameters.getTable(),
                    e);
        }
    }

    /**
     * Applies a batch of row mutations to Bigtable using BulkMutation for efficiency.
     *
     * @param mutations list of (rowKey, Mutation) pairs to apply
     */
    public void bulkMutate(List<RowKeyMutation> mutations) {
        if (mutations.isEmpty()) {
            return;
        }
        try {
            BulkMutation bulk = BulkMutation.create(parameters.getTable());
            for (RowKeyMutation entry : mutations) {
                bulk.add(entry.getRowKey(), entry.getMutation());
            }
            dataClient.bulkMutateRows(bulk);
        } catch (Exception e) {
            throw new BigtableConnectorException(
                    BigtableConnectorErrorCode.WRITE_FAILED,
                    "Failed to bulk mutate "
                            + mutations.size()
                            + " rows in table "
                            + parameters.getTable(),
                    e);
        }
    }

    /**
     * Returns the underlying data client (for use in Source reader streaming).
     *
     * @return BigtableDataClient
     */
    public BigtableDataClient getDataClient() {
        return dataClient;
    }

    @Override
    public void close() {
        if (dataClient != null) {
            try {
                dataClient.close();
                dataClient = null;
            } catch (Exception e) {
                log.error("Failed to close Bigtable data client", e);
            }
        }
    }

    /** Holds a row key and its associated Mutation for bulk operations. */
    public static class RowKeyMutation implements Serializable {
        private final ByteString rowKey;
        private final Mutation mutation;

        public RowKeyMutation(ByteString rowKey, Mutation mutation) {
            this.rowKey = rowKey;
            this.mutation = mutation;
        }

        public ByteString getRowKey() {
            return rowKey;
        }

        public Mutation getMutation() {
            return mutation;
        }
    }
}
