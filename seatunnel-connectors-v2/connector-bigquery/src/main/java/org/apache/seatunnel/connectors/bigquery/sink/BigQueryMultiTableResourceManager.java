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

package org.apache.seatunnel.connectors.bigquery.sink;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;

import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * Resource manager for coordinating and sharing a single {@link BigQueryWriteClient} across
 * multiple sink writers to optimize connection usage and simplify resource cleanup.
 */
@Slf4j
public class BigQueryMultiTableResourceManager
        implements MultiTableResourceManager<BigQueryWriteClient> {

    private final BigQueryWriteClient client;

    /**
     * Constructs a BigQueryMultiTableResourceManager with the specified shared client.
     *
     * @param client the shared {@link BigQueryWriteClient} instance
     */
    public BigQueryMultiTableResourceManager(BigQueryWriteClient client) {
        this.client = client;
    }

    /**
     * Retrieves the shared {@link BigQueryWriteClient} resource.
     *
     * @return an {@link Optional} containing the shared client if present
     */
    @Override
    public Optional<BigQueryWriteClient> getSharedResource() {
        return Optional.ofNullable(client);
    }

    @Override
    public void close() {
        if (client != null) {
            log.info("Closing shared worker BigQueryWriteClient connection...");
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Failed to close shared BigQueryWriteClient", e);
            }
        }
    }
}
