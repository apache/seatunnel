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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ElasticsearchClusterInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Owns Elasticsearch REST clients shared by table writers with identical connection settings in one
 * multi-table sink subtask.
 *
 * <p>Table placeholders may produce different hosts, credentials, or TLS settings. Such writers
 * must use separate clients, while table-only settings such as index names do not prevent sharing.
 */
class ElasticsearchMultiTableResourceManager implements MultiTableResourceManager<EsRestClient> {

    // Clients grouped by the complete set of options consumed during client construction.
    private final Map<List<Object>, ClientResource> clientResources = new HashMap<>();

    /**
     * Creates an empty manager; clients are initialized lazily from each table writer's config.
     *
     * <p>Lazy initialization prevents transient creation of one REST client per table.
     */
    ElasticsearchMultiTableResourceManager() {}

    /**
     * Returns the client and cluster metadata for one table's connection settings.
     *
     * <p>Initialization is synchronized so writers assigned to different queues cannot create
     * duplicate clients for the same connection.
     *
     * @param config table-specific Elasticsearch configuration
     * @return client resource shared by writers with identical connection settings
     */
    synchronized ClientResource getOrCreateClientResource(ReadonlyConfig config) {
        List<Object> connectionKey = connectionKey(config);
        ClientResource existingResource = clientResources.get(connectionKey);
        if (existingResource != null) {
            existingResource.retain();
            return existingResource;
        }
        EsRestClient esRestClient = null;
        try {
            esRestClient = EsRestClient.createInstance(config);
            ClientResource newResource =
                    new ClientResource(connectionKey, esRestClient, esRestClient.getClusterInfo());
            newResource.retain();
            clientResources.put(connectionKey, newResource);
            return newResource;
        } catch (RuntimeException | Error e) {
            if (esRestClient != null) {
                esRestClient.close();
            }
            // MultiTableSinkWriter does not close its manager when construction fails. Release all
            // connection groups here so task startup retries cannot accumulate client threads.
            close();
            throw e;
        }
    }

    /**
     * Releases one writer's reference to a connection group and closes the client at the last user.
     *
     * <p>Multi-table writers can be removed independently when a table fails with
     * CONTINUE_OTHER_TABLES. Releasing per writer prevents unique connection groups from living
     * until the whole sink stops.
     *
     * @param clientResource resource previously retained by {@link #getOrCreateClientResource}
     */
    synchronized void releaseClientResource(ClientResource clientResource) {
        if (!clientResources.containsKey(clientResource.getConnectionKey())) {
            return;
        }
        if (clientResource.release()) {
            clientResources.remove(clientResource.getConnectionKey());
            clientResource.getEsRestClient().close();
        }
    }

    /**
     * Returns the single shared client when all initialized writers use one connection group.
     *
     * <p>The multi-table writer uses {@link #getOrCreateClientResource(ReadonlyConfig)} because a
     * task may legitimately contain multiple connection groups.
     *
     * @return the only initialized client, or empty when zero or multiple groups exist
     */
    @Override
    public synchronized Optional<EsRestClient> getSharedResource() {
        if (clientResources.size() != 1) {
            return Optional.empty();
        }
        return Optional.of(clientResources.values().iterator().next().getEsRestClient());
    }

    /**
     * Closes every connection group exactly once and makes repeated close calls harmless.
     *
     * <p>The map is cleared only after every owned client has been closed.
     */
    @Override
    public synchronized void close() {
        clientResources.values().forEach(resource -> resource.getEsRestClient().close());
        clientResources.clear();
    }

    /**
     * Builds a stable key from every option consumed by {@link EsRestClient#createInstance}.
     *
     * <p>Do not add table-specific sink options here: doing so would recreate one client per table.
     *
     * @param config table-specific Elasticsearch configuration
     * @return immutable-by-construction connection key
     */
    private List<Object> connectionKey(ReadonlyConfig config) {
        return Arrays.asList(
                new ArrayList<>(config.get(ElasticsearchBaseOptions.HOSTS)),
                config.getOptional(ElasticsearchBaseOptions.USERNAME).orElse(null),
                config.getOptional(ElasticsearchBaseOptions.PASSWORD).orElse(null),
                config.get(ElasticsearchBaseOptions.TLS_VERIFY_CERTIFICATE),
                config.get(ElasticsearchBaseOptions.TLS_VERIFY_HOSTNAME),
                config.getOptional(ElasticsearchBaseOptions.TLS_KEY_STORE_PATH).orElse(null),
                config.getOptional(ElasticsearchBaseOptions.TLS_KEY_STORE_PASSWORD).orElse(null),
                config.getOptional(ElasticsearchBaseOptions.TLS_TRUST_STORE_PATH).orElse(null),
                config.getOptional(ElasticsearchBaseOptions.TLS_TRUST_STORE_PASSWORD).orElse(null),
                config.get(ElasticsearchBaseOptions.AUTH_TYPE),
                config.getOptional(ElasticsearchBaseOptions.API_KEY_ID).orElse(null),
                config.getOptional(ElasticsearchBaseOptions.API_KEY).orElse(null),
                config.getOptional(ElasticsearchBaseOptions.API_KEY_ENCODED).orElse(null));
    }

    /**
     * Holds one connection group's client and immutable cluster metadata.
     *
     * <p>Both values have the same lifecycle and are reused by every writer in the group.
     */
    static final class ClientResource {

        // Connection key used to remove this resource when the last writer releases it.
        private final List<Object> connectionKey;

        // REST client shared by all writers in this connection group.
        private final EsRestClient esRestClient;

        // Cluster metadata cached once for all serializers in this connection group.
        private final ElasticsearchClusterInfo clusterInfo;

        // Number of active table writers currently using this connection group.
        private int referenceCount;

        /**
         * Creates one connection-group resource.
         *
         * @param connectionKey stable connection-group key
         * @param esRestClient shared REST client
         * @param clusterInfo cluster metadata loaded through the client
         */
        private ClientResource(
                List<Object> connectionKey,
                EsRestClient esRestClient,
                ElasticsearchClusterInfo clusterInfo) {
            this.connectionKey = connectionKey;
            this.esRestClient = esRestClient;
            this.clusterInfo = clusterInfo;
        }

        /**
         * Returns the stable connection-group key.
         *
         * @return connection key
         */
        List<Object> getConnectionKey() {
            return connectionKey;
        }

        /**
         * Returns the shared REST client.
         *
         * @return connection-group client
         */
        EsRestClient getEsRestClient() {
            return esRestClient;
        }

        /**
         * Returns cached cluster metadata.
         *
         * @return connection-group cluster metadata
         */
        ElasticsearchClusterInfo getClusterInfo() {
            return clusterInfo;
        }

        /** Records one active table writer using this connection group. */
        void retain() {
            referenceCount++;
        }

        /**
         * Releases one active table writer.
         *
         * @return true when the connection group has no remaining users
         */
        boolean release() {
            referenceCount--;
            return referenceCount == 0;
        }
    }
}
