/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.azurecosmosdb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBConfig;
import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.source.AzureCosmosDBSourceReader;
import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.source.AzureCosmosDBSourceSplit;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CosmosDBEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class AbstractAzureCosmosDBIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractAzureCosmosDBIT.class);

    protected static final String DATABASE = "seatunnel_e2e";
    protected static final String BASIC_CONTAINER = "source_basic_orders";
    protected static final String FILTER_CONTAINER = "source_filter_orders";
    protected static final String PAGINATION_CONTAINER = "source_pagination_orders";
    protected static final String TRUST_STORE_PASSWORD = "changeit";

    private static final DockerImageName COSMOS_IMAGE =
            DockerImageName.parse("mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest");

    protected CosmosDBEmulatorContainer cosmosContainer;
    protected CosmosClient client;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        cosmosContainer = new CosmosDBEmulatorContainer(COSMOS_IMAGE);
        cosmosContainer.start();
        installEmulatorTrustStore();
        client =
                new CosmosClientBuilder()
                        .endpoint(cosmosContainer.getEmulatorEndpoint())
                        .key(cosmosContainer.getEmulatorKey())
                        .endpointDiscoveryEnabled(false)
                        .gatewayMode()
                        .buildClient();
        seedContainerWhenReady(BASIC_CONTAINER, item("1", "alpha", 10), item("2", "beta", 20));
        seedContainerWhenReady(
                FILTER_CONTAINER,
                item("1", "low-score", 5),
                item("2", "high-score", 30),
                item("3", "higher-score", 40));
        seedContainerWhenReady(
                PAGINATION_CONTAINER,
                item("1", "page-one", 10),
                item("2", "page-two", 20),
                item("3", "page-three", 30));
        LOG.info("Azure Cosmos DB emulator started for source e2e tests");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (client != null) {
            client.close();
        }
        if (cosmosContainer != null) {
            cosmosContainer.close();
        }
    }

    protected List<SeaTunnelRow> readRows(String container, String query, int maxItemCount)
            throws Exception {
        RecordingCollector collector = new RecordingCollector();
        RecordingReaderContext context = new RecordingReaderContext();
        AzureCosmosDBSourceReader reader =
                new AzureCosmosDBSourceReader(
                        context, createConfig(container, query, maxItemCount), rowType());

        reader.open();
        try {
            reader.addSplits(Collections.singletonList(new AzureCosmosDBSourceSplit(0)));
            reader.handleNoMoreSplits();
            while (!context.isNoMoreElement()) {
                reader.pollNext(collector);
            }
        } finally {
            reader.close();
        }
        return collector.rows;
    }

    protected AzureCosmosDBConfig createConfig(String container, String query, int maxItemCount) {
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", new HashMap<String, Object>());

        Map<String, Object> options = new HashMap<>();
        options.put("endpoint", cosmosContainer.getEmulatorEndpoint());
        options.put("key", cosmosContainer.getEmulatorKey());
        options.put("database", DATABASE);
        options.put("container", container);
        options.put("query", query);
        options.put("max_item_count", maxItemCount);
        options.put("schema", schema);
        return new AzureCosmosDBConfig(ReadonlyConfig.fromMap(options));
    }

    protected SeaTunnelRowType rowType() {
        return new SeaTunnelRowType(
                new String[] {"id", "name", "score"},
                new SeaTunnelDataType[] {
                    BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                });
    }

    protected static class RecordingCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows = new ArrayList<>();
        private final Object checkpointLock = new Object();

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }

        public List<SeaTunnelRow> getRows() {
            return rows;
        }
    }

    protected static class RecordingReaderContext implements SourceReader.Context {
        private volatile boolean noMoreElement;

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {
            noMoreElement = true;
        }

        public boolean isNoMoreElement() {
            return noMoreElement;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {}

        @Override
        public org.apache.seatunnel.api.common.metrics.MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public org.apache.seatunnel.api.event.EventListener getEventListener() {
            return null;
        }
    }

    private void installEmulatorTrustStore() throws Exception {
        KeyStore keyStore = cosmosContainer.buildNewKeyStore();
        Path trustStore = Files.createTempFile("cosmos-emulator", ".jks");
        try (OutputStream outputStream = Files.newOutputStream(trustStore)) {
            keyStore.store(outputStream, TRUST_STORE_PASSWORD.toCharArray());
        }
        System.setProperty("javax.net.ssl.trustStore", trustStore.toAbsolutePath().toString());
        System.setProperty("javax.net.ssl.trustStorePassword", TRUST_STORE_PASSWORD);
    }

    /**
     * Retries idempotent seeding until the emulator accepts data-plane requests.
     *
     * <p>The emulator's container health endpoint can be available before its database service
     * finishes accepting collection creation requests. Beyond that initial gap, the emulator's
     * gateway has also been observed (see CI run 33715122800, job "all-connectors-it-4") to stall
     * on an individual request for the client's full {@code httpNetworkRequestTimeout} (1 minute)
     * while it continues initializing in the background, even after an earlier request already
     * succeeded. A 2-minute ceiling only allows one such stall to be absorbed before the retry
     * budget is exhausted, so this is widened to 5 minutes to reliably ride out that warm-up
     * window, matching the more generous ceiling already used for the similarly slow-starting
     * Milvus readiness check in this same test module family.
     *
     * @param containerName container to initialize
     * @param items rows to persist in the container
     */
    @SafeVarargs
    private final void seedContainerWhenReady(String containerName, Map<String, Object>... items) {
        Awaitility.await()
                .ignoreExceptions()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(5, TimeUnit.MINUTES)
                .untilAsserted(() -> seedContainer(containerName, items));
    }

    @SafeVarargs
    private final void seedContainer(String containerName, Map<String, Object>... items) {
        client.createDatabaseIfNotExists(DATABASE);
        CosmosDatabase database = client.getDatabase(DATABASE);
        try {
            database.getContainer(containerName).delete();
        } catch (Exception ignored) {
            // Container may not exist during first setup.
        }
        database.createContainerIfNotExists(new CosmosContainerProperties(containerName, "/id"));
        CosmosContainer container = database.getContainer(containerName);
        for (Map<String, Object> item : items) {
            container.upsertItem(item);
        }
    }

    private Map<String, Object> item(String id, String name, int score) {
        Map<String, Object> item = new HashMap<>();
        item.put("id", id);
        item.put("name", name);
        item.put("score", score);
        return item;
    }
}
