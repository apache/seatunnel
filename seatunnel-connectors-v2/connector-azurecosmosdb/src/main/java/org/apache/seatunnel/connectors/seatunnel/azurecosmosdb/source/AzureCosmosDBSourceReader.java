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

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBConfig;
import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.serialize.CosmosItemDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

public class AzureCosmosDBSourceReader
        implements SourceReader<SeaTunnelRow, AzureCosmosDBSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(AzureCosmosDBSourceReader.class);

    private final SourceReader.Context context;
    private final AzureCosmosDBConfig config;
    private final CosmosItemDeserializer deserializer;

    private final Queue<AzureCosmosDBSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();

    private CosmosClient client;
    private CosmosContainer container;
    private AzureCosmosDBSourceSplit currentSplit;

    private volatile boolean noMoreSplit;
    private volatile boolean finished;

    public AzureCosmosDBSourceReader(
            SourceReader.Context context, AzureCosmosDBConfig config, SeaTunnelRowType rowType) {
        this.context = context;
        this.config = config;
        this.deserializer = new CosmosItemDeserializer(rowType);
    }

    @Override
    public void open() {
        try {
            this.client =
                    new CosmosClientBuilder()
                            .endpoint(config.getResolvedEndpoint())
                            .key(config.getResolvedKey())
                            .endpointDiscoveryEnabled(false)
                            .gatewayMode()
                            .buildClient();
            this.container =
                    client.getDatabase(config.getDatabase()).getContainer(config.getContainer());
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to open AzureCosmosDB source reader for database [%s], container [%s]",
                            config.getDatabase(), config.getContainer()),
                    e);
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        AzureCosmosDBSourceSplit activeSplit;
        String continuationToken;

        synchronized (output.getCheckpointLock()) {
            if (finished) {
                return;
            }

            if (currentSplit == null) {
                currentSplit = pendingSplits.poll();
            }
            if (currentSplit == null) {
                if (noMoreSplit) {
                    finishReader();
                }
                return;
            }

            activeSplit = currentSplit;
            continuationToken = activeSplit.getContinuationToken();
        }

        FeedResponse<Object> page = fetchPage(continuationToken);
        List<SeaTunnelRow> rows = deserializePage(page);

        synchronized (output.getCheckpointLock()) {
            applyPage(activeSplit, page, rows, output);
            finishReaderIfNoMoreWork();
        }
    }

    @Override
    public List<AzureCosmosDBSourceSplit> snapshotState(long checkpointId) {
        List<AzureCosmosDBSourceSplit> state = new ArrayList<>();
        pendingSplits.forEach(split -> state.add(split.copy()));
        if (currentSplit != null) {
            state.add(currentSplit.copy());
        }
        return state;
    }

    @Override
    public void addSplits(List<AzureCosmosDBSourceSplit> splits) {
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        this.noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // no-op
    }

    private List<SeaTunnelRow> deserializePage(FeedResponse<Object> page) {
        List<SeaTunnelRow> rows = new ArrayList<>();
        if (page == null) {
            return rows;
        }

        for (Object item : page.getResults()) {
            if (Objects.nonNull(item)) {
                rows.add(deserializer.deserialize(item));
            }
        }
        return rows;
    }

    private void applyPage(
            AzureCosmosDBSourceSplit activeSplit,
            FeedResponse<Object> page,
            List<SeaTunnelRow> rows,
            Collector<SeaTunnelRow> output) {
        if (finished || currentSplit != activeSplit) {
            return;
        }

        if (page == null) {
            finishCurrentSplit();
            return;
        }

        rows.forEach(output::collect);

        String continuationToken = page.getContinuationToken();
        currentSplit.setContinuationToken(continuationToken);
        if (isLastPage(continuationToken)) {
            finishCurrentSplit();
        }
    }

    FeedResponse<Object> fetchPage(String continuationToken) {
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();

        try {
            Iterator<FeedResponse<Object>> pageIterator;
            if (isLastPage(continuationToken)) {
                pageIterator =
                        container
                                .<Object>queryItems(config.getQuery(), queryOptions, Object.class)
                                .iterableByPage(config.getMaxItemCount())
                                .iterator();
            } else {
                pageIterator =
                        container
                                .<Object>queryItems(config.getQuery(), queryOptions, Object.class)
                                .iterableByPage(continuationToken, config.getMaxItemCount())
                                .iterator();
            }
            return pageIterator.hasNext() ? pageIterator.next() : null;
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to read AzureCosmosDB data from database [%s], container [%s] with query [%s]",
                            config.getDatabase(), config.getContainer(), config.getQuery()),
                    e);
        }
    }

    private static boolean isLastPage(String continuationToken) {
        return continuationToken == null || continuationToken.isEmpty();
    }

    private void finishReader() {
        context.signalNoMoreElement();
        finished = true;
    }

    private void finishReaderIfNoMoreWork() {
        if (currentSplit == null && pendingSplits.isEmpty() && noMoreSplit) {
            finishReader();
        }
    }

    private void finishCurrentSplit() {
        LOG.info("AzureCosmosDB reader [{}] finished source scan", context.getIndexOfSubtask());
        currentSplit = null;
    }

    int getQueryPageSize() {
        return config.getMaxItemCount();
    }
}
