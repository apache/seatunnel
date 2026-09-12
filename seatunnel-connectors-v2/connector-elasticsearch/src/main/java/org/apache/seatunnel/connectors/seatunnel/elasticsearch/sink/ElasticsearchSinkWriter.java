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
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.SinkContextProxy;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.schema.event.AlterColumnCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventHandler;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.common.utils.RetryUtils.RetryMaterial;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog.ElasticSearchTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.BulkResponse;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ElasticsearchClusterInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.IndexInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.ElasticsearchRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchSinkState;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * ElasticsearchSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Elasticsearch.
 */
@Slf4j
public class ElasticsearchSinkWriter
        implements SinkWriter<SeaTunnelRow, ElasticsearchCommitInfo, ElasticsearchSinkState>,
                SupportMultiTableSinkWriter<EsRestClient>,
                SupportSchemaEvolutionSinkWriter {

    private final Context context;

    private final int maxBatchSize;

    private SeaTunnelRowSerializer seaTunnelRowSerializer;
    private final List<String> requestEsList;
    private EsRestClient esRestClient;

    // Cluster metadata cached by the owner of esRestClient.
    private ElasticsearchClusterInfo clusterInfo;

    // Whether this writer, instead of the multi-table resource manager, owns the REST client.
    private boolean ownsEsRestClient;

    // Resource manager that owns the shared client injected into this multi-table writer.
    private ElasticsearchMultiTableResourceManager multiTableResourceManager;

    // Retained connection-group resource released exactly once when this writer closes.
    private ElasticsearchMultiTableResourceManager.ClientResource multiTableClientResource;

    private RetryMaterial retryMaterial;
    private static final long DEFAULT_SLEEP_TIME_MS = 200L;
    private final IndexInfo indexInfo;

    // Initial physical row type used to build the serializer after shared-client injection.
    private final SeaTunnelRowType initialRowType;

    private TableSchema tableSchema;
    private final TableSchemaChangeEventHandler tableSchemaChangeEventHandler;
    private final ReadonlyConfig config;

    public ElasticsearchSinkWriter(
            Context context,
            CatalogTable catalogTable,
            ReadonlyConfig config,
            int maxBatchSize,
            int maxRetryCount) {
        this.context = context;
        this.maxBatchSize = maxBatchSize;
        this.config = config;

        this.indexInfo =
                new IndexInfo(catalogTable.getTableId().getTableName().toLowerCase(), config);
        this.initialRowType = catalogTable.getSeaTunnelRowType();
        this.requestEsList = new ArrayList<>(maxBatchSize);
        this.retryMaterial =
                new RetryMaterial(maxRetryCount, true, exception -> true, DEFAULT_SLEEP_TIME_MS);
        this.tableSchema = catalogTable.getTableSchema();
        this.tableSchemaChangeEventHandler = new TableSchemaChangeEventDispatcher();

        // MultiTableSinkWriter injects one shared client after constructing all table writers.
        // Standalone writers keep the existing fail-fast connection initialization behavior.
        if (!(context instanceof SinkContextProxy)) {
            initializeStandaloneClient();
        }
        context.registerFlushAction(this::timerFlush);
    }

    @Override
    public MultiTableResourceManager<EsRestClient> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        return new ElasticsearchMultiTableResourceManager();
    }

    @Override
    public void setMultiTableResourceManager(
            MultiTableResourceManager<EsRestClient> multiTableResourceManager, int queueIndex) {
        if (!(multiTableResourceManager instanceof ElasticsearchMultiTableResourceManager)) {
            throw new IllegalArgumentException(
                    "Elasticsearch multi-table writer requires ElasticsearchMultiTableResourceManager");
        }
        releaseSharedClientResource();
        closeOwnedClient();
        ElasticsearchMultiTableResourceManager resourceManager =
                (ElasticsearchMultiTableResourceManager) multiTableResourceManager;
        try {
            ElasticsearchMultiTableResourceManager.ClientResource clientResource =
                    resourceManager.getOrCreateClientResource(config);
            this.esRestClient = clientResource.getEsRestClient();
            this.clusterInfo = clientResource.getClusterInfo();
            this.multiTableResourceManager = resourceManager;
            this.multiTableClientResource = clientResource;
            this.ownsEsRestClient = false;
            initializeSerializer(initialRowType);
        } catch (RuntimeException | Error e) {
            // A failed injection aborts MultiTableSinkWriter construction, whose close lifecycle
            // will not run. Close every cached connection group before propagating the failure.
            resourceManager.close();
            this.multiTableResourceManager = null;
            this.multiTableClientResource = null;
            this.esRestClient = null;
            this.clusterInfo = null;
            throw e;
        }
    }

    @Override
    public void write(SeaTunnelRow element) {
        if (RowKind.UPDATE_BEFORE.equals(element.getRowKind())) {
            return;
        }

        String indexRequestRow = seaTunnelRowSerializer.serializeRow(element);
        requestEsList.add(indexRequestRow);
        if (requestEsList.size() >= maxBatchSize) {
            bulkEsWithRetry(this.esRestClient, this.requestEsList);
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        if (isCommentOnlyEvent(event)) {
            log.debug("Ignore comment-only schema change event: {}", event);
        } else if (event instanceof AlterTableColumnsEvent) {
            for (AlterTableColumnEvent columnEvent : ((AlterTableColumnsEvent) event).getEvents()) {
                applySingleSchemaChangeEvent(columnEvent);
            }
        } else if (event instanceof AlterTableColumnEvent) {
            applySingleSchemaChangeEvent(event);
        } else {
            throw new UnsupportedOperationException("Unsupported alter table event: " + event);
        }

        this.tableSchema = tableSchemaChangeEventHandler.reset(tableSchema).apply(event);

        initializeSerializer(tableSchema.toPhysicalRowDataType());
    }

    static boolean isCommentOnlyEvent(SchemaChangeEvent event) {
        return event instanceof AlterTableCommentEvent || event instanceof AlterColumnCommentEvent;
    }

    private void applySingleSchemaChangeEvent(SchemaChangeEvent event) {
        if (isCommentOnlyEvent(event)) {
            log.debug("Ignore comment-only schema change event: {}", event);
        } else if (event instanceof AlterTableAddColumnEvent) {
            AlterTableAddColumnEvent addColumnEvent = (AlterTableAddColumnEvent) event;
            Column column = addColumnEvent.getColumn();
            BasicTypeDefine<EsType> reconvert =
                    ElasticSearchTypeConverter.INSTANCE.reconvert(column);
            esRestClient.addField(indexInfo.getIndex(), reconvert);
            log.info("Add column {} to index {}", column.getName(), indexInfo.getIndex());
        } else {
            throw new SeaTunnelException("Unsupported schemaChangeEvent : " + event.getEventType());
        }
    }

    @Override
    public Optional<ElasticsearchCommitInfo> prepareCommit() {
        bulkEsWithRetry(this.esRestClient, this.requestEsList);
        return Optional.empty();
    }

    /**
     * Flushes pending bulk requests when the Zeta engine delivers a timer flush signal.
     *
     * <p>The action is registered before multi-table resource injection but invoked after startup.
     */
    private void timerFlush() {
        bulkEsWithRetry(this.esRestClient, this.requestEsList);
    }

    @Override
    public void abortPrepare() {}

    public synchronized void bulkEsWithRetry(
            EsRestClient esRestClient, List<String> requestEsList) {
        try {
            RetryUtils.retryWithException(
                    () -> {
                        if (!requestEsList.isEmpty()) {
                            String requestBody = String.join("\n", requestEsList) + "\n";
                            BulkResponse bulkResponse = esRestClient.bulk(requestBody);
                            if (bulkResponse.isErrors()) {
                                throw new ElasticsearchConnectorException(
                                        ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                                        "bulk es error: " + bulkResponse.getResponse());
                            }
                            return bulkResponse;
                        }
                        return null;
                    },
                    retryMaterial);
            requestEsList.clear();
        } catch (Exception e) {
            throw new ElasticsearchConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "ElasticSearch execute batch statement error",
                    e);
        }
    }

    @Override
    public void close() {
        if (esRestClient == null) {
            return;
        }
        try {
            bulkEsWithRetry(this.esRestClient, this.requestEsList);
        } finally {
            releaseSharedClientResource();
            closeOwnedClient();
        }
    }

    /**
     * Initializes the client owned by a standalone writer and preserves fail-fast startup
     * validation.
     */
    private void initializeStandaloneClient() {
        EsRestClient client = EsRestClient.createInstance(config);
        try {
            this.clusterInfo = client.getClusterInfo();
            this.esRestClient = client;
            this.ownsEsRestClient = true;
            initializeSerializer(initialRowType);
        } catch (RuntimeException e) {
            client.close();
            throw e;
        }
    }

    /**
     * Rebuilds the table-specific serializer using cluster metadata cached by the client owner.
     *
     * @param rowType current physical row type
     */
    private void initializeSerializer(SeaTunnelRowType rowType) {
        List<String> vectorizationFields =
                config.getOptional(ElasticsearchSinkOptions.VECTORIZATION_FIELDS)
                        .orElse(Collections.emptyList());
        int vectorDimension = config.get(ElasticsearchSinkOptions.VECTOR_DIMENSIONS);
        this.seaTunnelRowSerializer =
                new ElasticsearchRowSerializer(
                        clusterInfo, indexInfo, rowType, vectorizationFields, vectorDimension);
    }

    /**
     * Closes only a client owned by this writer; shared clients are closed by their manager.
     *
     * <p>Clearing the ownership flag makes repeated writer close calls safe.
     */
    private void closeOwnedClient() {
        if (ownsEsRestClient && esRestClient != null) {
            esRestClient.close();
            ownsEsRestClient = false;
        }
    }

    /**
     * Releases the shared multi-table client reference retained during resource injection.
     *
     * <p>The manager closes the underlying REST client only when this writer was the last active
     * user of the connection group.
     */
    private void releaseSharedClientResource() {
        if (multiTableResourceManager != null && multiTableClientResource != null) {
            multiTableResourceManager.releaseClientResource(multiTableClientResource);
            multiTableResourceManager = null;
            multiTableClientResource = null;
        }
    }
}
