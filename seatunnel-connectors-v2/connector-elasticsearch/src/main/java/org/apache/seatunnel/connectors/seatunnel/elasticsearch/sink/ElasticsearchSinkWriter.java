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
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.common.utils.RetryUtils.RetryMaterial;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog.ElasticSearchTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.BulkResponse;
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
import java.util.List;
import java.util.Optional;

/**
 * ElasticsearchSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Elasticsearch.
 */
@Slf4j
public class ElasticsearchSinkWriter
        implements SinkWriter<SeaTunnelRow, ElasticsearchCommitInfo, ElasticsearchSinkState>,
                SupportMultiTableSinkWriter<Void>,
                SupportSchemaEvolutionSinkWriter {

    private final Context context;

    private final int maxBatchSize;

    private final SeaTunnelRowSerializer seaTunnelRowSerializer;
    private final List<String> requestEsList;
    private EsRestClient esRestClient;
    private RetryMaterial retryMaterial;
    private static final long DEFAULT_SLEEP_TIME_MS = 200L;
    private final IndexInfo indexInfo;

    public ElasticsearchSinkWriter(
            Context context,
            CatalogTable catalogTable,
            ReadonlyConfig config,
            int maxBatchSize,
            int maxRetryCount) {
        this.context = context;
        this.maxBatchSize = maxBatchSize;

        this.indexInfo =
                new IndexInfo(catalogTable.getTableId().getTableName().toLowerCase(), config);
        esRestClient = EsRestClient.createInstance(config);
        this.seaTunnelRowSerializer =
                new ElasticsearchRowSerializer(
                        esRestClient.getClusterInfo(),
                        indexInfo,
                        catalogTable.getSeaTunnelRowType());

        this.requestEsList = new ArrayList<>(maxBatchSize);
        this.retryMaterial =
                new RetryMaterial(maxRetryCount, true, exception -> true, DEFAULT_SLEEP_TIME_MS);
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
        if (event instanceof AlterTableColumnsEvent) {
            for (AlterTableColumnEvent columnEvent : ((AlterTableColumnsEvent) event).getEvents()) {
                applySingleSchemaChangeEvent(columnEvent);
            }
        } else if (event instanceof AlterTableColumnEvent) {
            applySingleSchemaChangeEvent(event);
        } else {
            throw new UnsupportedOperationException("Unsupported alter table event: " + event);
        }
    }

    private void applySingleSchemaChangeEvent(SchemaChangeEvent event) {
        if (event instanceof AlterTableAddColumnEvent) {
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

    @Override
    public void abortPrepare() {}

    public synchronized void bulkEsWithRetry(
            EsRestClient esRestClient, List<String> requestEsList) {
        try {
            RetryUtils.retryWithException(
                    () -> {
                        if (requestEsList.size() > 0) {
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
    public void close() throws IOException {
        bulkEsWithRetry(this.esRestClient, this.requestEsList);
        esRestClient.close();
    }
}
