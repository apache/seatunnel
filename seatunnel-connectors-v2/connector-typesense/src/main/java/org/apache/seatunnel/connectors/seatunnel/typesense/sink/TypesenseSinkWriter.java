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

package org.apache.seatunnel.connectors.seatunnel.typesense.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.common.utils.RetryUtils.RetryMaterial;
import org.apache.seatunnel.connectors.seatunnel.typesense.client.TypesenseClient;
import org.apache.seatunnel.connectors.seatunnel.typesense.dto.CollectionInfo;
import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink.TypesenseRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.typesense.state.TypesenseCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.typesense.state.TypesenseSinkState;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.api.table.type.RowKind.INSERT;
import static org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorErrorCode.INSERT_DOC_ERROR;

@Slf4j
public class TypesenseSinkWriter
        implements SinkWriter<SeaTunnelRow, TypesenseCommitInfo, TypesenseSinkState>,
                SupportMultiTableSinkWriter<Void> {

    private final Context context;
    private final int maxBatchSize;
    private final SeaTunnelRowSerializer seaTunnelRowSerializer;

    private final List<String> requestEsList;

    private final String collection;
    private TypesenseClient typesenseClient;
    private RetryMaterial retryMaterial;
    private static final long DEFAULT_SLEEP_TIME_MS = 200L;

    public TypesenseSinkWriter(
            Context context,
            CatalogTable catalogTable,
            ReadonlyConfig config,
            int maxBatchSize,
            int maxRetryCount) {
        this.context = context;
        this.maxBatchSize = maxBatchSize;

        collection = catalogTable.getTableId().getTableName();
        CollectionInfo collectionInfo =
                new CollectionInfo(catalogTable.getTableId().getTableName(), config);
        typesenseClient = TypesenseClient.createInstance(config);
        this.seaTunnelRowSerializer =
                new TypesenseRowSerializer(collectionInfo, catalogTable.getSeaTunnelRowType());

        this.requestEsList = new ArrayList<>(maxBatchSize);
        this.retryMaterial =
                new RetryMaterial(maxRetryCount, true, exception -> true, DEFAULT_SLEEP_TIME_MS);
    }

    @Override
    public void write(SeaTunnelRow element) {
        if (RowKind.UPDATE_BEFORE.equals(element.getRowKind())) {
            return;
        }

        switch (element.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                String indexRequestRow = seaTunnelRowSerializer.serializeRow(element);
                requestEsList.add(indexRequestRow);
                if (requestEsList.size() >= maxBatchSize) {
                    insert(collection, requestEsList);
                }
                break;
            case UPDATE_BEFORE:
            case DELETE:
                String id = seaTunnelRowSerializer.serializeRowForDelete(element);
                typesenseClient.deleteCollectionData(collection, id);
                break;
            default:
                throw new TypesenseConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        "Unsupported write row kind: " + element.getRowKind());
        }
    }

    @Override
    public Optional<TypesenseCommitInfo> prepareCommit() {
        insert(this.collection, this.requestEsList);
        return Optional.empty();
    }

    private void insert(String collection, List<String> requestEsList) {
        try {
            RetryUtils.retryWithException(
                    () -> {
                        typesenseClient.insert(collection, requestEsList);
                        return null;
                    },
                    retryMaterial);
            requestEsList.clear();
        } catch (Exception e) {
            log.error(INSERT_DOC_ERROR.getDescription());
            throw new TypesenseConnectorException(
                    INSERT_DOC_ERROR, INSERT_DOC_ERROR.getDescription());
        }
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() {
        insert(collection, requestEsList);
    }
}
