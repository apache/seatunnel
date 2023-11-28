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

package org.apache.seatunnel.connectors.seatunnel.easysearch.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.common.utils.RetryUtils.RetryMaterial;
import org.apache.seatunnel.connectors.seatunnel.easysearch.client.EasysearchClient;
import org.apache.seatunnel.connectors.seatunnel.easysearch.dto.BulkResponse;
import org.apache.seatunnel.connectors.seatunnel.easysearch.dto.IndexInfo;
import org.apache.seatunnel.connectors.seatunnel.easysearch.exception.EasysearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.easysearch.exception.EasysearchConnectorException;
import org.apache.seatunnel.connectors.seatunnel.easysearch.serialize.EasysearchRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.easysearch.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.easysearch.state.EasysearchCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.easysearch.state.EasysearchSinkState;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.easysearch.exception.EasysearchConnectorErrorCode.SQL_OPERATION_FAILED;

/** EasysearchSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Easysearch. */
@Slf4j
public class EasysearchSinkWriter
        implements SinkWriter<SeaTunnelRow, EasysearchCommitInfo, EasysearchSinkState> {

    private static final long DEFAULT_SLEEP_TIME_MS = 200L;
    private final SinkWriter.Context context;
    private final int maxBatchSize;
    private final SeaTunnelRowSerializer seaTunnelRowSerializer;
    private final List<String> requestEzsList;
    private EasysearchClient ezsClient;
    private RetryMaterial retryMaterial;

    public EasysearchSinkWriter(
            SinkWriter.Context context,
            SeaTunnelRowType seaTunnelRowType,
            Config pluginConfig,
            int maxBatchSize,
            int maxRetryCount) {
        this.context = context;
        this.maxBatchSize = maxBatchSize;

        IndexInfo indexInfo = new IndexInfo(pluginConfig);
        ezsClient = EasysearchClient.createInstance(pluginConfig);
        this.seaTunnelRowSerializer = new EasysearchRowSerializer(indexInfo, seaTunnelRowType);

        this.requestEzsList = new ArrayList<>(maxBatchSize);
        this.retryMaterial =
                new RetryMaterial(maxRetryCount, true, exception -> true, DEFAULT_SLEEP_TIME_MS);
    }

    @Override
    public void write(SeaTunnelRow element) {
        if (RowKind.UPDATE_BEFORE.equals(element.getRowKind())) {
            return;
        }

        String indexRequestRow = seaTunnelRowSerializer.serializeRow(element);
        requestEzsList.add(indexRequestRow);
        if (requestEzsList.size() >= maxBatchSize) {
            bulkEzsWithRetry(this.ezsClient, this.requestEzsList);
        }
    }

    @Override
    public Optional<EasysearchCommitInfo> prepareCommit() {
        bulkEzsWithRetry(this.ezsClient, this.requestEzsList);
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    public synchronized void bulkEzsWithRetry(
            EasysearchClient ezsClient, List<String> requestEzsList) {
        try {
            RetryUtils.retryWithException(
                    () -> {
                        if (requestEzsList.size() > 0) {
                            String requestBody = String.join("\n", requestEzsList) + "\n";
                            BulkResponse bulkResponse = ezsClient.bulk(requestBody);
                            if (bulkResponse.isErrors()) {
                                throw new EasysearchConnectorException(
                                        EasysearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                                        "bulk ezs error: " + bulkResponse.getResponse());
                            }
                            return bulkResponse;
                        }
                        return null;
                    },
                    retryMaterial);
            requestEzsList.clear();
        } catch (Exception e) {
            throw new EasysearchConnectorException(
                    SQL_OPERATION_FAILED, "Easysearch execute batch statement error", e);
        }
    }

    @Override
    public void close() throws IOException {
        bulkEzsWithRetry(this.ezsClient, this.requestEzsList);
        ezsClient.close();
    }
}
