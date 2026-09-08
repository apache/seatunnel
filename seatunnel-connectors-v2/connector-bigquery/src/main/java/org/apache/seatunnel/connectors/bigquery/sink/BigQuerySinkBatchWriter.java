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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.bigquery.convert.BigQuerySerializer;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.sink.committer.BigQueryCommitInfo;
import org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryBatchWriter;
import org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryWriter;

import org.json.JSONArray;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.rpc.Code;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BigQuerySinkBatchWriter extends AbstractBigQuerySinkWriter {
    public static final String BATCH = "batch";

    private final String restoredStreamName;
    private final long restoredNextOffset;

    public BigQuerySinkBatchWriter(ReadonlyConfig readOnlyConfig, BigQuerySerializer serializer) {
        this(readOnlyConfig, serializer, null, 0L);
    }

    public BigQuerySinkBatchWriter(
            ReadonlyConfig readOnlyConfig,
            BigQuerySerializer serializer,
            String restoredStreamName,
            long restoredNextOffset) {
        super(readOnlyConfig, serializer);
        this.restoredStreamName = restoredStreamName;
        this.restoredNextOffset = restoredNextOffset;
    }

    public BigQuerySinkBatchWriter(
            ReadonlyConfig readOnlyConfig,
            BigQueryWriter streamWriter,
            BigQuerySerializer serializer) {
        super(readOnlyConfig, streamWriter, serializer);
        this.restoredStreamName = null;
        this.restoredNextOffset = 0L;
    }

    @Override
    public void setMultiTableResourceManager(
            MultiTableResourceManager<BigQueryWriteClient> manager, int queueIndex) {
        log.info("Injecting shared client and initializing Batch stream writer...");
        this.client = manager.getSharedResource().get();
        if (restoredStreamName != null) {
            this.streamWriter =
                    BigQueryBatchWriter.restore(
                            client, config, restoredStreamName, restoredNextOffset);
        } else {
            this.streamWriter = BigQueryBatchWriter.of(client, config);
        }
    }

    @Override
    protected void flush() {
        if (buffer.length() == 0) {
            return;
        }

        JSONArray dataToSend = buffer;
        buffer = new JSONArray();

        try {
            ApiFuture<AppendRowsResponse> future = streamWriter.append(dataToSend);
            AppendRowsResponse response = future.get(60, TimeUnit.SECONDS);

            if (response.hasError()) {
                if (isAlreadyExists(response)) {
                    markAppendAsSuccessful(dataToSend);
                    return;
                }
                throw new BigQueryConnectorException(
                        BigQueryConnectorErrorCode.APPEND_ROWS_FAILED,
                        response.getError().getMessage());
            }

            streamWriter.onAppendSuccess(dataToSend.length());
            log.info("Successfully appended {} rows.", dataToSend.length());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            buffer = dataToSend;
            throw new BigQueryConnectorException(BigQueryConnectorErrorCode.APPEND_ROWS_FAILED, e);
        } catch (Exception e) {
            if (isAlreadyExists(e)) {
                markAppendAsSuccessful(dataToSend);
                return;
            }
            buffer = dataToSend;
            throw new BigQueryConnectorException(BigQueryConnectorErrorCode.APPEND_ROWS_FAILED, e);
        }
    }

    private void markAppendAsSuccessful(JSONArray dataToSend) {
        streamWriter.onAppendSuccess(dataToSend.length());
        log.info(
                "BigQuery already accepted the append at the requested offset; "
                        + "advancing the local offset by {} rows.",
                dataToSend.length());
    }

    private boolean isAlreadyExists(AppendRowsResponse response) {
        if (response == null || !response.hasError()) {
            return false;
        }

        return response.getError().getCode() == Code.ALREADY_EXISTS_VALUE;
    }

    private boolean isAlreadyExists(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof ApiException) {
                StatusCode.Code code = ((ApiException) current).getStatusCode().getCode();
                return code == StatusCode.Code.ALREADY_EXISTS;
            }
            current = current.getCause();
        }
        return false;
    }

    @Override
    public void write(SeaTunnelRow element) {
        buffer.put(serializer.convert(element, false));

        if (buffer.length() >= batchSize) {
            flush();
        }
    }

    @Override
    public Optional<BigQueryCommitInfo> prepareCommit() {
        flush();

        BigQueryBatchWriter batchWriter = (BigQueryBatchWriter) streamWriter;
        long flushOffset = batchWriter.getNextOffset() - 1;

        if (flushOffset < 0) {
            return Optional.empty();
        }

        return Optional.of(new BigQueryCommitInfo(batchWriter.getStreamName(), flushOffset));
    }

    @Override
    public void abortPrepare() {
        // No external side effect is committed in prepareCommit().
        // Visibility is advanced only by the committer via FlushRows after checkpoint completion.
    }

    @Override
    public List<BigQuerySinkState> snapshotState(long checkpointId) {
        flush();

        BigQueryBatchWriter batchWriter = (BigQueryBatchWriter) streamWriter;
        return Collections.singletonList(
                new BigQuerySinkState(
                        batchWriter.getStreamName(), batchWriter.getNextOffset(), checkpointId));
    }

    @Override
    protected boolean flushOnClose() {
        // Batch mode uses BigQuery buffered streams and stores streamName + nextOffset
        // in checkpoint state. Flushing during close could append rows outside the
        // latest checkpoint state and make the external stream offset move ahead of
        // the restored nextOffset, breaking 2PC state recovery contract.
        return false;
    }
}
