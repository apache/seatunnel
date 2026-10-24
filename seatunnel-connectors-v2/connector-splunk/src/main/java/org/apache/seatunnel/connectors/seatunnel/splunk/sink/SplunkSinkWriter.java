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

package org.apache.seatunnel.connectors.seatunnel.splunk.sink;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.common.utils.RetryUtils.RetryMaterial;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.splunk.client.SplunkHecClient;
import org.apache.seatunnel.connectors.seatunnel.splunk.config.SplunkSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.splunk.exception.SplunkConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.splunk.exception.SplunkConnectorException;
import org.apache.seatunnel.connectors.seatunnel.splunk.serialize.SplunkEventSerializer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Buffers rows as Splunk HEC event envelopes and POSTs them in batches to the collector.
 *
 * <p>A batch is flushed when it reaches {@code max_batch_size}, when the engine delivers a periodic
 * flush signal, on checkpoint, and on close.
 */
@Slf4j
public class SplunkSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private final SplunkSinkConfig config;
    private final SplunkEventSerializer serializer;
    private final SplunkHecClient client;
    private final RetryMaterial retryMaterial;

    /** Serialized event envelopes awaiting the next flush. */
    private final List<String> batchBuffer;

    public SplunkSinkWriter(
            SeaTunnelRowType rowType, SplunkSinkConfig config, SinkWriter.Context context) {
        this(rowType, config, context, new SplunkHecClient(config));
    }

    @VisibleForTesting
    SplunkSinkWriter(
            SeaTunnelRowType rowType,
            SplunkSinkConfig config,
            SinkWriter.Context context,
            SplunkHecClient client) {
        this.config = config;
        this.serializer = new SplunkEventSerializer(rowType, config);
        this.client = client;
        this.batchBuffer = new ArrayList<>(config.getMaxBatchSize());
        this.retryMaterial =
                new RetryMaterial(
                        config.getMaxRetryCount(),
                        true,
                        e -> e instanceof SplunkHecClient.SplunkHecRetryableException,
                        config.getRetryBackoffMs(),
                        true);

        // Opt in to engine-level timer flush, driven by the env option `sink.flush.interval`. Only
        // Zeta implements it; on Spark and Flink the Context keeps the interface's no-op default,
        // so there the buffer is flushed on max_batch_size, on checkpoint and on close.
        if (context != null) {
            context.registerFlushAction(this::flush);
        }
    }

    @Override
    public void write(SeaTunnelRow element) {
        // Splunk indexes an append-only event stream, so an UPDATE_BEFORE row would be indexed as
        // a second, misleading copy of the pre-image rather than retracting anything.
        if (RowKind.UPDATE_BEFORE.equals(element.getRowKind())) {
            return;
        }

        String event = serializer.serialize(element);
        synchronized (batchBuffer) {
            batchBuffer.add(event);
            if (batchBuffer.size() >= config.getMaxBatchSize()) {
                flush();
            }
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        flush();
        return Optional.empty();
    }

    /**
     * Sends the buffered events, retrying transient collector failures.
     *
     * <p>The buffer is cleared only after the collector has accepted the batch, so a failed attempt
     * never silently drops events.
     */
    @VisibleForTesting
    void flush() {
        synchronized (batchBuffer) {
            if (batchBuffer.isEmpty()) {
                return;
            }
            int eventCount = batchBuffer.size();
            String body = String.join("\n", batchBuffer);
            try {
                RetryUtils.retryWithException(
                        () -> {
                            client.send(body);
                            return null;
                        },
                        retryMaterial);
            } catch (SplunkConnectorException e) {
                // RetryUtils rethrows a non-retryable failure unchanged, so this batch was rejected
                // permanently on the first attempt. Claiming a retry count here would be wrong.
                throw new SplunkConnectorException(
                        SplunkConnectorErrorCode.SEND_EVENTS_FAILED,
                        String.format(
                                "Failed to send a batch of %d event(s) to the Splunk HTTP Event "
                                        + "Collector: the collector rejected it permanently, so it was not retried",
                                eventCount),
                        e);
            } catch (Exception e) {
                throw new SplunkConnectorException(
                        SplunkConnectorErrorCode.SEND_EVENTS_FAILED,
                        String.format(
                                "Failed to send a batch of %d event(s) to the Splunk HTTP Event Collector "
                                        + "after %d attempt(s)",
                                eventCount, config.getMaxRetryCount()),
                        e);
            }
            batchBuffer.clear();
            log.debug("Sent {} event(s) to the Splunk HTTP Event Collector", eventCount);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            flush();
        } finally {
            client.close();
        }
    }
}
