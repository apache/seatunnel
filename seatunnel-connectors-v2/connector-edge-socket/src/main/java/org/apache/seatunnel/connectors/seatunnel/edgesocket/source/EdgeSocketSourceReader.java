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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.source;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorException;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.protocol.EdgeSocketResponseCode;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.protocol.IncomingRecordHandler;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.DefaultEdgeSocketRecordQueue;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.EdgeSocketQueuedRecord;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.EdgeSocketRecordQueue;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.QueueOfferResult;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.DefaultEdgeSocketPayloadDeserializer;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.DefaultEdgeSocketRecordDeserializer;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketPayloadDeserializer;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketRecordDeserializer;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.state.EdgeSocketSourceState;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
public class EdgeSocketSourceReader extends AbstractSingleSplitReader<SeaTunnelRow>
        implements IncomingRecordHandler {

    private final EdgeSocketConfig config;
    private final SingleSplitReaderContext context;
    private final EdgeSocketRecordQueue recordQueue;
    private final EdgeSocketRecordDeserializer recordDeserializer;
    private final EdgeSocketPayloadDeserializer payloadDeserializer;
    private final DeserializationSchema<SeaTunnelRow> rowDeserializationSchema;
    private final Object stateLock = new Object();
    private final EdgeSocketSourceState sourceState = new EdgeSocketSourceState();
    private final EdgeSocketIngressServer socketIngressServer;
    private long queueFullCount;

    EdgeSocketSourceReader(
            EdgeSocketConfig edgeSocketConfig,
            SingleSplitReaderContext readerContext,
            DeserializationSchema<SeaTunnelRow> rowDeserializationSchema) {
        this.config = edgeSocketConfig;
        this.context = readerContext;
        this.recordQueue =
                new DefaultEdgeSocketRecordQueue(
                        config.getLocalQueueCapacity(),
                        config.getQueueBackpressureWatermarkRatio());
        this.recordDeserializer = DefaultEdgeSocketRecordDeserializer.create(config);
        this.payloadDeserializer = DefaultEdgeSocketPayloadDeserializer.create();
        this.rowDeserializationSchema = rowDeserializationSchema;
        this.socketIngressServer = new EdgeSocketIngressServer(config, this);
    }

    @Override
    public void open() {
        socketIngressServer.start();
    }

    @Override
    public void close() throws IOException {
        socketIngressServer.stop();
    }

    @VisibleForTesting
    boolean isListening() {
        return socketIngressServer.isListening();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        socketIngressServer.rethrowFatalIfNeeded();
        synchronized (output.getCheckpointLock()) {
            synchronized (stateLock) {
                EdgeSocketQueuedRecord record = recordQueue.poll();
                if (record != null) {
                    emitRecordSafely(record, output);
                }
            }
        }
    }

    private void emitRecordSafely(EdgeSocketQueuedRecord record, Collector<SeaTunnelRow> output) {
        try {
            String payload = payloadDeserializer.deserializeRecord(record);
            SeaTunnelRow row =
                    rowDeserializationSchema.deserialize(payload.getBytes(StandardCharsets.UTF_8));
            if (row != null) {
                output.collect(row);
            }
            sourceState.markRecordEmitted(record.getBatchId());
        } catch (Exception deserializeException) {
            throw new EdgeSocketConnectorException(
                    EdgeSocketConnectorErrorCode.PACKET_DECODE_ERROR,
                    "Deserialize queued record to SeaTunnelRow failed. "
                            + "Incoming data does not match configured schema or payload format.",
                    deserializeException);
        }
    }

    @Override
    protected byte[] snapshotStateToBytes(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return sourceState.snapshotState(checkpointId, recordQueue.snapshot());
        }
    }

    @Override
    protected void restoreState(byte[] restoredState) {
        List<EdgeSocketQueuedRecord> records = sourceState.restoreState(restoredState);
        for (EdgeSocketQueuedRecord record : records) {
            QueueOfferResult result = recordQueue.offer(record);
            if (result != QueueOfferResult.ACCEPTED) {
                throw new EdgeSocketConnectorException(
                        EdgeSocketConnectorErrorCode.PACKET_DECODE_ERROR,
                        "Queue full while restoring snapshot record batchId="
                                + record.getBatchId()
                                + ". Increase local_queue_capacity and retry.");
            }
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        synchronized (stateLock) {
            sourceState.notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        synchronized (stateLock) {
            sourceState.notifyCheckpointAborted(checkpointId);
        }
    }

    @Override
    public String handleBatchRecord(long batchId, String payload) {
        synchronized (stateLock) {
            if (recordQueue.isBackpressure()) {
                queueFullCount++;
                if (queueFullCount == 1 || queueFullCount % 100 == 0) {
                    log.warn(
                            "Ingress queue at backpressure watermark, returning QUEUE_FULL:{}ms "
                                    + "(capacity={}, watermarkRatio={}, rejectCount={}, batchId={})",
                            config.getQueueFullRetryAfterMs(),
                            config.getLocalQueueCapacity(),
                            config.getQueueBackpressureWatermarkRatio(),
                            queueFullCount,
                            batchId);
                }
                return EdgeSocketResponseCode.QUEUE_FULL.withPayload(
                        config.getQueueFullRetryAfterMs());
            }
        }
        try {
            EdgeSocketQueuedRecord decoded = recordDeserializer.deserializeRecord(payload);
            decoded.setBatchId(batchId);
            synchronized (stateLock) {
                QueueOfferResult offerResult = recordQueue.offer(decoded);
                if (offerResult == QueueOfferResult.ACCEPTED) {
                    sourceState.markRecordReceived(batchId);
                    return EdgeSocketResponseCode.RECEIVED.getCode();
                }
            }
            queueFullCount++;
            if (queueFullCount == 1 || queueFullCount % 100 == 0) {
                log.warn(
                        "Ingress queue physically full, returning RETRY "
                                + "(capacity={}, rejectCount={}, batchId={})",
                        config.getLocalQueueCapacity(),
                        queueFullCount,
                        batchId);
            }
            return EdgeSocketResponseCode.RETRY.getCode();
        } catch (EdgeSocketConnectorException connectorException) {
            if (isDecryptionError(connectorException)) {
                log.warn(
                        "Decryption failed for batchId={}, check secret_key configuration",
                        batchId,
                        connectorException);
                return EdgeSocketResponseCode.DECRYPT_FAILED.getCode();
            }
            log.warn("Decode ingress packet failed for batchId={}", batchId, connectorException);
            return EdgeSocketResponseCode.DECODE_FAILED.getCode();
        } catch (Exception decodeException) {
            log.warn("Decode or enqueue ingress packet failed", decodeException);
            return EdgeSocketResponseCode.DECODE_FAILED.getCode();
        }
    }

    @Override
    public String handleCommitRequest(long batchId) {
        synchronized (stateLock) {
            return sourceState.resolveCommitResponse(batchId);
        }
    }

    private boolean isDecryptionError(EdgeSocketConnectorException exception) {
        EdgeSocketConnectorErrorCode errorCode =
                (EdgeSocketConnectorErrorCode) exception.getSeaTunnelErrorCode();
        return errorCode == EdgeSocketConnectorErrorCode.PACKET_AES_KEY_MISSING
                || (errorCode == EdgeSocketConnectorErrorCode.PACKET_DECODE_ERROR
                        && exception.getCause() instanceof java.security.GeneralSecurityException);
    }
}
