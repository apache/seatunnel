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

package org.apache.seatunnel.connectors.seatunnel.websocket.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.websocket.config.WebSocketSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Reads the frames buffered by {@link WebSocketSourceClient} and turns them into {@link
 * SeaTunnelRow}s.
 *
 * <p>{@code pollNext} is overridden instead of {@code internalPollNext} because the base class only
 * invokes {@code internalPollNext} once, which does not fit a continuously pushing source.
 */
@Slf4j
public class WebSocketSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private final WebSocketSourceConfig config;
    private final SingleSplitReaderContext context;
    private final WebSocketDeserializationCollector deserializationCollector;

    private WebSocketSourceClient client;
    private long emittedRecords;
    private long lastRecordTimestamp;
    private boolean noMoreElementSignaled;

    WebSocketSourceReader(
            WebSocketSourceConfig config,
            SingleSplitReaderContext context,
            DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.config = config;
        this.context = context;
        this.deserializationCollector =
                new WebSocketDeserializationCollector(deserializationSchema);
    }

    /** Opens the connection to the websocket server. Called once before the first read. */
    @Override
    public void open() {
        this.lastRecordTimestamp = System.currentTimeMillis();
        this.client = new WebSocketSourceClient(config);
        this.client.start();
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (noMoreElementSignaled) {
            return;
        }
        String message = client.poll();
        if (message != null) {
            synchronized (output.getCheckpointLock()) {
                emittedRecords += collect(message, output);
            }
            lastRecordTimestamp = System.currentTimeMillis();
        }
        // the bounded stop conditions are checked before the connection errors on purpose: a server
        // that closes the connection right after pushing all its data must not fail a batch job
        // whose stop condition is already satisfied
        if (Boundedness.BOUNDED.equals(context.getBoundedness()) && reachedBoundedEnd()) {
            signalNoMoreElement();
            return;
        }
        client.rethrowFatalIfNeeded();
    }

    private int collect(String message, Collector<SeaTunnelRow> output) {
        try {
            return deserializationCollector.collect(
                    message.getBytes(StandardCharsets.UTF_8), output);
        } catch (Exception deserializeException) {
            throw new WebSocketConnectorException(
                    WebSocketConnectorErrorCode.DESERIALIZE_FAILED,
                    String.format(
                            "Deserialize the message received from websocket server [%s] failed. "
                                    + "The incoming data does not match the configured schema or format [%s].",
                            config.getMaskedUrl(), config.getFormat()),
                    deserializeException);
        }
    }

    private boolean reachedBoundedEnd() {
        if (config.getMaxRecords() > 0 && emittedRecords >= config.getMaxRecords()) {
            log.info(
                    "Reached max_records [{}], stop reading from websocket server [{}]",
                    config.getMaxRecords(),
                    config.getMaskedUrl());
            return true;
        }
        if (config.getReadTimeoutMs() > 0
                && System.currentTimeMillis() - lastRecordTimestamp > config.getReadTimeoutMs()) {
            log.info(
                    "No message received from websocket server [{}] for more than read_timeout_ms [{}], "
                            + "stop reading with [{}] rows emitted",
                    config.getMaskedUrl(),
                    config.getReadTimeoutMs(),
                    emittedRecords);
            return true;
        }
        return false;
    }

    private void signalNoMoreElement() {
        noMoreElementSignaled = true;
        context.signalNoMoreElement();
    }
}
