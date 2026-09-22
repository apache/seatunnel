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

package org.apache.seatunnel.connectors.seatunnel.sentry.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.sentry.exception.SentryConnectorException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

final class SentrySourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    static SentryConnectorException failure(String message) {
        return new SentryConnectorException(
                CommonErrorCodeDeprecated.READER_OPERATION_FAILED, message);
    }

    private final SentrySourceConfig config;
    private final SingleSplitReaderContext context;
    private volatile boolean closed;
    private SentryClient client;

    SentrySourceReader(SentrySourceConfig config, SingleSplitReaderContext context) {
        this.config = config;
        this.context = context;
    }

    /** Create HTTP resources on the worker, not inside serialized configuration or split state. */
    @Override
    public synchronized void open() {
        if (closed || client != null) {
            throw failure("Reader closed or already opened");
        }
        client = new SentryClient(config);
    }

    /** Read a complete bounded window. Recovery replays the window, not unstable remote offsets. */
    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        Set<String> visited = new HashSet<>();
        String cursor = null;
        for (int page = 0; ; page++) {
            checkOpen();
            if (page >= config.maxPages) {
                throw failure("Sentry query exceeds max_pages; use a smaller time window");
            }
            SentryPage result = client.page(cursor);
            if (result.nextCursor != null && !visited.add(result.nextCursor)) {
                throw failure("Sentry returned a repeated pagination cursor");
            }
            for (SeaTunnelRow row : result.rows) {
                checkOpen();
                output.collect(row);
            }
            cursor = result.nextCursor;
            if (cursor == null) {
                break;
            }
        }
        checkOpen();
        context.signalNoMoreElement();
    }

    private void checkOpen() {
        if (closed || Thread.currentThread().isInterrupted() || client == null) {
            throw failure("Sentry reader is not open or was cancelled");
        }
    }

    /** Abort in-flight requests, wake retry waits, and release all worker-local resources. */
    @Override
    public synchronized void close() throws IOException {
        closed = true;
        if (client != null) {
            client.close();
        }
    }
}
