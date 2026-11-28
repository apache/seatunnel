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

package org.apache.seatunnel.connectors.seatunnel.coap;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;
import org.eclipse.californium.core.coap.Message.OffloadMode;
import org.eclipse.californium.core.coap.MessageObserverAdapter;
import org.eclipse.californium.core.coap.Response;
import org.eclipse.californium.core.network.Exchange;
import org.eclipse.californium.core.server.MessageDeliverer;
import org.eclipse.californium.core.server.resources.CoapExchange;
import org.eclipse.californium.core.server.resources.Resource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Delivers a completed protocol body at most once within the SDK's active deduplication window. */
final class CoapReader implements SourceReader<SeaTunnelRow, CoapSource.Split> {
    private static final int POLL_LIMIT = 32;
    private final Context context;
    private final CoapSettings.Listener settings;
    private final CatalogTable table;
    private final String fingerprint;
    private final AtomicReference<CoapSource.Split> assignment = new AtomicReference<>();
    private final AtomicReference<Session> session = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private CoapSource.Split state;
    private boolean eof;

    CoapReader(
            Context context,
            CoapSettings.Listener settings,
            CatalogTable table,
            String fingerprint) {
        this.context = context;
        this.settings = settings;
        this.table = table;
        this.fingerprint = fingerprint;
    }

    @Override
    public void open() {}

    /**
     * Acquires listener ownership only after the split is assigned. Neither UDP I/O nor protocol
     * decoding holds the engine checkpoint lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws IOException {
        if (closed.get() || eof) {
            return;
        }
        Object lock = output.getCheckpointLock();
        synchronized (lock) {
            if (state == null) {
                CoapSource.Split initial = assignment.getAndSet(null);
                if (initial == null) {
                    return;
                }
                if (!fingerprint.equals(initial.fingerprint)) {
                    throw CoapSettings.invalid(
                            "listener settings changed during checkpoint restore");
                }
                state = initial;
            }
        }

        if (isFinished()) {
            Session listener = session.get();
            if (listener == null || listener.pendingReceipts.get() == 0) {
                finish();
            }
            return;
        }
        Session listener = session.get();
        if (listener == null) {
            listener = openListener();
            if (listener == null) {
                return;
            }
        }
        synchronized (lock) {
            int emittedThisPoll = 0;
            while (emittedThisPoll < POLL_LIMIT && !closed.get() && !isFinished()) {
                List<SeaTunnelRow> rows = listener.queue.poll();
                if (rows == null) {
                    break;
                }
                for (SeaTunnelRow row : rows) {
                    if (closed.get()) {
                        return;
                    }
                    row.setTableId(table.getTablePath().toString());
                    output.collect(row);
                }
                emittedThisPoll += Math.max(1, rows.size());
                state = new CoapSource.Split(fingerprint, state.emitted + 1);
            }
        }
    }

    private boolean isFinished() {
        return settings.limit > 0 && state.emitted >= settings.limit;
    }

    private Session openListener() throws IOException {
        Session listener = new Session(settings, table, state.emitted);
        if (closed.get() || !session.compareAndSet(null, listener)) {
            listener.close();
            return null;
        }
        if (closed.get()) {
            stopListener();
            return null;
        }
        try {
            listener.start();
            return listener;
        } catch (IOException error) {
            stopListener();
            if (!closed.get()) {
                throw error;
            }
            return null;
        }
    }

    private void finish() {
        stopListener();
        eof = true;
        context.signalNoMoreElement();
    }

    /**
     * Persists emitted progress. The protocol queue and already acknowledged datagrams are
     * inherently transient and are not represented as replayable remote offsets.
     */
    @Override
    public List<CoapSource.Split> snapshotState(long checkpointId) {
        CoapSource.Split split = state == null ? assignment.get() : state;
        return split == null ? Collections.emptyList() : Collections.singletonList(split);
    }

    @Override
    public void addSplits(List<CoapSource.Split> splits) {
        if (splits.size() != 1 || !assignment.compareAndSet(null, splits.get(0))) {
            throw CoapSettings.invalid("only one listener split can be assigned");
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void cancel() {
        close();
    }

    @Override
    public void close() {
        closed.set(true);
        stopListener();
    }

    private void stopListener() {
        Session current = session.getAndSet(null);
        if (current != null) {
            current.close();
        }
    }

    /** Owns one listener and its transient protocol admission and response state. */
    private static final class Session implements AutoCloseable {
        private final CoapRuntime runtime;
        private final CoapSettings.Listener settings;
        private final CoapPayload.Decoder decoder;
        private final ArrayBlockingQueue<List<SeaTunnelRow>> queue;
        private final AtomicInteger pendingReceipts = new AtomicInteger();
        private boolean accepting = true;
        private long accepted;

        private Session(CoapSettings.Listener settings, CatalogTable table, long emitted) {
            this.settings = settings;
            this.queue = new ArrayBlockingQueue<>(settings.queue);
            this.decoder = CoapPayload.decoder(settings, table);
            this.runtime = CoapRuntime.listener(settings);
            this.accepted = emitted;
            Resource discovery = runtime.server.getRoot().getChild(".well-known");
            if (discovery != null) {
                runtime.server.remove(discovery);
            }
            Resource root = runtime.server.getRoot();
            for (int i = 0; i < settings.path.size() - 1; i++) {
                Resource path = new CoapResource(settings.path.get(i));
                root.add(path);
                root = path;
            }
            root.add(new Receiver(settings.path.get(settings.path.size() - 1)));
            MessageDeliverer delegate = runtime.server.getMessageDeliverer();
            runtime.server.setMessageDeliverer(
                    new MessageDeliverer() {
                        @Override
                        public void deliverRequest(Exchange exchange) {
                            try {
                                delegate.deliverRequest(exchange);
                            } finally {
                                // The UDP endpoint offloads the last block after sending its
                                // response. Release the separately assembled body for every route.
                                if (exchange.getRequest() != exchange.getCurrentRequest()) {
                                    exchange.getRequest().offload(OffloadMode.PAYLOAD);
                                }
                            }
                        }

                        @Override
                        public void deliverResponse(Exchange exchange, Response response) {
                            delegate.deliverResponse(exchange, response);
                        }
                    });
        }

        private void start() throws IOException {
            runtime.start();
        }

        private synchronized ResponseCode enqueue(List<SeaTunnelRow> rows) {
            if (!accepting || (settings.limit > 0 && accepted >= settings.limit)) {
                return ResponseCode.SERVICE_UNAVAILABLE;
            }
            pendingReceipts.incrementAndGet();
            if (!queue.offer(rows)) {
                pendingReceipts.decrementAndGet();
                return ResponseCode.SERVICE_UNAVAILABLE;
            }
            accepted++;
            return ResponseCode.VALID;
        }

        @Override
        public void close() {
            synchronized (this) {
                accepting = false;
                queue.clear();
            }
            runtime.close();
        }

        /**
         * Validates one complete request representation before offering its records to the reader.
         */
        private final class Receiver extends CoapResource {
            private Receiver(String name) {
                super(name);
            }

            @Override
            public void handleGET(CoapExchange exchange) {
                ingest(exchange);
            }

            @Override
            public void handlePOST(CoapExchange exchange) {
                ingest(exchange);
            }

            @Override
            public void handlePUT(CoapExchange exchange) {
                ingest(exchange);
            }

            private void ingest(CoapExchange exchange) {
                if (exchange.getRequestOptions().hasObserve()) {
                    exchange.respond(ResponseCode.BAD_OPTION);
                    return;
                }
                if (exchange.getRequestPayloadSize() > settings.maxBytes) {
                    exchange.respond(ResponseCode.REQUEST_ENTITY_TOO_LARGE);
                    return;
                }
                int supplied = exchange.getRequestOptions().getContentFormat();
                if (exchange.getRequestOptions().hasContentFormat()
                        && supplied != CoapPayload.contentFormat(settings.format)) {
                    exchange.respond(ResponseCode.UNSUPPORTED_CONTENT_FORMAT);
                    return;
                }
                List<SeaTunnelRow> rows;
                try {
                    synchronized (decoder) {
                        rows =
                                decoder.decode(
                                        exchange.getRequestPayload(),
                                        exchange.getSourceSocketAddress());
                    }
                    CoapPayload.limitRows(rows, settings.maxBytes);
                } catch (IOException | RuntimeException invalidPayload) {
                    exchange.respond(ResponseCode.BAD_REQUEST);
                    return;
                }
                ResponseCode code = enqueue(rows);
                if (code == ResponseCode.VALID) {
                    sendReceipt(exchange);
                } else {
                    exchange.respond(code);
                }
            }

            private void sendReceipt(CoapExchange exchange) {
                Response receipt = new Response(ResponseCode.VALID);
                AtomicBoolean completed = new AtomicBoolean();
                receipt.addMessageObserver(
                        new MessageObserverAdapter() {
                            @Override
                            public void onSent(boolean retransmission) {
                                finished();
                            }

                            @Override
                            public void onCancel() {
                                finished();
                            }

                            @Override
                            protected void failed() {
                                finished();
                            }

                            private void finished() {
                                if (completed.compareAndSet(false, true)) {
                                    pendingReceipts.decrementAndGet();
                                }
                            }
                        });
                try {
                    exchange.respond(receipt);
                } catch (RuntimeException error) {
                    receipt.cancel();
                    throw error;
                }
            }
        }
    }
}
