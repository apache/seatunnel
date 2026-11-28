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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.coap.Request;
import org.eclipse.californium.core.coap.Response;
import org.eclipse.californium.elements.AddressEndpointContext;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Keeps at most one request in flight and never starts a replacement exchange on failure. */
final class CoapWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final CoapSettings.Sender settings;
    private final CoapPayload.Encoder encoder;
    private final AddressEndpointContext destination;
    private final CoapRuntime transport;
    private final AtomicReference<Request> active = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    CoapWriter(CoapSettings.Sender settings, SeaTunnelRowType rowType) throws IOException {
        this.settings = settings;
        this.encoder = CoapPayload.encoder(settings, rowType);
        this.destination = resolve(settings);
        this.transport = CoapRuntime.sender(settings);
        transport.start();
    }

    /**
     * Waits for a final 2.xx response. A timeout or RST can leave delivery unknown. Only the
     * protocol retransmission of the same confirmable message is permitted.
     */
    @Override
    public void write(SeaTunnelRow row) throws IOException {
        if (closed.get()) {
            throw failure("writer is closed");
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settings.timeout);
        Request request;
        try {
            request =
                    new Request(
                            CoAP.Code.valueOf(settings.method.name()),
                            CoAP.Type.valueOf(settings.requestType.name()));
            request.setDestinationContext(destination);
            request.setURI(settings.uri);
            if (settings.hasBody()) {
                request.setPayload(encoder.encode(row));
                request.getOptions().setContentFormat(CoapPayload.contentFormat(settings.format));
            }
        } catch (RuntimeException e) {
            throw failure("row cannot be serialized or the URI cannot be encoded");
        }
        if (!active.compareAndSet(null, request)) {
            throw failure("concurrent writer calls are unsupported");
        }
        try {
            if (closed.get()) {
                throw failure("writer is closed");
            }
            remaining(deadline);
            request.send(transport.endpoint);
            Response response = request.waitForResponse(remaining(deadline));
            if (request.isRejected()) {
                throw failure("remote reset; delivery is unknown");
            }
            if (response == null || System.nanoTime() - deadline >= 0) {
                throw failure(
                        closed.get()
                                ? "request canceled; delivery is unknown"
                                : "no final response within the request budget; delivery is unknown");
            }
            if (!response.isSuccess() || response.getCode() == CoAP.ResponseCode.CONTINUE) {
                throw failure("remote response " + response.getCode());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw failure("request interrupted; delivery is unknown");
        } catch (RuntimeException e) {
            throw failure("UDP exchange failed; delivery is unknown");
        } finally {
            active.compareAndSet(request, null);
            request.cancel();
        }
    }

    private long remaining(long deadline) throws IOException {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw failure("request deadline exceeded; delivery is unknown");
        }
        return TimeUnit.NANOSECONDS.toMillis(nanos - 1) + 1;
    }

    private IOException failure(String detail) {
        return new IOException("CoAP sink " + settings.method + ": " + detail);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            Request request = active.get();
            if (request != null) {
                request.cancel();
            }
            transport.close();
        }
    }

    private static AddressEndpointContext resolve(CoapSettings.Sender settings) throws IOException {
        FutureTask<InetAddress> lookup =
                new FutureTask<>(() -> InetAddress.getByName(settings.uri.getHost()));
        Thread resolver = new Thread(lookup, "seatunnel-coap-resolver");
        resolver.setDaemon(true);
        resolver.start();
        try {
            InetAddress address = lookup.get(settings.timeout, TimeUnit.MILLISECONDS);
            if (address.isAnyLocalAddress()
                    || address.isMulticastAddress()
                    || "255.255.255.255".equals(address.getHostAddress())) {
                throw new IOException("CoAP sink requires a unicast destination");
            }
            int port = settings.uri.getPort() < 0 ? 5683 : settings.uri.getPort();
            return new AddressEndpointContext(new InetSocketAddress(address, port));
        } catch (ExecutionException | TimeoutException e) {
            throw new IOException(
                    "CoAP sink could not resolve the destination within the request budget");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("CoAP sink destination resolution was interrupted");
        } finally {
            lookup.cancel(true);
        }
    }
}
