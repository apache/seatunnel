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

package org.apache.seatunnel.connectors.seatunnel.websocket.sink;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.socksx.v5.DefaultSocks5PasswordAuthRequest;
import io.netty.handler.codec.socksx.v5.Socks5PasswordAuthRequest;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.util.ReferenceCountUtil;
import okhttp3.HttpUrl;
import okhttp3.Request;

import java.io.IOException;
import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Preserves the former client's JVM proxy selection without changing process-wide configuration.
 * Selection and address resolution run on the writer's event loop under the caller's opening wait.
 */
final class WebSocketSinkProxy {
    // The selector and forwarding request use the same canonical HTTP URL as the old client.
    final URI uri;
    // Keep the origin unresolved for CONNECT/SOCKS; only direct/forward socket addresses resolve.
    final InetSocketAddress destination;
    // Plain ws needs absolute-form HTTP forwarding rather than CONNECT.
    final boolean forwardHttp;
    // TCP connect alone does not establish a SOCKS/CONNECT route.
    final boolean tunnel;
    private final Proxy.Type type;
    private final InetSocketAddress address;
    private final ProxySelector selector;
    private final Proxy selected;
    // A failed transport may report through both its future and an inbound exception.
    private final AtomicBoolean failureReported = new AtomicBoolean();

    private WebSocketSinkProxy(
            URI uri,
            InetSocketAddress destination,
            boolean forwardHttp,
            Proxy.Type type,
            InetSocketAddress address,
            ProxySelector selector,
            Proxy selected) {
        this.uri = uri;
        this.destination = destination;
        this.forwardHttp = forwardHttp;
        this.tunnel = address != null && !forwardHttp;
        this.type = type;
        this.address = address;
        this.selector = selector;
        this.selected = selected;
    }

    /*
     * Like OkHttp's HttpUrl, the selector sees http/https rather than ws/wss. With connection retries
     * disabled only the first selected route is attempted. Tunnel destinations stay unresolved so
     * HTTP CONNECT and SOCKS5 resolve the origin at the proxy, not in the worker's DNS environment.
     */
    static WebSocketSinkProxy select(URI endpoint, ProxySelector selector) throws IOException {
        // Retain the existing request URL canonicalization (including default port/path and
        // ws/wss rewriting); this creates no OkHttp client, connection or incoming reader.
        HttpUrl url = new Request.Builder().url(endpoint.toASCIIString()).build().url();
        boolean secure = url.isHttps();
        URI uri = url.uri();
        String host = url.host();
        int port = url.port();
        List<Proxy> selected = selector == null ? null : selector.select(uri);
        Proxy proxy = selected == null || selected.isEmpty() ? Proxy.NO_PROXY : selected.get(0);
        if (proxy.type() == Proxy.Type.DIRECT) {
            return new WebSocketSinkProxy(
                    uri, resolve(host, port), false, proxy.type(), null, selector, proxy);
        }
        if (!(proxy.address() instanceof InetSocketAddress)) {
            throw new IOException("Unsupported JVM proxy address");
        }
        InetSocketAddress target = (InetSocketAddress) proxy.address();
        InetSocketAddress address =
                target.isUnresolved() ? resolve(target.getHostString(), target.getPort()) : target;
        boolean forward = proxy.type() == Proxy.Type.HTTP && !secure;
        return new WebSocketSinkProxy(
                uri,
                forward ? address : InetSocketAddress.createUnresolved(host, port),
                forward,
                proxy.type(),
                address,
                selector,
                proxy);
    }

    /*
     * Proxy codecs precede TLS. CONNECT carries no destination Authorization header. Their decoded
     * handshake data is streamed rather than aggregated; terminal causes reach the sanitized sink
     * listener instead of a logging handler.
     */
    void install(ChannelPipeline pipeline, int timeout, BooleanSupplier canceled) {
        if (address == null || forwardHttp) {
            return;
        }
        ProxyHandler handler;
        if (type == Proxy.Type.HTTP) {
            handler = new HttpProxyHandler(address);
        } else {
            // Match JDK Socket(proxy): offer both methods, request credentials only on challenge.
            handler = new Socks5ProxyHandler(address, "jvm-authenticator", "");
        }
        handler.setConnectTimeoutMillis(timeout);
        pipeline.addLast("proxy", handler);
        if (type == Proxy.Type.SOCKS) {
            // handlerAdded has inserted the public SOCKS encoder immediately before "proxy".
            pipeline.addBefore(
                    "proxy", "proxy-credentials", new SocksCredentials(address, canceled));
        }
    }

    /*
     * Match the former Exchange I/O boundary after TCP/proxy/TLS transport establishment. Callers
     * never notify for initial connect failures, HTTP upgrade rejection or local frame/budget errors.
     * Notify once without exposing peer-provided details or masking the original transport failure.
     */
    void failed() {
        if (address != null && selector != null && failureReported.compareAndSet(false, true)) {
            try {
                selector.connectFailed(
                        uri,
                        selected.address(),
                        new IOException("WebSocket proxy connection failed"));
            } catch (RuntimeException ignored) {
                // The original transport failure is already visible to the writer.
            }
        }
    }

    private static InetSocketAddress resolve(String host, int port) throws IOException {
        InetSocketAddress address = new InetSocketAddress(host, port);
        if (address.isUnresolved()) {
            throw new IOException("Could not resolve connection address");
        }
        return address;
    }

    /**
     * Adapts only the public authentication message; Netty retains ownership of protocol codecs and
     * negotiation. JDK SOCKS authentication uses ISO-8859-1 and falls back to user.name without a
     * password when the deployment's default Authenticator supplies no credentials.
     */
    private static final class SocksCredentials extends ChannelOutboundHandlerAdapter {
        private final InetSocketAddress proxy;
        private final BooleanSupplier canceled;

        private SocksCredentials(InetSocketAddress proxy, BooleanSupplier canceled) {
            this.proxy = proxy;
            this.canceled = canceled;
        }

        @Override
        public void write(ChannelHandlerContext context, Object message, ChannelPromise promise)
                throws Exception {
            if (message instanceof Socks5PasswordAuthRequest) {
                try {
                    PasswordAuthentication authentication =
                            Authenticator.requestPasswordAuthentication(
                                    proxy.getHostString(),
                                    proxy.getAddress(),
                                    proxy.getPort(),
                                    "SOCKS5",
                                    "SOCKS authentication",
                                    null);
                    String username =
                            authentication == null
                                    ? System.getProperty("user.name", "")
                                    : authentication.getUserName();
                    String password =
                            authentication == null ? "" : new String(authentication.getPassword());
                    message =
                            new DefaultSocks5PasswordAuthRequest(
                                    latin1(username), latin1(password));
                } catch (RuntimeException e) {
                    // Deployment callbacks may include arbitrary credential text in their cause.
                    // Keep that exception entirely inside the adapter, before Netty sees it.
                    throw new IOException("WebSocket proxy authentication failed");
                }
            }
            // A deployment authenticator can outlive cancellation; never send its returned
            // credential after the writer has released this connection.
            if (canceled.getAsBoolean()) {
                ReferenceCountUtil.release(message);
                promise.tryFailure(new IOException("WebSocket proxy connection canceled"));
                context.close();
                return;
            }
            context.write(message, promise);
        }

        private static String latin1(String value) {
            return new String(
                    value.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.ISO_8859_1);
        }
    }
}
