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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakeException;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.proxy.ProxyConnectionEvent;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.timeout.WriteTimeoutHandler;
import io.netty.resolver.NoopAddressResolverGroup;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultThreadFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

import java.io.IOException;
import java.net.ProxySelector;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * Owns one writer's connection, bounded asynchronous sends and streaming bounded receive pipeline.
 * Incoming application data is discarded frame by frame; it never acknowledges exported rows.
 */
final class WebSocketSinkClient {
    private static final int NORMAL_CLOSE = 1000;
    private static final int MAX_PENDING_MESSAGES = 512;
    private static final int MAX_HANDSHAKE_BYTES = 65536;
    private static final long CHECK_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    private final WebSocketSinkConfig config;
    // Capture the deployment selector when the client is constructed, as OkHttpClient did.
    private final ProxySelector proxySelector;
    private final Object changed = new Object();
    private final AtomicReference<String> failure = new AtomicReference<>();
    private final AtomicBoolean released = new AtomicBoolean();
    // Reservations include in-flight socket writes, not just tasks waiting in the event-loop queue.
    private final AtomicLong pendingBytes = new AtomicLong();
    private final AtomicInteger pendingMessages = new AtomicInteger();

    // Writer methods are serialized by WebSocketSinkWriter. Event-loop callbacks publish these
    // flags.
    private volatile WebSocketSinkProxy proxyRoute;
    private volatile Channel socket;
    private volatile EventLoopGroup worker;
    // Event-loop only: HTTP exchange failures count only after TCP/proxy/TLS establishment.
    private boolean transportEstablished;
    private volatile boolean opened;
    private volatile boolean closing;
    private volatile boolean closed;

    WebSocketSinkClient(WebSocketSinkConfig config) {
        this.config = config;
        this.proxySelector = ProxySelector.getDefault();
    }

    /*
     * Reserves bounded space before enqueue. Completion releases space only after the local write.
     */
    void send(byte[] payload, boolean binary) throws IOException {
        Channel connection;
        try {
            connection = open();
        } catch (SSLException e) {
            throw fail("TLS handshake failed");
        } catch (RuntimeException e) {
            throw fail("could not start opening handshake");
        }
        long queueLimit = config.queueBytes - (long) payload.length;
        await(
                () ->
                        pendingBytes.get() <= queueLimit
                                && pendingMessages.get() < MAX_PENDING_MESSAGES,
                config.writeTimeout,
                "outbound queue admission timed out");
        WebSocketFrame frame =
                binary
                        ? new BinaryWebSocketFrame(Unpooled.copiedBuffer(payload))
                        : new TextWebSocketFrame(Unpooled.copiedBuffer(payload));
        PendingWrite reservation = new PendingWrite(payload.length);
        ChannelFuture sent = null;
        try {
            sent = connection.writeAndFlush(frame);
            sent.addListener(completed -> reservation.complete(completed.isSuccess()));
        } catch (RuntimeException e) {
            if (sent == null) {
                ReferenceCountUtil.release(frame);
            }
            reservation.complete(false);
            throw fail("could not enqueue the message");
        }
        checkFailure();
    }

    /*
     * Waits for local writes only. A peer response is never a delivery acknowledgement.
     */
    void drain() throws IOException {
        checkFailure();
        if (socket != null) {
            await(
                    () -> pendingMessages.get() == 0,
                    config.flushTimeout,
                    "outbound queue drain timed out");
        }
    }

    /*
     * Drains first, then requires the peer's normal close response before releasing this client.
     */
    void close() throws IOException {
        try {
            drain();
            Channel connection = socket;
            if (connection != null) {
                closing = true;
                connection
                        .writeAndFlush(new CloseWebSocketFrame(NORMAL_CLOSE, ""))
                        .addListener(
                                completed -> {
                                    if (!completed.isSuccess()) {
                                        fail("could not start the normal close handshake");
                                    }
                                });
                await(() -> closed, config.closeTimeout, "close handshake timed out");
            }
        } finally {
            release();
        }
    }

    /*
     * Can run concurrently with a writer wait and therefore never acquires the writer monitor.
     */
    void abort(String message) {
        failure.compareAndSet(null, message);
        signalChange();
        release();
    }

    void checkFailure() throws IOException {
        String current = failure.get();
        if (current != null) {
            throw new IOException(current);
        }
        if (Thread.currentThread().isInterrupted()) {
            throw fail("operation interrupted");
        }
    }

    /*
     * Creates the only connection lazily. All handshake work and cancellation share a finite budget.
     */
    private Channel open() throws IOException {
        checkFailure();
        if (worker == null) {
            EventLoopGroup group =
                    new NioEventLoopGroup(1, new DefaultThreadFactory("websocket-sink", true));
            worker = group;
            group.execute(
                    () -> {
                        try {
                            if (!released.get()) {
                                startConnection(group);
                            }
                        } catch (Exception e) {
                            fail(
                                    e instanceof SSLException
                                            ? "TLS handshake failed"
                                            : "opening handshake failed");
                        }
                    });
            if (released.get()) {
                group.shutdownGracefully(0, config.closeTimeout, TimeUnit.MILLISECONDS);
            }
        }
        await(() -> opened, config.connectTimeout, "opening handshake timed out");
        return socket;
    }

    /*
     * Selector/DNS/authenticator callbacks are deployment code and can block. Running setup on the
     * owned event loop keeps the writer's wait/cancellation finite; a canceled setup never connects
     * after returning from such a callback. No executor or selector is changed process-wide.
     */
    private void startConnection(EventLoopGroup group) throws IOException {
        WebSocketSinkProxy route = WebSocketSinkProxy.select(config.endpoint, proxySelector);
        proxyRoute = route;
        final SslContext tls =
                "wss".equalsIgnoreCase(config.endpoint.getScheme())
                        ? SslContextBuilder.forClient().sslProvider(SslProvider.JDK).build()
                        : null;
        HttpHeaders headers = new DefaultHttpHeaders();
        if (config.authorization() != null) {
            headers.set("Authorization", config.authorization());
        }
        WebSocketClientProtocolConfig protocol =
                WebSocketClientProtocolConfig.newBuilder()
                        .webSocketUri(route.uri)
                        .absoluteUpgradeUrl(route.forwardHttp)
                        .customHeaders(headers)
                        .generateOriginHeader(false)
                        .allowExtensions(false)
                        .maxFramePayloadLength(config.inboundBytes)
                        .handleCloseFrames(false)
                        .sendCloseFrame(null)
                        .handshakeTimeoutMillis(config.connectTimeout)
                        .forceCloseTimeoutMillis(config.closeTimeout)
                        .build();
        Bootstrap bootstrap =
                new Bootstrap()
                        .group(group)
                        .channel(NioSocketChannel.class)
                        .resolver(NoopAddressResolverGroup.INSTANCE)
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeout)
                        .option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)
                        .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(8192))
                        .option(
                                ChannelOption.WRITE_BUFFER_WATER_MARK,
                                new WriteBufferWaterMark(config.queueBytes / 2, config.queueBytes))
                        .handler(
                                new ChannelInitializer<SocketChannel>() {
                                    @Override
                                    protected void initChannel(SocketChannel channel) {
                                        route.install(
                                                channel.pipeline(),
                                                config.connectTimeout,
                                                released::get);
                                        if (tls != null) {
                                            SslHandler ssl =
                                                    tls.newHandler(
                                                            channel.alloc(),
                                                            route.destination.getHostString(),
                                                            route.destination.getPort());
                                            SSLParameters parameters =
                                                    ssl.engine().getSSLParameters();
                                            parameters.setEndpointIdentificationAlgorithm("HTTPS");
                                            ssl.engine().setSSLParameters(parameters);
                                            ssl.setHandshakeTimeoutMillis(config.connectTimeout);
                                            channel.pipeline().addLast("tls", ssl);
                                        }
                                        channel.pipeline()
                                                .addLast(
                                                        "http",
                                                        new HttpClientCodec(4096, 8192, 8192));
                                        channel.pipeline()
                                                .addLast(
                                                        "handshake-body",
                                                        new HttpObjectAggregator(
                                                                MAX_HANDSHAKE_BYTES));
                                        channel.pipeline()
                                                .addLast("inbound-budget", new InboundBudget());
                                        channel.pipeline()
                                                .addLast(
                                                        "protocol",
                                                        new WebSocketClientProtocolHandler(
                                                                protocol));
                                        channel.pipeline()
                                                .addLast(
                                                        "write-deadline",
                                                        new WriteTimeoutHandler(
                                                                config.writeTimeout,
                                                                TimeUnit.MILLISECONDS));
                                        channel.pipeline().addLast("listener", new Listener());
                                    }
                                });
        if (released.get()) {
            return;
        }
        ChannelFuture connect = bootstrap.connect(route.destination);
        socket = connect.channel();
        connect.addListener(
                completed -> {
                    if (!completed.isSuccess()) {
                        fail("opening handshake failed");
                    } else if (tls == null && !route.tunnel) {
                        transportEstablished = true;
                    }
                });
        if (released.get()) {
            socket.close();
            group.shutdownGracefully(0, config.closeTimeout, TimeUnit.MILLISECONDS);
        }
    }

    /*
     * Releases only this writer's channel and single event loop; no global client pool is affected.
     */
    private void release() {
        if (released.compareAndSet(false, true)) {
            Channel connection = socket;
            if (connection != null) {
                connection.close();
            }
            EventLoopGroup group = worker;
            if (group != null) {
                group.shutdownGracefully(0, config.closeTimeout, TimeUnit.MILLISECONDS);
            }
        }
    }

    /*
     * Polls completion under a monotonic deadline without exposing Netty or peer exception details.
     */
    private void await(BooleanSupplier done, int timeout, String timeoutMessage)
            throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
        while (true) {
            checkFailure();
            if (done.getAsBoolean()) {
                checkFailure();
                return;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw fail(timeoutMessage);
            }
            synchronized (changed) {
                try {
                    TimeUnit.NANOSECONDS.timedWait(
                            changed, Math.min(remaining, CHECK_INTERVAL_NANOS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw fail("operation interrupted");
                }
            }
        }
    }

    private IOException fail(String message) {
        failure.compareAndSet(null, "WebSocket sink: " + message);
        signalChange();
        return new IOException(failure.get());
    }

    private void signalChange() {
        synchronized (changed) {
            changed.notifyAll();
        }
    }

    /**
     * Reserves before enqueue and releases counters exactly once, even if a failed enqueue and its
     * completion callback race. The writer monitor serializes application-message reservations.
     */
    private final class PendingWrite {
        private final int bytes;
        private final AtomicBoolean completed = new AtomicBoolean();

        private PendingWrite(int bytes) {
            this.bytes = bytes;
            pendingBytes.addAndGet(bytes);
            pendingMessages.incrementAndGet();
        }

        private void complete(boolean success) {
            if (completed.compareAndSet(false, true)) {
                pendingBytes.addAndGet(-bytes);
                pendingMessages.decrementAndGet();
                if (!success) {
                    fail("connection failed during transmission");
                }
                signalChange();
            }
        }
    }

    /**
     * Counts fragments without retaining their content. The decoder rejects oversized lengths and
     * extension bits before allocating a frame; this guard bounds the complete fragmented message.
     */
    private final class InboundBudget extends ChannelDuplexHandler {
        private int messageBytes;

        /*
         * The old HTTP Exchange reported request/response I/O failures, not every socket or WS
         * error. Observe the actual HTTP write while retaining the sanitized terminal listener.
         */
        @Override
        public void write(ChannelHandlerContext context, Object message, ChannelPromise promise)
                throws Exception {
            if (message instanceof HttpRequest) {
                promise.addListener(
                        completed -> {
                            if (!completed.isSuccess()
                                    && completed.cause() instanceof IOException
                                    && transportEstablished
                                    && !opened
                                    && !released.get()
                                    && failure.get() == null) {
                                proxyRoute.failed();
                            }
                        });
            }
            context.write(message, promise);
        }

        @Override
        public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
            if (message instanceof WebSocketFrame) {
                WebSocketFrame frame = (WebSocketFrame) message;
                // Only Ping causes the protocol handler to enqueue an automatic Pong. Application
                // frames are discarded and must remain harmless while outgoing data is backed up.
                if (frame instanceof PingWebSocketFrame
                        && opened
                        && !context.channel().isWritable()) {
                    ReferenceCountUtil.release(message);
                    throw fail("outbound control response budget exhausted");
                }
                if (frame instanceof TextWebSocketFrame
                        || frame instanceof BinaryWebSocketFrame
                        || frame instanceof ContinuationWebSocketFrame) {
                    int bytes = frame.content().readableBytes();
                    if (bytes > config.inboundBytes - messageBytes) {
                        ReferenceCountUtil.release(message);
                        throw fail("incoming message exceeds max_inbound_message_bytes");
                    }
                    messageBytes = frame.isFinalFragment() ? 0 : messageBytes + bytes;
                }
            }
            context.fireChannelRead(message);
        }
    }

    /**
     * Publishes sanitized terminal state from the event loop. Application frames are released by
     * SimpleChannelInboundHandler after dispatch and never retained or aggregated here.
     */
    private final class Listener extends SimpleChannelInboundHandler<WebSocketFrame> {
        @Override
        public void userEventTriggered(ChannelHandlerContext context, Object event)
                throws Exception {
            if (event
                    == WebSocketClientProtocolHandler.ClientHandshakeStateEvent
                            .HANDSHAKE_COMPLETE) {
                opened = true;
                signalChange();
            } else if (event
                    == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_TIMEOUT) {
                fail("opening handshake timed out");
                context.close();
            }
            if (event instanceof SslHandshakeCompletionEvent) {
                if (((SslHandshakeCompletionEvent) event).isSuccess()) {
                    transportEstablished = true;
                } else {
                    fail("TLS handshake failed");
                }
            } else if (event instanceof ProxyConnectionEvent
                    && !"wss".equalsIgnoreCase(config.endpoint.getScheme())) {
                transportEstablished = true;
            }
            context.fireUserEventTriggered(event);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext context, WebSocketFrame frame) {
            if (frame instanceof CloseWebSocketFrame) {
                int code = ((CloseWebSocketFrame) frame).statusCode();
                if (closing && code == NORMAL_CLOSE) {
                    closed = true;
                    signalChange();
                } else {
                    fail("peer closed the connection unexpectedly (code " + code + ")");
                }
                context.close();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext context) throws Exception {
            if (!closed && !released.get()) {
                // A decoder can close while reporting a protocol violation. Preserve its specific,
                // sanitized exception before supplying the generic disconnect fallback next turn.
                try {
                    context.executor()
                            .execute(
                                    () -> {
                                        if (!closed && !released.get()) {
                                            fail(
                                                    opened
                                                            ? "connection failed during transmission"
                                                            : "opening handshake failed");
                                        }
                                    });
                } catch (RejectedExecutionException ignored) {
                    if (!released.get()) {
                        fail("connection failed during transmission");
                    }
                }
            }
            context.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
            String message =
                    opened ? "connection failed during transmission" : "opening handshake failed";
            boolean exchangeIoFailure = false;
            boolean protocolFailure = false;
            for (Throwable current = cause; current != null; current = current.getCause()) {
                exchangeIoFailure |= current instanceof IOException;
                if (current instanceof SSLException) {
                    message = "TLS handshake failed";
                    break;
                }
                if (current instanceof WebSocketClientHandshakeException) {
                    protocolFailure = true;
                    WebSocketClientHandshakeException handshake =
                            (WebSocketClientHandshakeException) current;
                    if (handshake.response() != null) {
                        message =
                                "opening handshake rejected (HTTP "
                                        + handshake.response().status().code()
                                        + ")";
                    }
                } else if (current instanceof CorruptedWebSocketFrameException) {
                    protocolFailure = true;
                    CorruptedWebSocketFrameException frame =
                            (CorruptedWebSocketFrameException) current;
                    message =
                            frame.closeStatus().code() == 1009
                                    ? "incoming message exceeds max_inbound_message_bytes"
                                    : "unsupported or malformed incoming WebSocket frame";
                }
            }
            // A locally raised budget error has already set failure. A rejected upgrade is a
            // completed HTTP response; neither proves that the selected proxy failed.
            boolean notifyProxy =
                    transportEstablished
                            && !opened
                            && !released.get()
                            && failure.get() == null
                            && exchangeIoFailure
                            && !protocolFailure;
            fail(message);
            context.close();
            if (notifyProxy) {
                proxyRoute.failed();
            }
        }
    }
}
