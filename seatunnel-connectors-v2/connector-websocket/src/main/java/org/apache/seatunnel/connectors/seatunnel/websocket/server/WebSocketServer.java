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

package org.apache.seatunnel.connectors.seatunnel.websocket.server;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Socket owner. Network workers only enqueue complete messages, never emit engine rows. */
final class WebSocketServer implements AutoCloseable {
    private final WebSocketServerSourceOptions.Settings options;
    private final MessageQueue queue;
    private final Set<Client> clients = ConcurrentHashMap.newKeySet();
    private final ThreadPoolExecutor connections;
    private final ScheduledExecutorService deadlines;
    private final ServerSocket listener;
    private volatile boolean closed;
    private volatile boolean listenerFailed;

    WebSocketServer(WebSocketServerSourceOptions.Settings options) throws IOException {
        this.options = options;
        queue = new MessageQueue(options.queueMessages, options.queueBytes);
        listener = bind(options);
        connections =
                new ThreadPoolExecutor(
                        options.connections,
                        options.connections,
                        0,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(options.connections),
                        task -> daemon(task, "websocket-server-client"));
        deadlines =
                Executors.newSingleThreadScheduledExecutor(
                        task -> daemon(task, "websocket-server-deadline"));
        deadlines.scheduleWithFixedDelay(this::expireClients, 25, 25, TimeUnit.MILLISECONDS);
        daemon(this::accept, "websocket-server-accept").start();
    }

    private static ServerSocket bind(WebSocketServerSourceOptions.Settings options)
            throws IOException {
        ServerSocket socket = null;
        try {
            if (options.keystorePath == null) {
                socket = new ServerSocket();
            } else {
                SSLServerSocket tls =
                        (SSLServerSocket)
                                tlsContext(options).getServerSocketFactory().createServerSocket();
                socket = tls;
                String[] enabled =
                        Arrays.stream(tls.getSupportedProtocols())
                                .filter(
                                        protocol ->
                                                "TLSv1.2".equals(protocol)
                                                        || "TLSv1.3".equals(protocol))
                                .toArray(String[]::new);
                tls.setEnabledProtocols(enabled);
            }
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(options.bindHost, options.port), options.connections);
            return socket;
        } catch (IOException | GeneralSecurityException | RuntimeException ex) {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // No listener remains available.
                }
            }
            throw new IOException("WebSocketServer could not initialize the configured listener");
        }
    }

    private static SSLContext tlsContext(WebSocketServerSourceOptions.Settings options)
            throws GeneralSecurityException, IOException {
        char[] secret = options.password.toCharArray();
        try {
            KeyStore keys = KeyStore.getInstance(options.keystoreType);
            try (InputStream file = Files.newInputStream(Paths.get(options.keystorePath))) {
                keys.load(file, secret);
            }
            KeyManagerFactory keyManagers =
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagers.init(keys, secret);
            SSLContext tls = SSLContext.getInstance("TLS");
            tls.init(keyManagers.getKeyManagers(), null, null);
            return tls;
        } finally {
            Arrays.fill(secret, '\0');
        }
    }

    private static Thread daemon(Runnable task, String name) {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        return thread;
    }

    private void accept() {
        try {
            while (!closed) {
                Socket peer = listener.accept();
                if (closed || clients.size() >= options.connections) {
                    closeSocket(peer);
                    continue;
                }
                Client client = new Client(peer);
                clients.add(client);
                try {
                    connections.execute(client);
                } catch (RejectedExecutionException ex) {
                    clients.remove(client);
                    client.abort();
                }
            }
        } catch (IOException ex) {
            if (!closed) {
                listenerFailed = true;
            }
        }
    }

    private void expireClients() {
        long now = System.nanoTime();
        for (Client client : clients) {
            if (client.deadline - now <= 0) {
                client.abort();
            }
        }
    }

    Message poll() throws IOException {
        if (listenerFailed) {
            throw new IOException("WebSocketServer listener stopped accepting clients");
        }
        return queue.poll();
    }

    /** Close the listener and every peer regardless of their handshake, read or write state. */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            listener.close();
        } catch (IOException ignored) {
            // A failed listener is already unavailable.
        }
        connections.shutdownNow();
        deadlines.shutdownNow();
        clients.forEach(Client::abort);
        queue.close();
    }

    private static void closeSocket(Socket peer) {
        try {
            peer.close();
        } catch (IOException ignored) {
            // A peer error is contained to the connection.
        }
    }

    private final class Client implements Runnable {
        private final Socket peer;
        private volatile long deadline;
        private ByteArrayOutputStream fragments;
        private int fragmentedType;
        private int fragmentCount;

        private Client(Socket peer) {
            this.peer = peer;
            deadline = after(options.handshakeTimeout);
        }

        @Override
        public void run() {
            try {
                if (peer instanceof SSLSocket) {
                    ((SSLSocket) peer).startHandshake();
                }
                InputStream in = new BufferedInputStream(peer.getInputStream());
                OutputStream out = new BufferedOutputStream(peer.getOutputStream());
                try {
                    WebSocketServerProtocol.upgrade(in, out, options);
                } catch (WebSocketServerProtocol.UpgradeFailure ex) {
                    WebSocketServerProtocol.reject(out, ex.status);
                    return;
                }
                frames(in, out);
            } catch (IOException ignored) {
                // Do not log peer-provided headers, close reasons, TLS details or payloads.
            } finally {
                abort();
                clients.remove(this);
            }
        }

        private void frames(InputStream in, OutputStream out) throws IOException {
            deadline = after(options.idleTimeout);
            try {
                while (!closed) {
                    int first = in.read();
                    if (first < 0) {
                        return;
                    }
                    if (deadline - System.nanoTime() <= 0) {
                        return;
                    }
                    if (fragments == null) {
                        deadline = after(options.messageTimeout);
                    }
                    int remaining =
                            options.messageBytes - (fragments == null ? 0 : fragments.size());
                    WebSocketServerProtocol.Frame frame =
                            WebSocketServerProtocol.readFrame(
                                    in, first, remaining, options.messageBytes);
                    if (deadline - System.nanoTime() <= 0) {
                        return;
                    }
                    if (frame.opcode == 8) {
                        WebSocketServerProtocol.checkClosePayload(frame.payload);
                        WebSocketServerProtocol.control(out, 8, frame.payload);
                        return;
                    }
                    if (frame.opcode == 9) {
                        WebSocketServerProtocol.control(out, 10, frame.payload);
                    } else if (frame.opcode != 10) {
                        data(frame);
                    }
                    if (fragments == null) {
                        deadline = after(options.idleTimeout);
                    }
                }
            } catch (WebSocketServerProtocol.FrameFailure ex) {
                WebSocketServerProtocol.close(out, ex.code);
            }
        }

        /** Interleaved control frames never reset a fragmented message's deadline. */
        private void data(WebSocketServerProtocol.Frame frame) throws IOException {
            if (frame.opcode == 0) {
                if (fragments == null) {
                    throw new WebSocketServerProtocol.FrameFailure(1002);
                }
                if (++fragmentCount > 1024) {
                    throw new WebSocketServerProtocol.FrameFailure(1009);
                }
                fragments.write(frame.payload);
                if (frame.fin) {
                    enqueue(fragmentedType, fragments.toByteArray());
                    fragments = null;
                }
            } else if (fragments != null) {
                throw new WebSocketServerProtocol.FrameFailure(1002);
            } else if (frame.fin) {
                enqueue(frame.opcode, frame.payload);
            } else {
                fragmentedType = frame.opcode;
                fragmentCount = 1;
                fragments = new ByteArrayOutputStream(frame.payload.length);
                fragments.write(frame.payload);
            }
        }

        private void enqueue(int opcode, byte[] bytes) throws IOException {
            if (opcode == 1 || !"raw".equals(options.format)) {
                WebSocketServerProtocol.text(bytes);
            }
            if (!queue.offer(new Message(opcode == 1, bytes))) {
                // The sender must reconnect and decide what to replay after overload.
                throw new WebSocketServerProtocol.FrameFailure(1013);
            }
        }

        private void abort() {
            closeSocket(peer);
        }
    }

    private static long after(int milliseconds) {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(milliseconds);
    }

    static final class Message {
        final boolean text;
        final byte[] data;

        Message(boolean text, byte[] data) {
            this.text = text;
            this.data = data;
        }
    }

    /** Count and byte limits are applied as one admission decision, including empty messages. */
    static final class MessageQueue {
        private final ArrayDeque<Message> messages = new ArrayDeque<>();
        private final int maxCount;
        private final int maxBytes;
        private boolean closed;
        private int bytes;

        MessageQueue(int maxCount, int maxBytes) {
            this.maxCount = maxCount;
            this.maxBytes = maxBytes;
        }

        synchronized boolean offer(Message message) {
            if (closed || messages.size() >= maxCount || message.data.length > maxBytes - bytes) {
                return false;
            }
            messages.addLast(message);
            bytes += message.data.length;
            return true;
        }

        synchronized Message poll() {
            Message message = messages.pollFirst();
            if (message != null) {
                bytes -= message.data.length;
            }
            return message;
        }

        synchronized void close() {
            closed = true;
            bytes = 0;
            messages.clear();
        }
    }
}
