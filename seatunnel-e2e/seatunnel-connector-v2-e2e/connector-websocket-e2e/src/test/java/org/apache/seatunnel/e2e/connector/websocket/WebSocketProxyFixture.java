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

package org.apache.seatunnel.e2e.connector.websocket;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.Testcontainers;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Routes only the named fixture origin to loopback peers. Observes real HTTP forwarding, CONNECT
 * and SOCKS5 password negotiation without changing the test JVM's selector or authenticator.
 */
final class WebSocketProxyFixture implements AutoCloseable {
    static final String ORIGIN = "sink-proxy.invalid";
    final List<String> routes = new CopyOnWriteArrayList<>();
    final List<String> users = new CopyOnWriteArrayList<>();
    final List<String> failures = new CopyOnWriteArrayList<>();
    private final ServerSocket listener = new ServerSocket();
    private final ExecutorService workers = Executors.newCachedThreadPool();
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final int plainPort;
    private final int tlsPort;

    WebSocketProxyFixture(int plainPort, int tlsPort) throws IOException {
        this.plainPort = plainPort;
        this.tlsPort = tlsPort;
        listener.bind(new InetSocketAddress(0));
        Testcontainers.exposeHostPorts(port());
        workers.execute(this::accept);
    }

    int port() {
        return listener.getLocalPort();
    }

    String url(boolean secure) {
        return (secure ? "wss://" : "ws://")
                + ORIGIN
                + ":"
                + (secure ? tlsPort : plainPort)
                + "/sink";
    }

    @Override
    public void close() throws Exception {
        listener.close();
        for (Socket socket : sockets) {
            socket.close();
        }
        workers.shutdownNow();
        Assertions.assertTrue(workers.awaitTermination(20, TimeUnit.SECONDS));
    }

    private void accept() {
        try {
            while (!listener.isClosed()) {
                Socket socket = listener.accept();
                sockets.add(socket);
                workers.execute(() -> serve(socket));
            }
        } catch (IOException e) {
            if (!listener.isClosed()) {
                failures.add("proxy accept failed");
            }
        }
    }

    /*
     * Bounds headers and routes before tunneling. It is deliberately not a general proxy server:
     * only the exact fixture hostname and its two already-started peer ports can be reached.
     */
    private void serve(Socket client) {
        try (Socket socket = client) {
            socket.setSoTimeout(30000);
            InputStream input = socket.getInputStream();
            int first = input.read();
            byte[] forward = null;
            int destination;
            if (first == 5) {
                destination = socks(socket, input);
            } else {
                String request = headers(input, first);
                String[] lines = request.split("\r\n");
                String[] line = lines[0].split(" ");
                routes.add(lines[0]);
                if (line.length != 3) {
                    throw new IOException("Invalid fixture proxy request");
                }
                if ("CONNECT".equals(line[0])) {
                    destination = target(line[1], tlsPort);
                    for (String header : lines) {
                        if (header.regionMatches(true, 0, "Authorization:", 0, 14)) {
                            throw new IOException("Origin credential leaked to CONNECT");
                        }
                    }
                    socket.getOutputStream()
                            .write(
                                    "HTTP/1.1 200 Connection Established\r\n\r\n"
                                            .getBytes(StandardCharsets.US_ASCII));
                    socket.getOutputStream().flush();
                } else {
                    URI uri = URI.create(line[1]);
                    if (!"GET".equals(line[0])
                            || !"http".equals(uri.getScheme())
                            || !ORIGIN.equals(uri.getHost())
                            || uri.getPort() != plainPort) {
                        throw new IOException("Expected absolute-form HTTP proxy request");
                    }
                    destination = plainPort;
                    String originForm = "GET " + uri.getRawPath() + " " + line[2];
                    forward =
                            (originForm + request.substring(lines[0].length()))
                                    .getBytes(StandardCharsets.US_ASCII);
                }
            }
            try (Socket upstream = new Socket()) {
                sockets.add(upstream);
                upstream.connect(new InetSocketAddress("127.0.0.1", destination), 10000);
                upstream.setSoTimeout(30000);
                if (forward != null) {
                    upstream.getOutputStream().write(forward);
                    upstream.getOutputStream().flush();
                }
                workers.execute(() -> copy(socket, upstream));
                copy(upstream, socket);
                sockets.remove(upstream);
            }
        } catch (Exception e) {
            if (!listener.isClosed()) {
                failures.add("proxy route failed: " + e.getClass().getSimpleName());
            }
        } finally {
            sockets.remove(client);
        }
    }

    private int socks(Socket socket, InputStream stream) throws IOException {
        DataInputStream input = new DataInputStream(stream);
        int count = input.readUnsignedByte();
        boolean noAuth = false;
        boolean password = false;
        for (int i = 0; i < count; i++) {
            int method = input.readUnsignedByte();
            noAuth |= method == 0;
            password |= method == 2;
        }
        if (!noAuth || !password) {
            throw new IOException("Expected both JDK SOCKS5 authentication methods");
        }
        OutputStream output = socket.getOutputStream();
        output.write(new byte[] {5, 2});
        output.flush();
        if (input.readUnsignedByte() != 1) {
            throw new IOException("Expected SOCKS password response");
        }
        String username = field(input);
        String secret = field(input);
        users.add(username);
        if (!secret.isEmpty()) {
            throw new IOException("Expected user.name fallback without a password");
        }
        output.write(new byte[] {1, 0});
        output.flush();
        if (input.readUnsignedByte() != 5
                || input.readUnsignedByte() != 1
                || input.readUnsignedByte() != 0
                || input.readUnsignedByte() != 3) {
            throw new IOException("Expected unresolved SOCKS5 CONNECT domain");
        }
        String host = field(input);
        int port = input.readUnsignedShort();
        if (!ORIGIN.equals(host) || port != plainPort) {
            throw new IOException("Unexpected SOCKS5 destination");
        }
        routes.add("SOCKS5 " + host + ":" + port);
        output.write(new byte[] {5, 0, 0, 1, 127, 0, 0, 1, 0, 0});
        output.flush();
        return port;
    }

    private static String field(DataInputStream input) throws IOException {
        byte[] value = new byte[input.readUnsignedByte()];
        input.readFully(value);
        return new String(value, StandardCharsets.ISO_8859_1);
    }

    private static int target(String authority, int port) throws IOException {
        if (!(ORIGIN + ":" + port).equals(authority)) {
            throw new IOException("Unexpected CONNECT destination");
        }
        return port;
    }

    private static String headers(InputStream input, int first) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int end = 0;
        int next = first;
        while (next >= 0 && bytes.size() < 16384) {
            bytes.write(next);
            end = (end << 8) | next;
            if (end == 0x0d0a0d0a) {
                return new String(bytes.toByteArray(), StandardCharsets.US_ASCII);
            }
            next = input.read();
        }
        throw new IOException("Incomplete proxy headers");
    }

    private void copy(Socket from, Socket to) {
        try {
            byte[] bytes = new byte[8192];
            int count;
            while ((count = from.getInputStream().read(bytes)) != -1) {
                to.getOutputStream().write(bytes, 0, count);
                to.getOutputStream().flush();
            }
            to.shutdownOutput();
        } catch (IOException ignored) {
            // Closing either end terminates this bounded-lifetime fixture tunnel.
        }
    }
}
