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

package org.apache.seatunnel.e2e.common.container;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Forwards service ports on an isolated loopback address to dynamically mapped container ports.
 *
 * <p>This is useful for services that advertise their own stable port to clients. Each test JVM
 * binds the advertised ports to a random address in the 127.0.0.0/8 loopback range, so concurrent
 * E2E runs can use the same service ports without competing for a fixed host port.
 */
public final class ContainerTcpProxy implements Closeable {

    private static final int MAX_BIND_ATTEMPTS = 100;
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger();

    private final String loopbackAddress;
    private final List<ServerSocket> serverSockets;
    private final Set<Socket> activeSockets =
            Collections.newSetFromMap(new ConcurrentHashMap<Socket, Boolean>());
    private final ExecutorService executor;

    private volatile boolean closed;

    private ContainerTcpProxy(String loopbackAddress, List<ServerSocket> serverSockets) {
        this.loopbackAddress = loopbackAddress;
        this.serverSockets = serverSockets;
        this.executor =
                Executors.newCachedThreadPool(
                        runnable -> {
                            Thread thread =
                                    new Thread(
                                            runnable,
                                            "container-tcp-proxy-"
                                                    + THREAD_COUNTER.incrementAndGet());
                            thread.setDaemon(true);
                            return thread;
                        });
    }

    public static ContainerTcpProxy start(List<PortMapping> portMappings) throws IOException {
        if (portMappings.isEmpty()) {
            throw new IllegalArgumentException("At least one port mapping is required");
        }

        IOException lastFailure = null;
        for (int attempt = 0; attempt < MAX_BIND_ATTEMPTS; attempt++) {
            String loopbackAddress = randomLoopbackAddress();
            List<ServerSocket> serverSockets = new ArrayList<>();
            try {
                for (PortMapping portMapping : portMappings) {
                    ServerSocket serverSocket = new ServerSocket();
                    serverSocket.setReuseAddress(false);
                    serverSocket.bind(
                            new InetSocketAddress(
                                    InetAddress.getByName(loopbackAddress),
                                    portMapping.getPublishedPort()));
                    serverSockets.add(serverSocket);
                }

                ContainerTcpProxy proxy = new ContainerTcpProxy(loopbackAddress, serverSockets);
                proxy.startAcceptors(portMappings);
                return proxy;
            } catch (IOException e) {
                lastFailure = e;
                closeServerSockets(serverSockets);
            }
        }
        throw new IOException(
                "Unable to bind service ports to an isolated loopback address", lastFailure);
    }

    public String getLoopbackAddress() {
        return loopbackAddress;
    }

    private void startAcceptors(List<PortMapping> portMappings) {
        for (int i = 0; i < portMappings.size(); i++) {
            ServerSocket serverSocket = serverSockets.get(i);
            PortMapping portMapping = portMappings.get(i);
            executor.execute(() -> acceptConnections(serverSocket, portMapping));
        }
    }

    private void acceptConnections(ServerSocket serverSocket, PortMapping portMapping) {
        while (!closed) {
            Socket clientSocket = null;
            Socket targetSocket = null;
            try {
                clientSocket = serverSocket.accept();
                targetSocket = new Socket();
                targetSocket.connect(
                        new InetSocketAddress(
                                portMapping.getTargetHost(), portMapping.getTargetPort()));
                activeSockets.add(clientSocket);
                activeSockets.add(targetSocket);
                Socket acceptedClient = clientSocket;
                Socket connectedTarget = targetSocket;
                executor.execute(() -> copy(acceptedClient, connectedTarget));
                executor.execute(() -> copy(connectedTarget, acceptedClient));
            } catch (IOException e) {
                closeSocket(clientSocket);
                closeSocket(targetSocket);
            }
        }
    }

    private void copy(Socket source, Socket target) {
        try {
            InputStream input = source.getInputStream();
            OutputStream output = target.getOutputStream();
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = input.read(buffer)) >= 0) {
                output.write(buffer, 0, bytesRead);
                output.flush();
            }
        } catch (IOException ignored) {
            // Closing either end of a proxied connection also stops the opposite copy task.
        } finally {
            closeSocket(source);
            closeSocket(target);
        }
    }

    @Override
    public void close() {
        closed = true;
        closeServerSockets(serverSockets);
        for (Socket socket : activeSockets) {
            closeSocket(socket);
        }
        activeSockets.clear();
        executor.shutdownNow();
    }

    private static String randomLoopbackAddress() {
        return String.format(
                "127.%d.%d.%d",
                ThreadLocalRandom.current().nextInt(1, 255),
                ThreadLocalRandom.current().nextInt(1, 255),
                ThreadLocalRandom.current().nextInt(1, 255));
    }

    private static void closeServerSockets(List<ServerSocket> serverSockets) {
        for (ServerSocket serverSocket : serverSockets) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
                // Best-effort cleanup after a partial bind or during shutdown.
            }
        }
    }

    private void closeSocket(Socket socket) {
        if (socket == null) {
            return;
        }
        activeSockets.remove(socket);
        try {
            socket.close();
        } catch (IOException ignored) {
            // Best-effort cleanup during connection shutdown.
        }
    }

    public static final class PortMapping {
        private final int publishedPort;
        private final String targetHost;
        private final int targetPort;

        private PortMapping(int publishedPort, String targetHost, int targetPort) {
            this.publishedPort = publishedPort;
            this.targetHost = targetHost;
            this.targetPort = targetPort;
        }

        public static PortMapping of(int publishedPort, String targetHost, int targetPort) {
            return new PortMapping(publishedPort, targetHost, targetPort);
        }

        private int getPublishedPort() {
            return publishedPort;
        }

        private String getTargetHost() {
            return targetHost;
        }

        private int getTargetPort() {
            return targetPort;
        }
    }
}
