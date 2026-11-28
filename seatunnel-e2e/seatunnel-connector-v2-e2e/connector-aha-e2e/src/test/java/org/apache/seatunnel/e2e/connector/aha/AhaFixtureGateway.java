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

package org.apache.seatunnel.e2e.connector.aha;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Bridges the Docker DNS alias to the test host without terminating fixture TLS. */
public final class AhaFixtureGateway {
    private AhaFixtureGateway() {}

    public static void main(String[] args) throws IOException {
        ServerSocket https = new ServerSocket(8443);
        ServerSocket untrusted = new ServerSocket(8444);
        ExecutorService connections = Executors.newCachedThreadPool();
        connections.execute(() -> forward(https, Integer.parseInt(args[0]), connections));
        forward(untrusted, Integer.parseInt(args[1]), connections);
    }

    private static void forward(ServerSocket listener, int port, ExecutorService connections) {
        try (ServerSocket server = listener) {
            while (true) {
                Socket client = server.accept();
                connections.execute(() -> bridge(client, port, connections));
            }
        } catch (IOException e) {
            throw new IllegalStateException("Fixture gateway cannot accept connections", e);
        }
    }

    private static void bridge(Socket client, int port, ExecutorService connections) {
        try (Socket source = client;
                Socket target = new Socket()) {
            target.connect(new InetSocketAddress("host.testcontainers.internal", port), 5000);
            connections.execute(() -> copy(source, target));
            copy(target, source);
        } catch (IOException ignored) {
            // Health probes and deliberately canceled jobs can close either endpoint.
        }
    }

    private static void copy(Socket source, Socket target) {
        try {
            InputStream input = source.getInputStream();
            OutputStream output = target.getOutputStream();
            byte[] buffer = new byte[8192];
            int count;
            while ((count = input.read(buffer)) != -1) {
                output.write(buffer, 0, count);
                output.flush();
            }
            target.shutdownOutput();
        } catch (IOException ignored) {
            // The bridge owns both sockets and the fixture can detect the upstream close.
        }
    }
}
