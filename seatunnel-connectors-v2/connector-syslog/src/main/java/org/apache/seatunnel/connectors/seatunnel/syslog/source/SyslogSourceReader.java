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

package org.apache.seatunnel.connectors.seatunnel.syslog.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.syslog.config.SyslogConfig;
import org.apache.seatunnel.connectors.seatunnel.syslog.exception.SyslogConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.syslog.exception.SyslogConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class SyslogSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    /**
     * RFC 3164 pattern: <PRI>TIMESTAMP HOSTNAME APP_NAME[PID]: MESSAGE
     *
     * <p>Groups: 1=PRI, 2=TIMESTAMP, 3=HOSTNAME, 4=APP_NAME, 5=PID (optional), 6=MESSAGE
     */
    private static final Pattern RFC3164_PATTERN =
            Pattern.compile(
                    "^<(\\d{1,3})>"
                            + "(\\w{3}\\s+\\d{1,2}\\s+\\d{2}:\\d{2}:\\d{2})"
                            + "\\s+(\\S+)"
                            + "\\s+([^\\s\\[:]+)(?:\\[(\\w+)\\])?:?\\s*"
                            + "(.*)$");

    /** Accept timeout in milliseconds, allowing the reader to notice a close() call. */
    private static final int ACCEPT_TIMEOUT_MS = 500;

    private static final int POLL_TIMEOUT_MS = 100;

    private final SyslogConfig config;
    private final SingleSplitReaderContext context;
    private final BlockingQueue<SeaTunnelRow> pendingRows = new LinkedBlockingQueue<>();
    private ServerSocket serverSocket;
    private ExecutorService acceptExecutor;
    private ExecutorService connectionExecutor;
    private volatile boolean running;
    private volatile RuntimeException fatalException;

    SyslogSourceReader(SyslogConfig config, SingleSplitReaderContext context) {
        this.config = config;
        this.context = context;
    }

    @Override
    public void open() throws Exception {
        InetAddress bindAddress = InetAddress.getByName(config.getHost());
        try {
            serverSocket = new ServerSocket(config.getPort(), 50, bindAddress);
            serverSocket.setSoTimeout(ACCEPT_TIMEOUT_MS);
            running = true;
            startAcceptLoop();
            log.info("Syslog source listening on {}:{}", config.getHost(), config.getPort());
        } catch (IOException e) {
            throw new SyslogConnectorException(
                    SyslogConnectorErrorCode.SERVER_BIND_FAILED,
                    "Cannot bind to " + config.getHost() + ":" + config.getPort(),
                    e);
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
        shutdownExecutor(acceptExecutor, "syslog accept");
        shutdownExecutor(connectionExecutor, "syslog connection");
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        rethrowFatalIfNeeded();

        SeaTunnelRow row = pendingRows.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (row == null) {
            return;
        }

        synchronized (output.getCheckpointLock()) {
            output.collect(row);
            while ((row = pendingRows.poll()) != null) {
                output.collect(row);
            }
        }
    }

    private void startAcceptLoop() {
        acceptExecutor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(runnable, "syslog-accept-" + config.getPort());
                            thread.setDaemon(true);
                            return thread;
                        });
        connectionExecutor =
                Executors.newCachedThreadPool(
                        runnable -> {
                            Thread thread =
                                    new Thread(runnable, "syslog-connection-" + config.getPort());
                            thread.setDaemon(true);
                            return thread;
                        });

        acceptExecutor.submit(
                () -> {
                    try {
                        acceptConnections();
                    } catch (RuntimeException e) {
                        fatalException = e;
                        throw e;
                    }
                });
    }

    private void acceptConnections() {
        while (running && serverSocket != null && !serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setSoTimeout(ACCEPT_TIMEOUT_MS);
                log.debug(
                        "Accepted syslog connection from {}",
                        clientSocket.getRemoteSocketAddress());
                connectionExecutor.submit(() -> processConnection(clientSocket));
            } catch (SocketTimeoutException e) {
                // no incoming connection within the timeout window, loop again
            } catch (IOException e) {
                if (running) {
                    throw new SyslogConnectorException(
                            SyslogConnectorErrorCode.SERVER_ACCEPT_FAILED,
                            "Failed to accept syslog connection on "
                                    + config.getHost()
                                    + ":"
                                    + config.getPort(),
                            e);
                }
            }
        }
    }

    /**
     * Reads newline-delimited syslog messages from a client connection and queues parsed rows.
     * Closes the client socket when the connection is terminated by the sender or the reader.
     */
    private void processConnection(Socket clientSocket) {
        try (Socket socket = clientSocket;
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            while (running) {
                String line;
                try {
                    line = reader.readLine();
                } catch (SocketTimeoutException e) {
                    continue;
                }
                if (line == null) {
                    break;
                }
                SeaTunnelRow row = parseRfc3164(line);
                if (row != null) {
                    pendingRows.offer(row);
                } else {
                    log.warn("Skipping malformed syslog line: {}", line);
                }
            }
        } catch (IOException e) {
            log.warn("Error reading from syslog client connection: {}", e.getMessage());
        }
    }

    private void rethrowFatalIfNeeded() {
        RuntimeException exception = fatalException;
        if (exception != null) {
            throw exception;
        }
    }

    private void shutdownExecutor(ExecutorService executor, String executorName) {
        if (executor == null) {
            return;
        }
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                log.warn("{} executor did not terminate within timeout", executorName);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Parses a single RFC 3164 syslog message into a SeaTunnelRow.
     *
     * <p>Output columns: facility (INT), severity (INT), timestamp (STRING), hostname (STRING),
     * app_name (STRING), proc_id (STRING), message (STRING).
     *
     * @param line raw syslog line
     * @return parsed row, or null if the line does not match RFC 3164 format
     */
    static SeaTunnelRow parseRfc3164(String line) {
        if (line == null || line.isEmpty()) {
            return null;
        }
        Matcher matcher = RFC3164_PATTERN.matcher(line);
        if (!matcher.matches()) {
            return null;
        }

        int pri = Integer.parseInt(matcher.group(1));
        int facility = pri >> 3;
        int severity = pri & 0x07;
        String timestamp = matcher.group(2).trim();
        String hostname = matcher.group(3);
        String appName = matcher.group(4);
        String procId = matcher.group(5) != null ? matcher.group(5) : "";
        String message = matcher.group(6);

        return new SeaTunnelRow(
                new Object[] {facility, severity, timestamp, hostname, appName, procId, message});
    }
}
