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

package org.apache.seatunnel.connectors.seatunnel.socket.sink;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.socket.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.socket.exception.SocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.socket.exception.SocketConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

@Slf4j
public class SocketClient {

    private final String hostName;
    private final int port;
    private int retries;
    private final int maxNumRetries;
    private transient Socket client;
    private transient OutputStream outputStream;
    private final SerializationSchema serializationSchema;
    private volatile boolean isRunning = Boolean.TRUE;
    private static final int CONNECTION_RETRY_DELAY = 500;

    public SocketClient(SinkConfig config, SerializationSchema serializationSchema) {
        this.hostName = config.getHost();
        this.port = config.getPort();
        this.serializationSchema = serializationSchema;
        retries = config.getMaxNumRetries();
        maxNumRetries = config.getMaxNumRetries();
    }

    private void createConnection() throws IOException {
        client = new Socket(hostName, port);
        client.setKeepAlive(true);
        client.setTcpNoDelay(true);

        outputStream = client.getOutputStream();
    }

    public void open() throws IOException {
        try {
            synchronized (SocketClient.class) {
                createConnection();
            }
        } catch (IOException e) {
            throw new SocketConnectorException(SocketConnectorErrorCode.SOCKET_SERVER_CONNECT_FAILED,
                String.format("Cannot connect to socket server at %s:%d",
                hostName, port), e);
        }
    }

    public void write(SeaTunnelRow row) throws IOException {
        byte[] msg = serializationSchema.serialize(row);
        try {
            outputStream.write(msg);
            outputStream.flush();
        } catch (IOException e) {
            // if no re-tries are enable, fail immediately
            if (maxNumRetries == 0) {
                throw new SocketConnectorException(SocketConnectorErrorCode.SEND_MESSAGE_TO_SOCKET_SERVER_FAILED,
                    String.format("Failed to send message '%s' to socket server at %s:%d. Connection re-tries are not enabled.",
                    row, hostName, port), e);
            }

            log.error(
                "Failed to send message '{}' to socket server at {}:{}. Trying to reconnect...",
                row, hostName, port, e);

            synchronized (SocketClient.class) {
                IOException lastException = null;
                retries = 0;
                while (isRunning && (maxNumRetries < 0 || retries < maxNumRetries)) {
                    // first, clean up the old resources
                    try {
                        if (outputStream != null) {
                            outputStream.close();
                        }
                    } catch (IOException ee) {
                        log.error("Could not close output stream from failed write attempt", ee);
                    }
                    try {
                        if (client != null) {
                            client.close();
                        }
                    } catch (IOException ee) {
                        log.error("Could not close socket from failed write attempt", ee);
                    }

                    // try again
                    retries++;

                    try {
                        // initialize a new connection
                        createConnection();
                        outputStream.write(msg);
                        return;
                    } catch (IOException ee) {
                        lastException = ee;
                        log.error("Re-connect to socket server and send message failed. Retry time(s): {}",
                            retries, ee);
                    }
                    try {
                        this.wait(CONNECTION_RETRY_DELAY);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new SocketConnectorException(SocketConnectorErrorCode.SOCKET_WRITE_FAILED,
                            "unable to write; interrupted while doing another attempt", e);
                    }
                }

                if (isRunning) {
                    throw new SocketConnectorException(SocketConnectorErrorCode.SEND_MESSAGE_TO_SOCKET_SERVER_FAILED,
                        String.format("Failed to send message '%s' to socket server at %s:%d. Failed after %d retries.",
                        row, hostName, port, retries), lastException);
                }
            }
        }
    }

    public void close() throws IOException {
        isRunning = false;
        synchronized (this) {
            this.notifyAll();
            try {
                if (outputStream != null) {
                    outputStream.close();
                }
            } finally {
                if (client != null) {
                    client.close();
                }
            }
        }
    }
}
