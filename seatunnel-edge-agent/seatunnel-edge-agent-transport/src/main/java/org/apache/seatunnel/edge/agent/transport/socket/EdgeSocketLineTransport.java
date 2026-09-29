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

package org.apache.seatunnel.edge.agent.transport.socket;

import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportConfig;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

final class EdgeSocketLineTransport {

    private final EdgeTransportConfig config;

    EdgeSocketLineTransport(EdgeTransportConfig config) {
        this.config = config;
    }

    void authenticate(BufferedReader reader, BufferedWriter writer) throws IOException {
        writeLine(writer, EdgeSocketProtocol.AUTH_LINE_PREFIX + config.getToken());
        handleAuthResponse(readLineNormalized(reader));
    }

    void sendBatchUntilReceived(
            BufferedReader reader, BufferedWriter writer, long batchId, String payload)
            throws IOException, InterruptedException {
        if (batchId <= 0) {
            throw new IllegalArgumentException("batchId must be positive");
        }
        String line = EdgeSocketProtocol.BATCH_PREFIX + batchId + ':' + payload;
        int attempts = 0;
        while (attempts < config.getMaxBatchSendAttempts()) {
            writeLine(writer, line);
            String reply = readLineNormalized(reader);
            if (EdgeSocketProtocol.RESP_RECEIVED.equals(reply)) {
                return;
            }
            if (EdgeSocketProtocol.RESP_RETRY.equals(reply)) {
                attempts++;
                EdgeTransportConfig.sleepQuiet(
                        EdgeTransportConfig.computeBackoffMillis(
                                attempts - 1,
                                config.getInitialBackoffMs(),
                                config.getMaxBackoffMs()));
                continue;
            }
            if (reply.startsWith(EdgeSocketProtocol.RESP_QUEUE_FULL_PREFIX)) {
                long waitMs = parseQueueFullBackoffMs(reply);
                EdgeTransportConfig.sleepQuiet(waitMs);
                continue;
            }
            if (EdgeSocketProtocol.RESP_DECRYPT_FAILED.equals(reply)) {
                throw new IOException(
                        "Edge socket ingress decryption failed (DECRYPT_FAILED): verify "
                                + "output.aes-secret-key-base64 matches EdgeSocket source "
                                + "secret_key");
            }
            throw new IOException(
                    "Unexpected batch response: "
                            + reply
                            + " (expected "
                            + EdgeSocketProtocol.RESP_RECEIVED
                            + ", "
                            + EdgeSocketProtocol.RESP_RETRY
                            + ", or "
                            + EdgeSocketProtocol.RESP_QUEUE_FULL_PREFIX
                            + "<ms>)");
        }
        throw new IOException(
                "Exceeded maxBatchSendAttempts="
                        + config.getMaxBatchSendAttempts()
                        + " without RECEIVED for batch "
                        + batchId);
    }

    private static void handleAuthResponse(String reply) throws IOException {
        if (EdgeSocketProtocol.RESP_REJECTED.equals(reply)) {
            throw new EdgeSocketCollectorRejectedException();
        }
        if (EdgeSocketProtocol.RESP_AUTH_FAILED.equals(reply)) {
            throw new EdgeSocketCollectorRejectedException(
                    "Edge socket authentication rejected (AUTH_FAILED): check output token matches"
                            + " EdgeSocket source secret_key");
        }
        if (!EdgeSocketProtocol.RESP_ACK.equals(reply)) {
            throw new IOException(
                    "Unexpected auth response: "
                            + reply
                            + " (expected "
                            + EdgeSocketProtocol.RESP_ACK
                            + " or "
                            + EdgeSocketProtocol.RESP_REJECTED
                            + ")");
        }
    }

    private static long parseQueueFullBackoffMs(String reply) {
        String suffix = reply.substring(EdgeSocketProtocol.RESP_QUEUE_FULL_PREFIX.length());
        try {
            long ms = Long.parseLong(suffix.trim());
            return ms > 0 ? ms : EdgeSocketProtocol.DEFAULT_QUEUE_FULL_BACKOFF_MS;
        } catch (NumberFormatException ex) {
            return EdgeSocketProtocol.DEFAULT_QUEUE_FULL_BACKOFF_MS;
        }
    }

    static String readLineNormalized(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if (line == null) {
            throw new IOException("EOF from edge socket ingress");
        }
        return stripTailCarriageReturn(line).trim();
    }

    static void writeLine(BufferedWriter writer, String value) throws IOException {
        writer.write(value);
        writer.newLine();
        writer.flush();
    }

    private static String stripTailCarriageReturn(String value) {
        if (value.endsWith("\r")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }
}
