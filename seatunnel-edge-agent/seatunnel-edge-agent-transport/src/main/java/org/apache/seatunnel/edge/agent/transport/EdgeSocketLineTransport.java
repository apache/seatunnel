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

package org.apache.seatunnel.edge.agent.transport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

/** Line-level EdgeSocket ingress dialog (collector side). */
final class EdgeSocketLineTransport {

    private final EdgeTransportConfig config;

    EdgeSocketLineTransport(EdgeTransportConfig config) {
        this.config = config;
    }

    void authenticate(BufferedReader reader, BufferedWriter writer)
            throws IOException, InterruptedException {
        writeLine(writer, EdgeSocketProtocol.AUTH_LINE_PREFIX + config.getAuthToken());
        String reply = readLineNormalized(reader);
        if (EdgeSocketProtocol.RESP_AUTH_FAILED.equals(reply)) {
            throw new IOException("Edge socket authentication rejected (AUTH_FAILED)");
        }
        if (!EdgeSocketProtocol.RESP_ACK.equals(reply)) {
            throw new IOException(
                    "Unexpected auth response: "
                            + reply
                            + " (expected "
                            + EdgeSocketProtocol.RESP_ACK
                            + ")");
        }
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
            attempts++;
            writeLine(writer, line);
            String reply = readLineNormalized(reader);
            if (EdgeSocketProtocol.RESP_RECEIVED.equals(reply)) {
                return;
            }
            if (EdgeSocketProtocol.RESP_RETRY.equals(reply)) {
                EdgeTransportConfig.sleepQuiet(
                        EdgeTransportConfig.computeBackoffMillis(
                                attempts - 1,
                                config.getInitialBackoffMs(),
                                config.getMaxBackoffMs()));
                continue;
            }
            throw new IOException(
                    "Unexpected batch response: "
                            + reply
                            + " (expected "
                            + EdgeSocketProtocol.RESP_RECEIVED
                            + " or "
                            + EdgeSocketProtocol.RESP_RETRY
                            + ")");
        }
        throw new IOException(
                "Exceeded maxBatchSendAttempts="
                        + config.getMaxBatchSendAttempts()
                        + " without RECEIVED for batch "
                        + batchId);
    }

    void awaitCommitAck(BufferedReader reader, BufferedWriter writer, long expectedBatchId)
            throws IOException, InterruptedException {
        String commitLine = EdgeSocketProtocol.COMMIT_PREFIX + expectedBatchId;
        int polls = 0;
        while (polls < config.getMaxCommitPollAttempts()) {
            polls++;
            writeLine(writer, commitLine);
            String reply = readLineNormalized(reader);
            if (EdgeSocketProtocol.RESP_PENDING.equals(reply)) {
                EdgeTransportConfig.sleepQuiet(config.getCommitPollSleepMs());
                continue;
            }
            if (EdgeSocketProtocol.RESP_RETRY.equals(reply)) {
                EdgeTransportConfig.sleepQuiet(config.getCommitPollSleepMs());
                continue;
            }
            if (reply != null && reply.startsWith(EdgeSocketProtocol.RESP_ACK_PREFIX)) {
                String suffix = reply.substring(EdgeSocketProtocol.RESP_ACK_PREFIX.length()).trim();
                try {
                    long acked = Long.parseLong(suffix);
                    if (acked >= expectedBatchId) {
                        return;
                    }
                } catch (NumberFormatException ex) {
                    throw new IOException("Malformed ACK line: " + reply, ex);
                }
                EdgeTransportConfig.sleepQuiet(config.getCommitPollSleepMs());
                continue;
            }
            throw new IOException(
                    "Unexpected commit response: " + reply + " for batch " + expectedBatchId);
        }
        throw new IOException(
                "Exceeded maxCommitPollAttempts="
                        + config.getMaxCommitPollAttempts()
                        + " waiting ACK for batch "
                        + expectedBatchId);
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
