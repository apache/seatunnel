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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.protocol;

import org.apache.seatunnel.connectors.seatunnel.edgesocket.channel.IngressChannel;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketAuthType;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.util.EdgeSocketLogUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.function.BooleanSupplier;

@Slf4j
public class IngressProtocolHandler {

    private final EdgeSocketConfig config;
    private final IncomingRecordHandler handler;

    public IngressProtocolHandler(EdgeSocketConfig config, IncomingRecordHandler handler) {
        this.config = config;
        this.handler = handler;
    }

    public boolean authenticate(IngressChannel channel) throws IOException {
        if (config.getAuthType() != EdgeSocketAuthType.TOKEN) {
            channel.writeLine(EdgeSocketResponseCode.AUTH_FAILED.getCode());
            log.warn(
                    "Unsupported auth type: {}, from {}",
                    config.getAuthType(),
                    channel.remoteAddress());
            return false;
        }
        String authLine;
        try {
            authLine = channel.readLine();
        } catch (SocketTimeoutException timeoutException) {
            channel.writeLine(EdgeSocketResponseCode.AUTH_FAILED.getCode());
            log.warn(
                    "Collector authentication timeout from {}, connection rejected",
                    channel.remoteAddress());
            return false;
        }
        if (authLine == null) {
            channel.writeLine(EdgeSocketResponseCode.AUTH_FAILED.getCode());
            log.warn(
                    "Collector from {} closed connection before authentication",
                    channel.remoteAddress());
            return false;
        }
        String presentedToken = parseAuthToken(authLine);
        if (!constantTimeEquals(config.getToken(), presentedToken)) {
            channel.writeLine(EdgeSocketResponseCode.AUTH_FAILED.getCode());
            log.warn("Collector authentication failed from {}", channel.remoteAddress());
            return false;
        }
        channel.writeLine(EdgeSocketResponseCode.ACK.getCode());
        return true;
    }

    public void receiveLoop(IngressChannel channel, BooleanSupplier isActive) throws IOException {
        while (isActive.getAsBoolean()) {
            String record;
            try {
                record = channel.readLine();
            } catch (SocketTimeoutException timeoutException) {
                continue;
            }
            if (record == null) {
                return;
            }
            channel.writeLine(dispatch(record));
        }
    }

    private String dispatch(String request) {
        IngressCommand command = IngressCommand.matchPrefix(request);
        if (command == null) {
            log.warn(
                    "Unrecognized command prefix, got: {}",
                    EdgeSocketLogUtils.abbreviateForLog(request));
            return EdgeSocketResponseCode.BAD_REQUEST.getCode();
        }
        switch (command) {
            case COMMIT:
                return dispatchCommit(request);
            case BATCH:
                return dispatchBatch(request);
            default:
                log.warn("Command {} is not dispatched yet", command);
                return EdgeSocketResponseCode.BAD_REQUEST.getCode();
        }
    }

    private String dispatchCommit(String request) {
        Long batchId = parseBatchId(request, IngressCommand.COMMIT.prefix());
        if (batchId == null || batchId <= 0) {
            log.warn(
                    "Invalid COMMIT batchId in request: {}",
                    EdgeSocketLogUtils.abbreviateForLog(request));
            return EdgeSocketResponseCode.INVALID_PARAM.getCode();
        }
        return handler.handleCommitRequest(batchId);
    }

    private String dispatchBatch(String request) {
        int separatorIndex = request.indexOf(':', IngressCommand.BATCH.prefix().length());
        if (separatorIndex < 0) {
            log.warn(
                    "Malformed BATCH request, missing payload separator: {}",
                    EdgeSocketLogUtils.abbreviateForLog(request));
            return EdgeSocketResponseCode.BAD_REQUEST.getCode();
        }
        Long batchId =
                parseBatchId(request.substring(0, separatorIndex), IngressCommand.BATCH.prefix());
        if (batchId == null || batchId <= 0) {
            log.warn(
                    "Invalid BATCH batchId in request: {}",
                    EdgeSocketLogUtils.abbreviateForLog(request));
            return EdgeSocketResponseCode.INVALID_PARAM.getCode();
        }
        String payload = request.substring(separatorIndex + 1);
        return handler.handleBatchRecord(batchId, payload);
    }

    private static boolean constantTimeEquals(String expected, String actual) {
        if (expected == null || actual == null) {
            return Objects.equals(expected, actual);
        }
        byte[] a = expected.getBytes(StandardCharsets.UTF_8);
        byte[] b = actual.getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(a, b);
    }

    private static String parseAuthToken(String authLine) {
        if (IngressCommand.AUTH.matches(authLine)) {
            return IngressCommand.AUTH.stripPrefix(authLine);
        }
        return authLine;
    }

    private static Long parseBatchId(String input, String prefix) {
        if (!input.startsWith(prefix)) {
            return null;
        }
        try {
            return Long.parseLong(input.substring(prefix.length()));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }
}
