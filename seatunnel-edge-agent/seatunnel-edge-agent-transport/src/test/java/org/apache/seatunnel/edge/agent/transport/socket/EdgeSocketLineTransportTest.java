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
import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportConfigTestHelper;
import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public class EdgeSocketLineTransportTest {

    @Test
    void authenticateSuccess() throws Exception {
        EdgeTransportConfig config = EdgeTransportConfigTestHelper.config("localhost:1", "tok");
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("ACK\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        transport.authenticate(in, out);
        Assertions.assertEquals("__AUTH__:tok\n", sw.toString().replace("\r\n", "\n"));
    }

    @Test
    void authenticateFailure() {
        EdgeTransportConfig config = EdgeTransportConfigTestHelper.config("localhost:1", "tok");
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("AUTH_FAILED\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        Assertions.assertThrows(
                EdgeSocketCollectorRejectedException.class, () -> transport.authenticate(in, out));
    }

    @Test
    void authenticateRejected() {
        EdgeTransportConfig config = EdgeTransportConfigTestHelper.config("localhost:1", "tok");
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("REJECTED\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        Assertions.assertThrows(
                EdgeSocketCollectorRejectedException.class, () -> transport.authenticate(in, out));
    }

    @Test
    void batchQueueFullThenReceived() throws Exception {
        EdgeTransportConfig config = retryLineTransportConfig(8);
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("QUEUE_FULL:500\nRECEIVED\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        transport.sendBatchUntilReceived(in, out, 5L, "payload");
        String written = sw.toString().replace("\r\n", "\n");
        Assertions.assertEquals(2, countOccurrences(written, "__BATCH__:5:payload"));
    }

    @Test
    void batchDecryptFailed() {
        EdgeTransportConfig config = retryLineTransportConfig(8);
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("DECRYPT_FAILED\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        IOException ex =
                Assertions.assertThrows(
                        IOException.class,
                        () -> transport.sendBatchUntilReceived(in, out, 1L, "payload"));
        Assertions.assertTrue(ex.getMessage().contains("DECRYPT_FAILED"));
    }

    @Test
    void batchRetriesThenReceived() throws Exception {
        EdgeTransportConfig config = retryLineTransportConfig(8);
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("RETRY\nRETRY\nRECEIVED\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        transport.sendBatchUntilReceived(in, out, 3L, "payload");
        String written = sw.toString().replace("\r\n", "\n");
        Assertions.assertEquals(3, countOccurrences(written, "__BATCH__:3:payload"));
    }

    @Test
    void batchUnexpectedResponseFailsFast() {
        EdgeTransportConfig config = retryLineTransportConfig(8);
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("PENDING\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        Assertions.assertThrows(
                IOException.class, () -> transport.sendBatchUntilReceived(in, out, 2L, "payload"));
    }

    @Test
    void batchMaxAttemptsExhausted() {
        EdgeTransportConfig config = retryLineTransportConfig(3);
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("RETRY\nRETRY\nRETRY\nRETRY\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        Assertions.assertThrows(
                IOException.class, () -> transport.sendBatchUntilReceived(in, out, 4L, "x"));
    }

    @Test
    void authenticateUnexpectedReplyFails() {
        EdgeTransportConfig config = EdgeTransportConfigTestHelper.config("localhost:1", "tok");
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("PENDING\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        Assertions.assertThrows(IOException.class, () -> transport.authenticate(in, out));
    }

    @Test
    void readLineNormalizedStripsCrAndTrims() throws Exception {
        BufferedReader r = new BufferedReader(new StringReader("  ACK\r\n"));
        Assertions.assertEquals("ACK", EdgeSocketLineTransport.readLineNormalized(r));
    }

    @Test
    void readLineNormalizedEofThrows() {
        BufferedReader r = new BufferedReader(new StringReader(""));
        Assertions.assertThrows(
                IOException.class, () -> EdgeSocketLineTransport.readLineNormalized(r));
    }

    private static int countOccurrences(String haystack, String needle) {
        int c = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) >= 0) {
            c++;
            idx += needle.length();
        }
        return c;
    }

    private static EdgeTransportConfig retryLineTransportConfig(int maxBatchSendAttempts) {
        Map<String, Object> overrides = new HashMap<>();
        overrides.put(EdgeTransportOptions.MAX_BATCH_SEND_ATTEMPTS.key(), maxBatchSendAttempts);
        overrides.put(EdgeTransportOptions.INITIAL_BACKOFF_MS.key(), 1L);
        overrides.put(EdgeTransportOptions.MAX_BACKOFF_MS.key(), 2L);
        return EdgeTransportConfigTestHelper.config("localhost:1", "tok", overrides);
    }
}
