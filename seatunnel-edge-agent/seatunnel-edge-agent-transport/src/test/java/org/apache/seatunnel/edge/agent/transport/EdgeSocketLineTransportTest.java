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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

class EdgeSocketLineTransportTest {

    @Test
    void authenticateSuccess() throws Exception {
        EdgeTransportConfig config =
                EdgeTransportConfig.builder().jobId(1).authToken("tok").edgeIngressPort(1).build();
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("ACK\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        transport.authenticate(in, out);
        Assertions.assertEquals("__AUTH__:tok\n", sw.toString().replace("\r\n", "\n"));
    }

    @Test
    void authenticateFailure() {
        EdgeTransportConfig config =
                EdgeTransportConfig.builder().jobId(1).authToken("tok").edgeIngressPort(1).build();
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("AUTH_FAILED\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        Assertions.assertThrows(IOException.class, () -> transport.authenticate(in, out));
    }

    @Test
    void batchRetriesThenReceived() throws Exception {
        EdgeTransportConfig config =
                EdgeTransportConfig.builder()
                        .jobId(1)
                        .authToken("tok")
                        .edgeIngressPort(1)
                        .maxBatchSendAttempts(8)
                        .initialBackoffMs(1)
                        .maxBackoffMs(2)
                        .build();
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("RETRY\nRETRY\nRECEIVED\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        transport.sendBatchUntilReceived(in, out, 3L, "payload");
        String written = sw.toString().replace("\r\n", "\n");
        Assertions.assertEquals(3, countOccurrences(written, "__BATCH__:3:payload"));
    }

    @Test
    void commitHandlesPendingAndAck() throws Exception {
        EdgeTransportConfig config =
                EdgeTransportConfig.builder()
                        .jobId(1)
                        .authToken("tok")
                        .edgeIngressPort(1)
                        .maxCommitPollAttempts(20)
                        .commitPollSleepMs(1)
                        .build();
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("PENDING\nACK:0\nACK:5\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        transport.awaitCommitAck(in, out, 5L);
        String written = sw.toString().replace("\r\n", "\n");
        Assertions.assertTrue(written.contains("__COMMIT__:5"));
        Assertions.assertEquals(3, countOccurrences(written, "__COMMIT__:5"));
    }

    @Test
    void commitHandlesRetryThenAck() throws Exception {
        EdgeTransportConfig config =
                EdgeTransportConfig.builder()
                        .jobId(1)
                        .authToken("tok")
                        .edgeIngressPort(1)
                        .maxCommitPollAttempts(20)
                        .commitPollSleepMs(1)
                        .build();
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("RETRY\nACK:7\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        transport.awaitCommitAck(in, out, 7L);
        Assertions.assertEquals(
                2, countOccurrences(sw.toString().replace("\r\n", "\n"), "__COMMIT__:7"));
    }

    @Test
    void commitStaleAckPollsUntilExpectedReached() throws Exception {
        EdgeTransportConfig config =
                EdgeTransportConfig.builder()
                        .jobId(1)
                        .authToken("tok")
                        .edgeIngressPort(1)
                        .maxCommitPollAttempts(20)
                        .commitPollSleepMs(1)
                        .build();
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("ACK:3\nPENDING\nACK:9\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        transport.awaitCommitAck(in, out, 9L);
        Assertions.assertEquals(
                3, countOccurrences(sw.toString().replace("\r\n", "\n"), "__COMMIT__:9"));
    }

    @Test
    void commitMalformedAckFails() {
        EdgeTransportConfig config =
                EdgeTransportConfig.builder()
                        .jobId(1)
                        .authToken("tok")
                        .edgeIngressPort(1)
                        .maxCommitPollAttempts(5)
                        .commitPollSleepMs(1)
                        .build();
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("ACK:not-a-number\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        Assertions.assertThrows(IOException.class, () -> transport.awaitCommitAck(in, out, 1L));
    }

    @Test
    void batchUnexpectedResponseFailsFast() {
        EdgeTransportConfig config =
                EdgeTransportConfig.builder()
                        .jobId(1)
                        .authToken("tok")
                        .edgeIngressPort(1)
                        .maxBatchSendAttempts(8)
                        .initialBackoffMs(1)
                        .maxBackoffMs(2)
                        .build();
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("PENDING\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        Assertions.assertThrows(
                IOException.class, () -> transport.sendBatchUntilReceived(in, out, 2L, "payload"));
    }

    @Test
    void batchMaxAttemptsExhausted() {
        EdgeTransportConfig config =
                EdgeTransportConfig.builder()
                        .jobId(1)
                        .authToken("tok")
                        .edgeIngressPort(1)
                        .maxBatchSendAttempts(3)
                        .initialBackoffMs(1)
                        .maxBackoffMs(2)
                        .build();
        EdgeSocketLineTransport transport = new EdgeSocketLineTransport(config);
        BufferedReader in = new BufferedReader(new StringReader("RETRY\nRETRY\nRETRY\nRETRY\n"));
        StringWriter sw = new StringWriter();
        BufferedWriter out = new BufferedWriter(sw);
        Assertions.assertThrows(
                IOException.class, () -> transport.sendBatchUntilReceived(in, out, 4L, "x"));
    }

    @Test
    void authenticateUnexpectedReplyFails() {
        EdgeTransportConfig config =
                EdgeTransportConfig.builder().jobId(1).authToken("tok").edgeIngressPort(1).build();
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
}
