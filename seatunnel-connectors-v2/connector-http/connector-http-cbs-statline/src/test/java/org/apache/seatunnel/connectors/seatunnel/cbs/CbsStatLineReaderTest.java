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

package org.apache.seatunnel.connectors.seatunnel.cbs;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CbsStatLineReaderTest {
    private static final String PATH = "/odata/v1/CBS/83765NED/Observations";
    private static final String FIRST =
            "{\"Id\":0,\"Measure\":\"GM000C\",\"Value\":null,\"StringValue\":\"Nederland "
                    + " \",\"WijkenEnBuurten\":\"NL00\"}";
    private static final String SECOND =
            "{\"Id\":1,\"Measure\":\"M2\",\"Value\":0.123456789012345678901,\"ValueAttribute\":\"None\"}";
    private HttpServer server;
    private String origin;
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicBoolean complete = new AtomicBoolean();
    private final List<SeaTunnelRow> rows = new ArrayList<>();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        origin = "http://127.0.0.1:" + server.getAddress().getPort();
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void readsOpaqueContinuationAndPreservesPublicDatasetFields() throws IOException {
        respond(
                200,
                "{\"value\":[" + FIRST + "],\"@odata.nextLink\":\"?$skiptoken=a%2Bb\"}",
                "{\"@odata.nextLink\":null,\"value\":[" + SECOND + "]}");
        read(0);
        assertTrue(complete.get());
        assertEquals(2, requests.get());
        assertEquals("83765NED", rows.get(0).getField(0));
        assertEquals(0L, rows.get(0).getField(1));
        assertNull(rows.get(0).getField(3));
        assertEquals("Nederland  ", rows.get(0).getField(4));
        assertTrue(((String) rows.get(0).getField(6)).contains("\"WijkenEnBuurten\":\"NL00\""));
        assertEquals("0.123456789012345678901", rows.get(1).getField(3));
        assertEquals("None", rows.get(1).getField(5));
    }

    @Test
    void limitsRowsWithoutRequestingAnotherPage() throws IOException {
        respond(
                200,
                "{\"value\":["
                        + FIRST
                        + ","
                        + SECOND
                        + "],\"@odata.nextLink\":\"?$skiptoken=a%2Bb\"}",
                null);
        read(1);
        assertTrue(complete.get());
        assertEquals(1, rows.size());
        assertEquals(1, requests.get());
    }

    @Test
    void rejectsWrongDatasetAndOrigins() {
        URI current = URI.create(origin + PATH);
        assertThrows(
                IOException.class,
                () -> CbsStatLineReader.continuation(current, "https://datasets.cbs.nl" + PATH));
        assertThrows(
                IOException.class,
                () -> CbsStatLineReader.continuation(current, "/odata/v1/CBS/other/Observations"));
        assertThrows(
                IOException.class, () -> CbsStatLineReader.continuation(current, PATH + "#part"));
        assertThrows(IOException.class, () -> CbsStatLineReader.continuation(current, ""));
    }

    @Test
    void failsWhenContinuationRepeats() {
        respond(200, "{\"value\":[],\"@odata.nextLink\":\"" + PATH + "\"}", null);
        assertThrows(IOException.class, () -> read(0));
        assertEquals(1, requests.get());
        assertFalse(complete.get());
    }

    @Test
    void nonSuccessIsNeverReportedAsEmptyDataset() {
        respond(429, "{\"error\":\"rate limited\"}", null);
        IOException error = assertThrows(IOException.class, () -> read(0));
        assertTrue(error.getMessage().contains("429"));
        assertFalse(complete.get());
    }

    @Test
    void failsOnAnInvalidEnvelope() {
        respond(200, "{\"value\":{}}", null);
        assertThrows(IOException.class, () -> read(0));
        assertFalse(complete.get());
    }

    @Test
    void validatesConfiguration() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CbsStatLineSource.observationsUri(origin, "../id", 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> CbsStatLineSource.observationsUri(origin, "valid", -1));
        assertThrows(
                IllegalArgumentException.class,
                () -> CbsStatLineSource.observationsUri("ftp://datasets.cbs.nl", "valid", 0));
    }

    private void respond(int status, String first, String second) {
        server.createContext(
                PATH,
                exchange -> {
                    int index = requests.getAndIncrement();
                    String payload = index == 0 ? first : second;
                    int code = status;
                    if (index != 0
                            && !"$skiptoken=a%2Bb".equals(exchange.getRequestURI().getRawQuery())) {
                        code = 400;
                    }
                    byte[] body =
                            (payload == null ? "{}" : payload).getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(code, body.length);
                    try (java.io.OutputStream output = exchange.getResponseBody()) {
                        output.write(body);
                    }
                });
    }

    private void read(int maxRows) throws IOException {
        SingleSplitReaderContext context =
                new SingleSplitReaderContext(null) {
                    @Override
                    public void signalNoMoreElement() {
                        complete.set(true);
                    }
                };
        try (CbsStatLineReader reader =
                new CbsStatLineReader(
                        CbsStatLineSource.observationsUri(origin, "83765NED", maxRows),
                        "83765NED",
                        maxRows,
                        context)) {
            reader.open();
            reader.internalPollNext(
                    new Collector<SeaTunnelRow>() {
                        @Override
                        public void collect(SeaTunnelRow row) {
                            rows.add(row);
                        }

                        @Override
                        public Object getCheckpointLock() {
                            return this;
                        }
                    });
        }
    }
}
