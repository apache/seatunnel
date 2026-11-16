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

package org.apache.seatunnel.connectors.druid.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DruidWriterTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name", "amount"},
                    new SeaTunnelDataType<?>[] {
                        BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.LONG_TYPE
                    });

    private static final List<SeaTunnelRow> ROWS =
            Arrays.asList(
                    new SeaTunnelRow(new Object[] {1, "a", 10L}),
                    new SeaTunnelRow(new Object[] {2, null, 20L}),
                    new SeaTunnelRow(new Object[] {3, "c", null}));

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void aNullFieldKeepsItsColumn() throws IOException {
        JsonNode ioConfig = submitIoConfig(ROWS);
        String[] lines = ioConfig.get("inputSource").get("data").asText().split("\n");

        Assertions.assertEquals(3, lines.length);
        // The last field is the processing time the writer appends, the same on every line.
        String timestamp = lines[0].substring(lines[0].lastIndexOf(',') + 1);
        Assertions.assertEquals("1,a,10," + timestamp, lines[0]);
        Assertions.assertEquals("2,,20," + timestamp, lines[1]);
        Assertions.assertEquals("3,c,," + timestamp, lines[2]);
    }

    @Test
    public void druidReadsEveryRowThatHasANullField(@TempDir Path temporaryDirectory)
            throws IOException {
        JsonNode ioConfig = submitIoConfig(ROWS);
        List<String> columns = new ArrayList<>();
        ioConfig.get("inputFormat").get("columns").forEach(column -> columns.add(column.asText()));
        Assertions.assertEquals(Arrays.asList("id", "name", "amount", "timestamp"), columns);

        // Read the submitted data with Druid's own CSV reader and the columns the task declares.
        // A row that comes out a field short is rejected there as having no timestamp.
        NullHandling.initializeForTests();
        InputRowSchema schema =
                new InputRowSchema(
                        new TimestampSpec("timestamp", "auto", null),
                        new DimensionsSpec(
                                Arrays.asList(
                                        new StringDimensionSchema("id"),
                                        new StringDimensionSchema("name"),
                                        new StringDimensionSchema("amount"))),
                        ColumnsFilter.all());
        List<InputRow> rows = new ArrayList<>();
        try (CloseableIterator<InputRow> iterator =
                new InlineInputSource(ioConfig.get("inputSource").get("data").asText())
                        .reader(
                                schema,
                                new CsvInputFormat(columns, "\n", null, false, 0),
                                temporaryDirectory.toFile())
                        .read()) {
            iterator.forEachRemaining(rows::add);
        }

        Assertions.assertEquals(3, rows.size());
        Assertions.assertNull(rows.get(1).getRaw("name"));
        Assertions.assertEquals("20", rows.get(1).getRaw("amount"));
        Assertions.assertEquals("c", rows.get(2).getRaw("name"));
        Assertions.assertNull(rows.get(2).getRaw("amount"));
    }

    /**
     * Every submission carries only the rows written since the previous submission. The writer
     * keeps the rows in one buffer, so a flush that does not clear it re-submits the earlier
     * batches; Druid runs with rollup disabled, so those rows are then stored twice.
     */
    @Test
    public void everyBatchSubmitsOnlyItsOwnRows() throws IOException {
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            rows.add(new SeaTunnelRow(new Object[] {i, "row-" + i, (long) i}));
        }

        List<JsonNode> ioConfigs = submitAllIoConfigs(rows, 2);

        List<Integer> linesPerBatch = new ArrayList<>();
        for (JsonNode ioConfig : ioConfigs) {
            linesPerBatch.add(ioConfig.get("inputSource").get("data").asText().split("\n").length);
        }
        Assertions.assertEquals(
                Arrays.asList(2, 2, 1),
                linesPerBatch,
                "a flush must not resubmit the rows of the previous batches");
    }

    /**
     * A submission Druid rejects must not count as done. The flush fails so the SeaTunnel task
     * fails, and the rejected rows stay pending for the next flush instead of being dropped
     * silently.
     */
    @Test
    public void rejectedSubmissionFailsTheFlushAndKeepsTheRows() throws IOException {
        List<String> requestBodies = new ArrayList<>();
        HttpServer server =
                startDruidOverlord(
                        requestBodies,
                        new Response(400, "{\"error\":\"Invalid task payload\"}"),
                        new Response(200, "{\"task\":\"task-1\"}"));
        try {
            // A batch size above the row count, so the only submissions are the explicit flushes.
            DruidWriter writer = newWriter(server, ROWS.size() + 1);
            for (SeaTunnelRow row : ROWS) {
                writer.write(row);
            }

            IOException failure = Assertions.assertThrows(IOException.class, writer::flush);
            Assertions.assertTrue(
                    failure.getMessage().contains("test_datasource"),
                    "the failure must name the datasource: " + failure.getMessage());
            Assertions.assertTrue(
                    failure.getMessage().contains("400"),
                    "the failure must carry the HTTP status: " + failure.getMessage());
            Assertions.assertTrue(
                    failure.getMessage().contains("Invalid task payload"),
                    "the failure must carry the response body: " + failure.getMessage());

            // The rejected rows are still pending, so the next flush submits them, once.
            writer.flush();
            writer.close();
        } finally {
            server.stop(0);
        }

        Assertions.assertEquals(
                2,
                requestBodies.size(),
                "the writer must not resubmit the rejected rows on its own");
        String[] lines = submittedRows(requestBodies.get(1)).split("\n");
        Assertions.assertEquals(
                ROWS.size(),
                lines.length,
                "the accepted flush must submit the original rows exactly once");
        Assertions.assertTrue(lines[0].startsWith("1,a,10,"), lines[0]);
        Assertions.assertTrue(lines[1].startsWith("2,,20,"), lines[1]);
        Assertions.assertTrue(lines[2].startsWith("3,c,,"), lines[2]);
    }

    /**
     * A 200 is an acceptance only when Druid returns the id of the indexing task it created as a
     * non-blank JSON string. A scalar or a blank id must fail the flush and leave the rows pending
     * instead of clearing them.
     */
    @Test
    public void malformedTaskIdFailsTheFlushAndKeepsTheRows() throws IOException {
        List<String> requestBodies = new ArrayList<>();
        HttpServer server =
                startDruidOverlord(
                        requestBodies,
                        new Response(200, "{\"task\":123}"),
                        new Response(200, "{\"task\":\"  \"}"),
                        new Response(200, "{\"task\":\"task-1\"}"));
        try {
            DruidWriter writer = newWriter(server, ROWS.size() + 1);
            for (SeaTunnelRow row : ROWS) {
                writer.write(row);
            }

            IOException numericId = Assertions.assertThrows(IOException.class, writer::flush);
            Assertions.assertTrue(
                    numericId.getMessage().contains("test_datasource"),
                    "the failure must name the datasource: " + numericId.getMessage());
            Assertions.assertTrue(
                    numericId.getMessage().contains("200"),
                    "the failure must carry the HTTP status: " + numericId.getMessage());
            Assertions.assertTrue(
                    numericId.getMessage().contains("123"),
                    "the failure must carry the response body: " + numericId.getMessage());

            IOException blankId = Assertions.assertThrows(IOException.class, writer::flush);
            Assertions.assertTrue(
                    blankId.getMessage().contains("200"),
                    "the failure must carry the HTTP status: " + blankId.getMessage());

            // Neither malformed answer cleared the rows, so the accepted flush submits them once.
            writer.flush();
            writer.close();
        } finally {
            server.stop(0);
        }

        Assertions.assertEquals(3, requestBodies.size(), "every flush must submit once");
        String[] lines = submittedRows(requestBodies.get(2)).split("\n");
        Assertions.assertEquals(
                ROWS.size(), lines.length, "a malformed 200 must not clear the buffered rows");
    }

    /**
     * A rejected final flush fails close() and must still close the HTTP client it opened. The
     * rejection is the failure the caller sees.
     */
    @Test
    public void rejectedFinalFlushFailsCloseAndClosesTheClient() throws IOException {
        List<String> requestBodies = new ArrayList<>();
        HttpServer server =
                startDruidOverlord(requestBodies, new Response(500, "{\"error\":\"rejected\"}"));
        CloseTrackingHttpClient httpClient = new CloseTrackingHttpClient();
        try {
            DruidWriter writer = newWriter(server, ROWS.size() + 1, httpClient);
            for (SeaTunnelRow row : ROWS) {
                writer.write(row);
            }

            IOException failure = Assertions.assertThrows(IOException.class, writer::close);
            Assertions.assertTrue(
                    failure.getMessage().contains("500"),
                    "the close failure must be the flush rejection: " + failure.getMessage());
            Assertions.assertTrue(
                    httpClient.isClosed(),
                    "close() must close the HTTP client even when the final flush is rejected");
        } finally {
            server.stop(0);
        }
    }

    /** A failure to close is suppressed onto the rejected flush instead of replacing it. */
    @Test
    public void aCloseFailureIsSuppressedOntoTheRejectedFlush() throws IOException {
        HttpServer server =
                startDruidOverlord(
                        new ArrayList<>(), new Response(500, "{\"error\":\"rejected\"}"));
        CloseTrackingHttpClient httpClient = new CloseTrackingHttpClient();
        IOException closeFailure = new IOException("client close failed");
        httpClient.failOnClose(closeFailure);
        try {
            DruidWriter writer = newWriter(server, ROWS.size() + 1, httpClient);
            for (SeaTunnelRow row : ROWS) {
                writer.write(row);
            }

            IOException failure = Assertions.assertThrows(IOException.class, writer::close);
            Assertions.assertTrue(
                    failure.getMessage().contains("500"),
                    "the rejected flush must be the failure the caller sees: "
                            + failure.getMessage());
            Assertions.assertArrayEquals(
                    new Throwable[] {closeFailure},
                    failure.getSuppressed(),
                    "the close failure must be suppressed onto the rejected flush");
        } finally {
            server.stop(0);
        }
    }

    /**
     * Writes the rows through a {@link DruidWriter} pointed at a local stand-in for the Druid
     * Overlord, and returns the {@code ioConfig} of the indexing task it submits on close.
     */
    private static JsonNode submitIoConfig(List<SeaTunnelRow> rows) throws IOException {
        // A batch size above the row count, so the only submission happens on close.
        List<JsonNode> ioConfigs = submitAllIoConfigs(rows, rows.size() + 1);
        Assertions.assertEquals(1, ioConfigs.size());
        return ioConfigs.get(0);
    }

    /**
     * Writes the rows through a {@link DruidWriter} with the given batch size and returns the
     * {@code ioConfig} of every indexing task it submits.
     */
    private static List<JsonNode> submitAllIoConfigs(List<SeaTunnelRow> rows, int batchSize)
            throws IOException {
        List<String> requestBodies = new ArrayList<>();
        HttpServer server =
                startDruidOverlord(requestBodies, new Response(200, "{\"task\":\"test\"}"));
        try {
            DruidWriter writer = newWriter(server, batchSize);
            for (SeaTunnelRow row : rows) {
                writer.write(row);
            }
            writer.close();
        } finally {
            server.stop(0);
        }

        List<JsonNode> ioConfigs = new ArrayList<>();
        for (String requestBody : requestBodies) {
            ioConfigs.add(MAPPER.readTree(requestBody).get("spec").get("ioConfig"));
        }
        return ioConfigs;
    }

    /** A {@link DruidWriter} pointed at the local stand-in for the Druid Overlord. */
    private static DruidWriter newWriter(HttpServer server, int batchSize) {
        return newWriter(server, batchSize, HttpClients.createDefault());
    }

    /** A {@link DruidWriter} pointed at the local stand-in, submitting with the given client. */
    private static DruidWriter newWriter(
            HttpServer server, int batchSize, CloseableHttpClient httpClient) {
        return new DruidWriter(
                ROW_TYPE,
                "127.0.0.1:" + server.getAddress().getPort(),
                "test_datasource",
                batchSize,
                httpClient);
    }

    /**
     * Starts a local stand-in for the Druid Overlord that records every submitted indexing task and
     * answers the requests with the given responses in order, repeating the last one once the
     * script runs out.
     */
    private static HttpServer startDruidOverlord(List<String> requestBodies, Response... responses)
            throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger received = new AtomicInteger();
        server.createContext(
                "/druid/indexer/v1/task",
                exchange -> {
                    try (InputStream in = exchange.getRequestBody()) {
                        ByteArrayOutputStream body = new ByteArrayOutputStream();
                        byte[] buffer = new byte[8192];
                        int read;
                        while ((read = in.read(buffer)) > 0) {
                            body.write(buffer, 0, read);
                        }
                        requestBodies.add(new String(body.toByteArray(), StandardCharsets.UTF_8));
                    }
                    Response response =
                            responses[Math.min(received.getAndIncrement(), responses.length - 1)];
                    byte[] payload = response.body.getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(response.status, payload.length);
                    try (OutputStream out = exchange.getResponseBody()) {
                        out.write(payload);
                    }
                });
        server.start();
        return server;
    }

    /** The CSV rows carried by the indexing task of a recorded request. */
    private static String submittedRows(String requestBody) throws IOException {
        return MAPPER.readTree(requestBody)
                .get("spec")
                .get("ioConfig")
                .get("inputSource")
                .get("data")
                .asText();
    }

    /** One scripted answer of the stand-in Overlord. */
    private static final class Response {
        private final int status;
        private final String body;

        private Response(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }

    /** A client that records whether it was closed, and can be made to fail on close. */
    private static final class CloseTrackingHttpClient extends CloseableHttpClient {
        private final CloseableHttpClient delegate = HttpClients.createDefault();
        private volatile boolean closed;
        private IOException closeFailure;

        @Override
        protected CloseableHttpResponse doExecute(
                HttpHost target, HttpRequest request, HttpContext context) throws IOException {
            return delegate.execute(target, request, context);
        }

        @Override
        public ClientConnectionManager getConnectionManager() {
            return delegate.getConnectionManager();
        }

        @Override
        public HttpParams getParams() {
            return delegate.getParams();
        }

        @Override
        public void close() throws IOException {
            closed = true;
            delegate.close();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        private boolean isClosed() {
            return closed;
        }

        private void failOnClose(IOException failure) {
            this.closeFailure = failure;
        }
    }
}
