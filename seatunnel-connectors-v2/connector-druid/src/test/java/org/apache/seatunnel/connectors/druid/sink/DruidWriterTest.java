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
     * Writes the rows through a {@link DruidWriter} pointed at a local stand-in for the Druid
     * Overlord, and returns the {@code ioConfig} of the indexing task it submits on close.
     */
    private static JsonNode submitIoConfig(List<SeaTunnelRow> rows) throws IOException {
        List<String> requestBodies = new ArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
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
                    byte[] response = "{\"task\":\"test\"}".getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, response.length);
                    try (OutputStream out = exchange.getResponseBody()) {
                        out.write(response);
                    }
                });
        server.start();
        try {
            // A batch size above the row count, so the only submission happens on close.
            DruidWriter writer =
                    new DruidWriter(
                            ROW_TYPE,
                            "127.0.0.1:" + server.getAddress().getPort(),
                            "test_datasource",
                            rows.size() + 1);
            for (SeaTunnelRow row : rows) {
                writer.write(row);
            }
            writer.close();
        } finally {
            server.stop(0);
        }

        Assertions.assertEquals(1, requestBodies.size());
        return new ObjectMapper().readTree(requestBodies.get(0)).get("spec").get("ioConfig");
    }
}
