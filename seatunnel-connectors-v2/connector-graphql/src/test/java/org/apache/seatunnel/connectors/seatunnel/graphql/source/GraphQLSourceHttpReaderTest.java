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

package org.apache.seatunnel.connectors.seatunnel.graphql.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.graphql.exception.GraphQLConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.graphql.exception.GraphQLConnectorException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Checks that the GraphQL source fails when the server reports errors with HTTP 200. */
public class GraphQLSourceHttpReaderTest {

    private HttpServer server;
    private volatile String responseBody;
    private volatile String requestBody;

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext(
                "/graphql",
                exchange -> {
                    requestBody = readAll(exchange.getRequestBody());
                    byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, bytes.length);
                    try (OutputStream out = exchange.getResponseBody()) {
                        out.write(bytes);
                    }
                });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void graphQLErrorsInAnOkResponseFailTheRead() throws Exception {
        responseBody =
                "{\"errors\":[{\"extensions\":{\"code\":\"validation-failed\"},"
                        + "\"message\":\"field 'source' not found in type: 'query_root'\"}]}";
        GraphQLConnectorException e =
                Assertions.assertThrows(GraphQLConnectorException.class, () -> read());
        Assertions.assertEquals(
                GraphQLConnectorErrorCode.GRAPHQL_SOURCE_OPERATION_ERROR,
                e.getSeaTunnelErrorCode());
        Assertions.assertTrue(e.getMessage().contains("not found in type"), e.getMessage());
    }

    @Test
    void partialDataWithErrorsFailsTheRead() {
        responseBody =
                "{\"data\":{\"source\":[{\"id\":1,\"name\":\"a\"}]},"
                        + "\"errors\":[{\"message\":\"cannot read field name\"}]}";
        Assertions.assertThrows(GraphQLConnectorException.class, () -> read());
    }

    @Test
    void anOkResponseWithoutErrorsIsRead() throws Exception {
        responseBody =
                "{\"data\":{\"source\":[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"}]}}";
        List<SeaTunnelRow> rows = read();
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(1, rows.get(0).getField(0));
        Assertions.assertEquals("b", rows.get(1).getField(1));
    }

    @Test
    void aQueryWithoutVariablesSendsAnEmptyObject() throws Exception {
        responseBody = "{\"data\":{\"source\":[{\"id\":1,\"name\":\"a\"}]}}";
        read();
        JsonNode variables = JsonUtils.stringToJsonNode(requestBody).get("variables");
        Assertions.assertTrue(variables.isObject(), requestBody);
        Assertions.assertEquals(0, variables.size(), requestBody);
    }

    private static String readAll(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int n;
        while ((n = in.read(buffer)) > 0) {
            out.write(buffer, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    private List<SeaTunnelRow> read() throws Exception {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", "int");
        fields.put("name", "string");
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", fields);
        Map<String, Object> config = new HashMap<>();
        config.put("url", "http://127.0.0.1:" + server.getAddress().getPort() + "/graphql");
        config.put("query", "query Q { source { id name } }");
        config.put("content_field", "$.data.source");
        config.put("schema", schema);

        SingleSplitReaderContext context = Mockito.mock(SingleSplitReaderContext.class);
        Mockito.when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        AbstractSingleSplitReader<SeaTunnelRow> reader =
                new GraphQLSource(ReadonlyConfig.fromMap(config)).createReader(context);
        List<SeaTunnelRow> rows = new ArrayList<>();
        Object lock = new Object();
        reader.open();
        try {
            reader.pollNext(
                    new Collector<SeaTunnelRow>() {
                        @Override
                        public void collect(SeaTunnelRow record) {
                            rows.add(record);
                        }

                        @Override
                        public Object getCheckpointLock() {
                            return lock;
                        }
                    });
        } finally {
            reader.close();
        }
        return rows;
    }
}
