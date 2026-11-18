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

package org.apache.seatunnel.connectors.seatunnel.graphql.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.graphql.config.GraphQLSinkParameter;
import org.apache.seatunnel.connectors.seatunnel.graphql.exception.GraphQLConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.graphql.exception.GraphQLConnectorException;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

/** Checks that the GraphQL sink fails a write the server rejects, and how it sends values. */
public class GraphQLSinkWriterTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "created_at", "day", "at_time"},
                    new SeaTunnelDataType<?>[] {
                        BasicType.INT_TYPE,
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        LocalTimeType.LOCAL_DATE_TYPE,
                        LocalTimeType.LOCAL_TIME_TYPE
                    });

    private static final SeaTunnelRow ROW =
            new SeaTunnelRow(
                    new Object[] {
                        1,
                        LocalDateTime.of(2020, 2, 2, 2, 2),
                        LocalDate.of(2020, 2, 2),
                        LocalTime.of(10, 15, 30, 123_000_000)
                    });

    private HttpServer server;
    private volatile int status;
    private volatile String responseBody;
    private volatile String requestBody;

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext(
                "/graphql",
                exchange -> {
                    requestBody = read(exchange.getRequestBody());
                    byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(status, bytes.length);
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
    void graphQLErrorsInAnOkResponseFailTheWrite() throws Exception {
        respond(
                200,
                "{\"errors\":[{\"extensions\":{\"code\":\"validation-failed\"},"
                        + "\"message\":\"no mutations exist\"}]}");
        GraphQLSinkWriter writer = newWriter();
        try {
            GraphQLConnectorException e =
                    Assertions.assertThrows(
                            GraphQLConnectorException.class, () -> writer.write(ROW));
            Assertions.assertEquals(
                    GraphQLConnectorErrorCode.GRAPHQL_SINK_OPERATION_ERROR,
                    e.getSeaTunnelErrorCode());
            Assertions.assertTrue(e.getMessage().contains("no mutations exist"), e.getMessage());
        } finally {
            writer.close();
        }
    }

    @Test
    void aNonOkResponseFailsTheWrite() throws Exception {
        respond(503, "service unavailable");
        GraphQLSinkWriter writer = newWriter();
        try {
            HttpConnectorException e =
                    Assertions.assertThrows(HttpConnectorException.class, () -> writer.write(ROW));
            Assertions.assertEquals(
                    HttpConnectorErrorCode.REQUEST_FAILED, e.getSeaTunnelErrorCode());
            Assertions.assertTrue(e.getMessage().contains("503"), e.getMessage());
        } finally {
            writer.close();
        }
    }

    @Test
    void anAcceptedMutationSendsDateAndTimeValuesAsIsoStrings() throws Exception {
        respond(200, "{\"data\":{\"insert_sink\":{\"affected_rows\":1}}}");
        GraphQLSinkWriter writer = newWriter();
        try {
            writer.write(ROW);
        } finally {
            writer.close();
        }
        JsonNode variables = JsonUtils.stringToJsonNode(requestBody).get("variables");
        Assertions.assertEquals(1, variables.get("id").asInt());
        Assertions.assertEquals("2020-02-02T02:02:00", variables.get("created_at").asText());
        Assertions.assertEquals("2020-02-02", variables.get("day").asText());
        Assertions.assertEquals("10:15:30.123", variables.get("at_time").asText());
    }

    @Test
    void anEmptyErrorsArrayIsNotAFailure() throws Exception {
        respond(200, "{\"data\":{\"insert_sink\":{\"affected_rows\":1}},\"errors\":[]}");
        GraphQLSinkWriter writer = newWriter();
        try {
            writer.write(ROW);
        } finally {
            writer.close();
        }
    }

    @Test
    void anOffsetDateTimeKeepsItsOffset() {
        Assertions.assertEquals(
                "2020-02-02T02:02:00+08:00",
                GraphQLSinkWriter.toVariableValue(
                        OffsetDateTime.of(2020, 2, 2, 2, 2, 0, 0, ZoneOffset.ofHours(8))));
        Assertions.assertEquals(7, GraphQLSinkWriter.toVariableValue(7));
    }

    @Test
    void aSinkWithoutVariablesSendsTheRowAsAnObject() throws Exception {
        respond(200, "{\"data\":{\"insert_sink\":{\"affected_rows\":1}}}");
        GraphQLSinkWriter writer = newWriter(false);
        try {
            writer.write(ROW);
        } finally {
            writer.close();
        }
        JsonNode variables = JsonUtils.stringToJsonNode(requestBody).get("variables");
        Assertions.assertTrue(variables.isObject(), requestBody);
        Assertions.assertEquals(1, variables.get("id").asInt());
    }

    private void respond(int status, String body) {
        this.status = status;
        this.responseBody = body;
    }

    private GraphQLSinkWriter newWriter() {
        return newWriter(true);
    }

    private GraphQLSinkWriter newWriter(boolean withVariables) {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "http://127.0.0.1:" + server.getAddress().getPort() + "/graphql");
        config.put(
                "query",
                "mutation M($id: Int!) { insert_sink(objects: {id: $id}) { affected_rows } }");
        if (withVariables) {
            config.put("variables", new HashMap<>());
        }
        return new GraphQLSinkWriter(
                ROW_TYPE, new GraphQLSinkParameter(ReadonlyConfig.fromMap(config)));
    }

    private static String read(InputStream in) throws java.io.IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int n;
        while ((n = in.read(buffer)) > 0) {
            out.write(buffer, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
}
