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

package org.apache.seatunnel.connectors.seatunnel.deeplake.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.VectorUtils;
import org.apache.seatunnel.connectors.seatunnel.deeplake.config.DeepLakeSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.deeplake.config.DeepLakeSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.deeplake.exception.DeepLakeConnectorException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeepLakeSinkWriterTest {

    private HttpServer server;
    private String apiUrl;
    private final List<String> requestBodies = Collections.synchronizedList(new ArrayList<>());
    private final List<String> requestPaths = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger responseStatus = new AtomicInteger(204);

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/workspaces/research/tables/query", this::handleRequest);
        server.start();
        apiUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void createsTableAndWritesParameterizedBatchWithAuthentication() throws IOException {
        CatalogTable table = catalogTable();
        DeepLakeSinkWriter writer = writer(table, SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, 2);

        writer.write(row(10L, "first document", new byte[] {1, 2, 3}, 0.1F, 0.2F));
        assertEquals(1, writer.bufferedRows());
        writer.write(
                row(
                        11L,
                        "second document'); DROP TABLE documents; --",
                        new byte[] {4, 5},
                        0.3F,
                        0.4F));

        assertEquals(0, writer.bufferedRows());
        assertEquals(2, requestBodies.size());
        assertEquals("/workspaces/research/tables/query", requestPaths.get(0));
        assertEquals("/workspaces/research/tables/query/batch", requestPaths.get(1));
        assertTrue(requestBodies.get(0).contains("CREATE TABLE IF NOT EXISTS"));
        assertTrue(requestBodies.get(0).contains("FLOAT4[]"));
        assertTrue(requestBodies.get(1).contains("params_batch"));
        assertTrue(requestBodies.get(1).contains("first document"));
        assertTrue(requestBodies.get(1).contains("[0.1,0.2]"));
        assertTrue(requestBodies.get(1).contains("AQID"));
        assertTrue(requestBodies.get(1).contains("DROP TABLE documents"));
        assertTrue(
                requestBodies
                        .get(1)
                        .contains("VALUES ($1, $2, decode($3, 'base64'), $4::float4[])"));
        writer.close();
    }

    @Test
    void doesNotRetryFailedBatchWhenWriterCloses() throws IOException {
        DeepLakeSinkWriter writer = writer(catalogTable(), SchemaSaveMode.IGNORE, 1);
        responseStatus.set(503);

        DeepLakeConnectorException error =
                assertThrows(
                        DeepLakeConnectorException.class,
                        () -> writer.write(row(10L, "document", new byte[] {1}, 0.1F, 0.2F)));

        assertTrue(error.getMessage().contains("HTTP 503"));
        assertEquals(1, writer.bufferedRows());
        assertEquals(1, requestBodies.size());

        DeepLakeConnectorException terminalError =
                assertThrows(
                        DeepLakeConnectorException.class,
                        () -> writer.write(row(11L, "later", new byte[] {2}, 0.3F, 0.4F)));
        assertTrue(terminalError.getMessage().contains("cannot continue"));
        assertEquals(1, requestBodies.size());
        responseStatus.set(204);
        writer.close();
        assertEquals(1, requestBodies.size());
    }

    @Test
    void rejectsNonInsertRowsBeforeSendingThem() throws IOException {
        DeepLakeSinkWriter writer = writer(catalogTable(), SchemaSaveMode.IGNORE, 10);
        SeaTunnelRow update = row(10L, "updated document", new byte[] {1}, 0.1F, 0.2F);
        update.setRowKind(RowKind.UPDATE_AFTER);

        DeepLakeConnectorException error =
                assertThrows(DeepLakeConnectorException.class, () -> writer.write(update));

        assertTrue(error.getMessage().contains("append-only"));
        assertTrue(requestBodies.isEmpty());
        writer.write(row(11L, "valid document", new byte[] {2}, 0.3F, 0.4F));
        writer.close();
        assertEquals(1, requestBodies.size());
    }

    @Test
    void rejectsUnsupportedTypesForEveryExistingTableMode() {
        CatalogTable unsupportedTable = unsupportedCatalogTable();

        for (SchemaSaveMode mode :
                Arrays.asList(SchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST, SchemaSaveMode.IGNORE)) {
            DeepLakeConnectorException error =
                    assertThrows(
                            DeepLakeConnectorException.class,
                            () -> writer(unsupportedTable, mode, 10));

            assertTrue(error.getMessage().contains("FLOAT16_VECTOR"));
        }
        assertTrue(requestBodies.isEmpty());
    }

    @Test
    void validatesExistingTableWhenConfigured() throws IOException {
        DeepLakeSinkWriter writer =
                writer(catalogTable(), SchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST, 10);

        assertEquals(1, requestBodies.size());
        assertTrue(
                requestBodies
                        .get(0)
                        .contains("SELECT 1 FROM \\\"research\\\".\\\"documents\\\" LIMIT 0"));
        writer.close();
    }

    @Test
    void flushesPartialBatchWhenPreparingCommit() throws IOException {
        DeepLakeSinkWriter writer = writer(catalogTable(), SchemaSaveMode.IGNORE, 2);
        writer.write(row(10L, "document", new byte[] {1}, 0.1F, 0.2F));

        assertTrue(requestBodies.isEmpty());
        writer.prepareCommit();
        assertEquals(1, requestBodies.size());
        assertEquals(0, writer.bufferedRows());
        writer.close();
    }

    @Test
    void flushesPartialBatchWhenWriterCloses() throws IOException {
        DeepLakeSinkWriter writer = writer(catalogTable(), SchemaSaveMode.IGNORE, 2);
        writer.write(row(10L, "document", new byte[] {1}, 0.1F, 0.2F));

        assertTrue(requestBodies.isEmpty());
        writer.close();
        assertEquals(1, requestBodies.size());
    }

    @Test
    void convertsNestedArrayElementsUsingTheirDeclaredType() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"scores"},
                        new SeaTunnelDataType[] {
                            new ArrayType<>(Float[][].class, ArrayType.FLOAT_ARRAY_TYPE)
                        });
        SeaTunnelRow row =
                new SeaTunnelRow(new Object[] {new Float[][] {{1.0F, 2.0F}, {3.0F, 4.0F}}});

        List<Object> converted = DeepLakeRowConverter.convert(row, rowType);

        assertEquals(
                Collections.singletonList(
                        Arrays.asList(Arrays.asList(1.0F, 2.0F), Arrays.asList(3.0F, 4.0F))),
                converted);
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        assertEquals("POST", exchange.getRequestMethod());
        assertEquals("Bearer test-api-key", exchange.getRequestHeaders().getFirst("Authorization"));
        assertEquals("test-org", exchange.getRequestHeaders().getFirst("X-Activeloop-Org-Id"));
        requestPaths.add(exchange.getRequestURI().getPath());
        requestBodies.add(
                new String(readAllBytes(exchange.getRequestBody()), StandardCharsets.UTF_8));
        int status = responseStatus.get();
        byte[] response =
                status >= 300
                        ? "service unavailable".getBytes(StandardCharsets.UTF_8)
                        : new byte[0];
        exchange.sendResponseHeaders(status, response.length == 0 ? -1 : response.length);
        if (response.length > 0) {
            exchange.getResponseBody().write(response);
        }
        exchange.close();
    }

    private static byte[] readAllBytes(InputStream input) throws IOException {
        java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int count;
        while ((count = input.read(buffer)) != -1) {
            output.write(buffer, 0, count);
        }
        return output.toByteArray();
    }

    private DeepLakeSinkWriter writer(
            CatalogTable table, SchemaSaveMode schemaSaveMode, int batchSize) {
        Map<String, Object> options = new HashMap<>();
        options.put(DeepLakeSinkOptions.API_URL.key(), apiUrl);
        options.put(DeepLakeSinkOptions.API_KEY.key(), "test-api-key");
        options.put(DeepLakeSinkOptions.ORG_ID.key(), "test-org");
        options.put(DeepLakeSinkOptions.WORKSPACE.key(), "research");
        options.put(DeepLakeSinkOptions.TABLE.key(), "documents");
        options.put(DeepLakeSinkOptions.BATCH_SIZE.key(), batchSize);
        options.put(DeepLakeSinkOptions.SCHEMA_SAVE_MODE.key(), schemaSaveMode.name());
        return new DeepLakeSinkWriter(
                table, new DeepLakeSinkConfig(ReadonlyConfig.fromMap(options), table));
    }

    private static CatalogTable catalogTable() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.builder()
                                .name("document_id")
                                .dataType(BasicType.LONG_TYPE)
                                .nullable(false)
                                .build(),
                        PhysicalColumn.builder()
                                .name("content")
                                .dataType(BasicType.STRING_TYPE)
                                .nullable(true)
                                .build(),
                        PhysicalColumn.builder()
                                .name("payload")
                                .dataType(PrimitiveByteArrayType.INSTANCE)
                                .nullable(true)
                                .build(),
                        PhysicalColumn.builder()
                                .name("embedding")
                                .dataType(VectorType.VECTOR_FLOAT_TYPE)
                                .nullable(true)
                                .build());
        return CatalogTable.of(
                TableIdentifier.of("deeplake", "research", "documents"),
                TableSchema.builder().columns(columns).build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "Deep Lake documents");
    }

    private static CatalogTable unsupportedCatalogTable() {
        return CatalogTable.of(
                TableIdentifier.of("deeplake", "research", "documents"),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("embedding")
                                        .dataType(VectorType.VECTOR_FLOAT16_TYPE)
                                        .nullable(true)
                                        .build())
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "Unsupported Deep Lake documents");
    }

    private static SeaTunnelRow row(long id, String content, byte[] payload, Float... embedding) {
        return new SeaTunnelRow(
                new Object[] {id, content, payload, VectorUtils.toByteBuffer(embedding)});
    }
}
