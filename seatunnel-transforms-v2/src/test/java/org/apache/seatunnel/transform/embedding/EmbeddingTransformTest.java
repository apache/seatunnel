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

package org.apache.seatunnel.transform.embedding;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractMultiCatalogTransform;
import org.apache.seatunnel.transform.nlpmodel.embedding.EmbeddingMultiCatalogTransform;
import org.apache.seatunnel.transform.nlpmodel.embedding.EmbeddingTransform;

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class EmbeddingTransformTest {

    @Test
    void multiTableLifecycleReusesSchemaClientAndReleasesItsConnectionPool() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/embeddings",
                exchange -> {
                    requests.incrementAndGet();
                    exchange.getRequestBody().readAllBytes();
                    byte[] response =
                            "{\"data\":[{\"embedding\":[0.1,0.2]}]}"
                                    .getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, response.length);
                    try (java.io.OutputStream output = exchange.getResponseBody()) {
                        output.write(response);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        EmbeddingMultiCatalogTransform wrapper = null;
        SeaTunnelTransform<SeaTunnelRow> child = null;
        org.apache.http.impl.client.CloseableHttpClient schemaClient = null;
        try {
            CatalogTable table =
                    CatalogTableUtil.getCatalogTable(
                            "catalog",
                            "database",
                            "schema",
                            "texts",
                            new SeaTunnelRowType(
                                    new String[] {"text"},
                                    new SeaTunnelDataType[] {BasicType.STRING_TYPE}));
            Map<String, Object> config = new HashMap<>();
            config.put("model_provider", "OPENAI");
            config.put("model", "test-model");
            config.put("api_key", "test-key");
            config.put(
                    "api_path",
                    "http://127.0.0.1:" + server.getAddress().getPort() + "/embeddings");
            config.put("vectorization_fields", Collections.singletonMap("vector", "text"));
            wrapper =
                    new EmbeddingMultiCatalogTransform(
                            Collections.singletonList(table), ReadonlyConfig.fromMap(config));
            Map<String, SeaTunnelTransform<SeaTunnelRow>> children =
                    (Map<String, SeaTunnelTransform<SeaTunnelRow>>)
                            field(wrapper, AbstractMultiCatalogTransform.class, "transformMap");
            child = children.values().iterator().next();
            Object model = field(child, EmbeddingTransform.class, "model");
            Object client = field(model, model.getClass(), "client");
            schemaClient = (org.apache.http.impl.client.CloseableHttpClient) client;
            PoolingHttpClientConnectionManager pool =
                    (PoolingHttpClientConnectionManager)
                            field(client, client.getClass(), "connManager");
            Assertions.assertEquals(
                    1, requests.get(), "Schema discovery should initialize dimension once");
            wrapper.open();
            wrapper.open();
            Assertions.assertSame(model, field(child, EmbeddingTransform.class, "model"));
            Assertions.assertEquals(
                    1, requests.get(), "Runtime open must reuse schema initialization");
            SeaTunnelRow input = new SeaTunnelRow(new Object[] {"hello"});
            input.setTableId(table.getTableId().toTablePath().toString());
            Assertions.assertEquals(2, wrapper.map(input).getArity());
            Assertions.assertEquals(2, requests.get());
            Assertions.assertEquals(1, pool.getTotalStats().getAvailable());
            wrapper.close();
            wrapper.close();
            Assertions.assertTrue(
                    ((AtomicBoolean) field(pool, pool.getClass(), "isShutDown")).get());
            Assertions.assertEquals(0, pool.getTotalStats().getAvailable());
            Assertions.assertThrows(IllegalStateException.class, child::open);
            Assertions.assertNull(field(child, EmbeddingTransform.class, "model"));
            Assertions.assertEquals(2, requests.get());
        } finally {
            try {
                if (wrapper != null) {
                    wrapper.close();
                }
            } finally {
                try {
                    // Also release the fixture's child if an assertion fails against the old
                    // wrapper.
                    if (child != null) {
                        child.close();
                    }
                } finally {
                    try {
                        if (schemaClient != null) {
                            schemaClient.close();
                        }
                    } finally {
                        server.stop(0);
                    }
                }
            }
        }
    }

    private static Object field(Object target, Class<?> owner, String name) throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    @Test
    void testOutputColumns() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        String sourceConfig =
                "{\"path\":\"/seatunnel/test_csv_data.csv\",\"bucket\":\"s3a://ltchen\",\"fs.s3a.endpoint\":\"tos-s3-cn-beijing.volces.com\",\"fs.s3a.aws.credentials.provider\":\"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider\",\"file_format_type\":\"csv\",\"access_key\":\"xxx\",\"secret_key\":\"xxx\",\"csv_use_header_line\":true,\"field_delimiter\":\",\",\"schema\":{\"fields\":{\"id\":\"int\",\"code\":\"int\",\"data\":\"string\",\"success\":\"boolean\"},\"primaryKey\":{\"name\":\"id\",\"columnNames\":[\"id\"]}},\"plugin_name\":\"S3File\"}";
        Map<String, Object> sourceConfigMap =
                objectMapper.readValue(sourceConfig, new TypeReference<Map<String, Object>>() {});
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(sourceConfigMap);
        CatalogTable inputCatalogTable = CatalogTableUtil.buildWithConfig("S3File", readonlyConfig);

        int dimension = 1024;
        String embeddingConfig =
                "{\"model_provider\":\"AMAZON\",\"model\":\"amazon.titan-embed-text-v2:0\",\"aws_region\": \"us-east-1\", \"api_key\":\"xxx\",\"secret_key\":\"xxx\",\"api_path\": \"https://aws.amazon.com/bedrock/amazon-models\", \"dimension\": "
                        + dimension
                        + ",\"vectorization_fields\":{\"data_vector\":\"data\"},\"plugin_name\":\"Embedding\"}";
        Map<String, Object> embeddingConfigMap =
                objectMapper.readValue(
                        embeddingConfig, new TypeReference<Map<String, Object>>() {});
        ReadonlyConfig config = ReadonlyConfig.fromMap(embeddingConfigMap);
        EmbeddingTransform embeddingTransform = new EmbeddingTransform(config, inputCatalogTable);

        Column[] columns = embeddingTransform.getOutputColumns();
        for (Column column : columns) {
            Assertions.assertEquals(dimension, column.getScale());
        }
    }
}
