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

package org.apache.seatunnel.connectors.seatunnel.posthog.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.connectors.seatunnel.http.source.DeserializationCollector;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class PostHogSourceReaderTest {

    private PostHogSourceReader reader;
    private List<SeaTunnelRow> rows;
    private Collector<SeaTunnelRow> output;

    @BeforeEach
    public void setUp() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"event", "distinct_id"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});
        DeserializationCollector collector =
                new DeserializationCollector(new JsonTextDeserializationSchema());
        reader = new PostHogSourceReader(new HttpParameter(), null, collector, rowType);
        rows = new ArrayList<>();
        output = listCollector(rows);
    }

    @Test
    public void testMapColumnOrientedResponseToJsonRows() throws Exception {
        reader.collectResponse(
                "{\"columns\":[\"distinct_id\",\"event\",\"extra\"],"
                        + "\"results\":[[\"user-1\",\"signup\",{\"plan\":\"pro\"}]],"
                        + "\"query_status\":{\"complete\":true,\"error\":false}}",
                output);

        Assertions.assertEquals(1, rows.size());
        JsonNode row = JsonUtils.stringToJsonNode((String) rows.get(0).getField(0));
        Assertions.assertEquals("signup", row.path("event").asText());
        Assertions.assertEquals("user-1", row.path("distinct_id").asText());
        Assertions.assertEquals("pro", row.at("/extra/plan").asText());
    }

    @Test
    public void testAcceptEmptyResults() throws Exception {
        reader.collectResponse("{\"columns\":[\"event\",\"distinct_id\"],\"results\":[]}", output);

        Assertions.assertTrue(rows.isEmpty());
    }

    @Test
    public void testRejectMissingSchemaColumn() {
        HttpConnectorException exception =
                Assertions.assertThrows(
                        HttpConnectorException.class,
                        () ->
                                reader.collectResponse(
                                        "{\"columns\":[\"event\"],\"results\":[[\"signup\"]]}",
                                        output));

        Assertions.assertTrue(exception.getMessage().contains("distinct_id"));
        Assertions.assertTrue(exception.getMessage().contains("Alias"));
    }

    @Test
    public void testRejectDuplicateColumns() {
        Assertions.assertThrows(
                HttpConnectorException.class,
                () ->
                        reader.collectResponse(
                                "{\"columns\":[\"event\",\"event\"],\"results\":[]}", output));
    }

    @Test
    public void testRejectResultWidthMismatch() {
        Assertions.assertThrows(
                HttpConnectorException.class,
                () ->
                        reader.collectResponse(
                                "{\"columns\":[\"event\",\"distinct_id\"],"
                                        + "\"results\":[[\"signup\"]]}",
                                output));
    }

    @Test
    public void testRejectIncompleteBlockingQuery() {
        Assertions.assertThrows(
                HttpConnectorException.class,
                () ->
                        reader.collectResponse(
                                "{\"query_status\":{\"complete\":false,\"error\":false},"
                                        + "\"columns\":[],\"results\":[]}",
                                output));
    }

    @Test
    public void testRejectQueryError() {
        HttpConnectorException exception =
                Assertions.assertThrows(
                        HttpConnectorException.class,
                        () ->
                                reader.collectResponse(
                                        "{\"query_status\":{\"complete\":true,\"error\":true,"
                                                + "\"error_message\":\"invalid HogQL\"}}",
                                        output));

        Assertions.assertTrue(exception.getMessage().contains("invalid HogQL"));
    }

    @Test
    public void testRejectTopLevelQueryError() {
        HttpConnectorException exception =
                Assertions.assertThrows(
                        HttpConnectorException.class,
                        () ->
                                reader.collectResponse(
                                        "{\"error\":\"query timed out\",\"columns\":[],\"results\":[]}",
                                        output));

        Assertions.assertTrue(exception.getMessage().contains("query timed out"));
    }

    @Test
    public void testRejectMissingResults() {
        Assertions.assertThrows(
                HttpConnectorException.class,
                () -> reader.collectResponse("{\"columns\":[\"event\",\"distinct_id\"]}", output));
    }

    @Test
    public void testRejectNonArrayResult() {
        Assertions.assertThrows(
                HttpConnectorException.class,
                () ->
                        reader.collectResponse(
                                "{\"columns\":[\"event\",\"distinct_id\"],"
                                        + "\"results\":[{\"event\":\"signup\"}]}",
                                output));
    }

    @Test
    public void testRejectInvalidJson() {
        Assertions.assertThrows(
                HttpConnectorException.class, () -> reader.collectResponse("not-json", output));
    }

    private static Collector<SeaTunnelRow> listCollector(List<SeaTunnelRow> rows) {
        return new Collector<SeaTunnelRow>() {
            @Override
            public void collect(SeaTunnelRow record) {
                rows.add(record);
            }

            @Override
            public Object getCheckpointLock() {
                return this;
            }
        };
    }

    private static final class JsonTextDeserializationSchema
            implements DeserializationSchema<SeaTunnelRow> {

        private final SeaTunnelRowType producedType;

        private JsonTextDeserializationSchema() {
            this.producedType =
                    new SeaTunnelRowType(
                            new String[] {"json"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        }

        @Override
        public SeaTunnelRow deserialize(byte[] message) throws IOException {
            return new SeaTunnelRow(new Object[] {new String(message, StandardCharsets.UTF_8)});
        }

        @Override
        public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
            return producedType;
        }
    }
}
