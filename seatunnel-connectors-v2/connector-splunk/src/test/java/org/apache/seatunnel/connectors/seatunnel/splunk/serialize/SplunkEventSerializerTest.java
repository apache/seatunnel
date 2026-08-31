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

package org.apache.seatunnel.connectors.seatunnel.splunk.serialize;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.splunk.config.SplunkSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.splunk.exception.SplunkConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

class SplunkEventSerializerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "message", "hostname", "event_time", "epoch_millis"},
                    new SeaTunnelDataType<?>[] {
                        BasicType.LONG_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.STRING_TYPE,
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        BasicType.LONG_TYPE
                    });

    private static SeaTunnelRow row() {
        return new SeaTunnelRow(
                new Object[] {
                    1L,
                    "hello splunk",
                    "web-01",
                    LocalDateTime.of(2026, 8, 17, 12, 30, 45, 123_000_000),
                    1_755_432_645_123L
                });
    }

    private static SplunkSinkConfig configOf(Map<String, Object> extraOptions) {
        Map<String, Object> options = new HashMap<>();
        options.put("url", "https://splunk-host:8088");
        options.put("token", "test-token");
        options.putAll(extraOptions);
        return new SplunkSinkConfig(ReadonlyConfig.fromMap(options));
    }

    private static JsonNode serialize(Map<String, Object> extraOptions) throws Exception {
        SplunkSinkConfig config = configOf(extraOptions);
        SplunkEventSerializer serializer = new SplunkEventSerializer(ROW_TYPE, config);
        return MAPPER.readTree(serializer.serialize(row()));
    }

    @Test
    void writesRowUnderEventAndOmitsUnconfiguredMetadata() throws Exception {
        JsonNode envelope = serialize(new HashMap<>());

        Assertions.assertFalse(envelope.has("index"));
        Assertions.assertFalse(envelope.has("source"));
        Assertions.assertFalse(envelope.has("sourcetype"));
        Assertions.assertFalse(envelope.has("host"));
        Assertions.assertFalse(envelope.has("time"));

        JsonNode event = envelope.get("event");
        Assertions.assertEquals(1L, event.get("id").asLong());
        Assertions.assertEquals("hello splunk", event.get("message").asText());
        Assertions.assertEquals("web-01", event.get("hostname").asText());
    }

    @Test
    void writesConfiguredMetadataFields() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("index", "main");
        options.put("source", "seatunnel");
        options.put("sourcetype", "_json");
        options.put("host", "static-host");

        JsonNode envelope = serialize(options);

        Assertions.assertEquals("main", envelope.get("index").asText());
        Assertions.assertEquals("seatunnel", envelope.get("source").asText());
        Assertions.assertEquals("_json", envelope.get("sourcetype").asText());
        Assertions.assertEquals("static-host", envelope.get("host").asText());
    }

    @Test
    void hostFieldOverridesStaticHost() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("host", "static-host");
        options.put("host_field", "hostname");

        Assertions.assertEquals("web-01", serialize(options).get("host").asText());
    }

    @Test
    void hostFieldFallsBackToStaticHostWhenRowValueIsNull() {
        Map<String, Object> options = new HashMap<>();
        options.put("host", "static-host");
        options.put("host_field", "hostname");

        SplunkEventSerializer serializer = new SplunkEventSerializer(ROW_TYPE, configOf(options));
        SeaTunnelRow rowWithNullHost = row();
        rowWithNullHost.setField(2, null);

        Assertions.assertTrue(
                serializer.serialize(rowWithNullHost).contains("\"host\":\"static-host\""));
    }

    @Test
    void timestampTimeFieldIsConvertedToEpochSecondsWithMillisPrecision() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("time_field", "event_time");

        // 2026-08-17T12:30:45.123 read as UTC == 1786969845.123 epoch seconds
        Assertions.assertEquals(
                1786969845.123d, serialize(options).get("time").asDouble(), 0.0005d);
    }

    @Test
    void bigintTimeFieldIsInterpretedAsEpochMillis() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("time_field", "epoch_millis");

        Assertions.assertEquals(
                1755432645.123d, serialize(options).get("time").asDouble(), 0.0005d);
    }

    @Test
    void timeIsWrittenInPlainNotation() {
        Map<String, Object> options = new HashMap<>();
        options.put("time_field", "event_time");

        SplunkEventSerializer serializer = new SplunkEventSerializer(ROW_TYPE, configOf(options));
        String event = serializer.serialize(row());

        // Splunk rejects a scientific-notation timestamp such as 1.786969845123E9.
        Assertions.assertTrue(event.contains("\"time\":1786969845.123"), event);
    }

    @Test
    void timeIsOmittedWhenTheRowValueIsNull() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("time_field", "event_time");

        SplunkEventSerializer serializer = new SplunkEventSerializer(ROW_TYPE, configOf(options));
        SeaTunnelRow rowWithNullTime = row();
        rowWithNullTime.setField(3, null);

        Assertions.assertFalse(MAPPER.readTree(serializer.serialize(rowWithNullTime)).has("time"));
    }

    @Test
    void unknownHostFieldFailsWithAvailableFields() {
        Map<String, Object> options = new HashMap<>();
        options.put("host_field", "does_not_exist");

        SplunkConnectorException exception =
                Assertions.assertThrows(
                        SplunkConnectorException.class,
                        () -> new SplunkEventSerializer(ROW_TYPE, configOf(options)));
        Assertions.assertTrue(
                exception.getMessage().contains("does not exist upstream"), exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains("hostname"), exception.getMessage());
    }

    @Test
    void unsupportedTimeFieldTypeFails() {
        Map<String, Object> options = new HashMap<>();
        options.put("time_field", "message");

        SplunkConnectorException exception =
                Assertions.assertThrows(
                        SplunkConnectorException.class,
                        () -> new SplunkEventSerializer(ROW_TYPE, configOf(options)));
        Assertions.assertTrue(
                exception.getMessage().contains("cannot be used as a Splunk event timestamp"),
                exception.getMessage());
    }

    @Test
    void consecutiveRowsDoNotLeakStateBetweenEnvelopes() throws Exception {
        SplunkEventSerializer serializer =
                new SplunkEventSerializer(ROW_TYPE, configOf(new HashMap<>()));

        SeaTunnelRow second = row();
        second.setField(0, 2L);
        second.setField(1, "second event");

        JsonNode first = MAPPER.readTree(serializer.serialize(row()));
        JsonNode secondEnvelope = MAPPER.readTree(serializer.serialize(second));

        Assertions.assertEquals(1L, first.get("event").get("id").asLong());
        Assertions.assertEquals("hello splunk", first.get("event").get("message").asText());
        Assertions.assertEquals(2L, secondEnvelope.get("event").get("id").asLong());
        Assertions.assertEquals(
                "second event", secondEnvelope.get("event").get("message").asText());
    }
}
