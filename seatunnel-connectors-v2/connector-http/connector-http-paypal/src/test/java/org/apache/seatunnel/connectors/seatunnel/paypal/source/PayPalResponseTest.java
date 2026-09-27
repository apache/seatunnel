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

package org.apache.seatunnel.connectors.seatunnel.paypal.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.multitable.MultiTableFailureHelper;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.common.constants.JobMode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PayPalResponseTest {
    @Test
    void permitsExactlyTenThousandWithConsistentFinalPage() {
        PayPalResponse response =
                new PayPalResponse(
                        json(page(5000, 10000, record("1", "USD") + "," + record("2", "USD"))),
                        config(),
                        5000);
        assertEquals(10000, response.totalItems);
        assertEquals(2, response.rows.size());
    }

    @Test
    void acceptsNativeMetadataAndNestedDagOptions() {
        Map<String, Object> values = options();
        values.put("metadata_datasource_id", "data-source");
        values.put("dag-parsing.mode", "SINGLENESS");
        assertNotNull(new PayPalConfig(ReadonlyConfig.fromMap(values)));
        values.remove("dag-parsing.mode");
        values.put("dag-parsing", java.util.Collections.singletonMap("mode", "SINGLENESS"));
        assertNotNull(new PayPalConfig(ReadonlyConfig.fromMap(values)));
    }

    @Test
    void acceptsEngineInjectedFailurePolicyWithoutMultiTableCapability() {
        ReadonlyConfig injected =
                MultiTableFailureHelper.withMultiTableFailurePolicy(
                        ReadonlyConfig.fromMap(options()),
                        ReadonlyConfig.fromMap(java.util.Collections.emptyMap()));
        assertNotNull(new PayPalSource(injected));
    }

    @Test
    void invalidOptionTypesNeverExposeValuesOrCauses() {
        for (String key :
                new String[] {
                    "page_size",
                    "max_retries",
                    "request_timeout_ms",
                    "retry_delay_ms",
                    "max_response_bytes",
                    "parallelism",
                    "mock_mode"
                }) {
            Map<String, Object> values = options();
            values.put(key, "DO-NOT-LOG-VALUE");
            RuntimeException error =
                    assertThrows(
                            RuntimeException.class,
                            () -> new PayPalSource(ReadonlyConfig.fromMap(values)));
            assertFalse(error.toString().contains("DO-NOT-LOG-VALUE"));
            assertNull(error.getCause());
        }
    }

    @Test
    void rejectsUnsupportedOptionsAndExecutionModes() {
        for (String key :
                new String[] {
                    "schema",
                    "fields",
                    "access_token",
                    "balance_affecting_records_only",
                    "unknown_option"
                }) {
            Map<String, Object> options = options();
            options.put(key, "value-not-to-log");
            assertThrows(
                    RuntimeException.class,
                    () -> new PayPalSource(ReadonlyConfig.fromMap(options)));
        }
        Map<String, Object> options = options();
        options.put("parallelism", 2);
        assertThrows(
                RuntimeException.class, () -> new PayPalSource(ReadonlyConfig.fromMap(options)));
        PayPalSource source = new PayPalSource(ReadonlyConfig.fromMap(options()));
        assertThrows(
                IllegalArgumentException.class,
                () -> source.setJobContext(new JobContext().setJobMode(JobMode.STREAMING)));
        source.setJobContext(new JobContext().setJobMode(JobMode.BATCH));
    }

    static Map<String, Object> options() {
        Map<String, Object> options = new HashMap<>();
        options.put("client_id", "mock-client");
        options.put("client_secret", "mock-secret");
        options.put("start_date", "2026-01-01T00:00:00Z");
        options.put("end_date", "2026-01-02T00:00:00Z");
        options.put("page_size", 2);
        return options;
    }

    static PayPalConfig config() {
        return new PayPalConfig(ReadonlyConfig.fromMap(options()));
    }

    static String record(String amount, String currency) {
        return "{\"transaction_info\":{\"transaction_id\":\"SAME-ID\",\"transaction_amount\":{\"value\":\""
                + amount
                + "\",\"currency_code\":\""
                + currency
                + "\"}}}";
    }

    static String page(int number, int total, String records) {
        return "{\"account_number\":\"ACCOUNT\",\"start_date\":\"2026-01-01T00:00:00Z\",\"end_date\":\"2026-01-02T00:00:00Z\",\"page\":"
                + number
                + ",\"total_items\":"
                + total
                + ",\"total_pages\":"
                + ((total + 1) / 2)
                + ",\"transaction_details\":["
                + records
                + "]}";
    }

    static JsonNode json(String value) {
        return PayPalResponse.parse(value.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void preservesDuplicateIdsAndExactCurrencyAmounts() {
        PayPalResponse response =
                new PayPalResponse(
                        json(page(1, 2, record("123", "JPY") + "," + record("-0.123", "TND"))),
                        config(),
                        1);
        assertEquals(2, response.rows.size());
        assertEquals(response.rows.get(0).getField(1), response.rows.get(1).getField(1));
        assertEquals(new BigDecimal("123.000000000"), response.rows.get(0).getField(6));
        assertEquals(new BigDecimal("-0.123000000"), response.rows.get(1).getField(6));
        assertNull(response.rows.get(0).getField(8));
    }

    @Test
    void optionalFieldsRemainNullAndRawRecordIsPreserved() {
        String record =
                "{\"transaction_info\":{\"transaction_id\":null,\"fee_amount\":null,\"paypal_reference_id\":null},\"payer_info\":{\"unknown\":\"kept\"}}";
        PayPalResponse response = new PayPalResponse(json(page(1, 1, record)), config(), 1);
        for (int index = 1; index < 10; index++) {
            assertNull(response.rows.get(0).getField(index));
        }
        assertEquals(json(record), json((String) response.rows.get(0).getField(10)));
    }

    @Test
    void acceptsEmptyResultsAndEquivalentOffsets() {
        String content =
                page(1, 0, "").replace("2026-01-01T00:00:00Z", "2026-01-01T01:00:00+01:00");
        assertTrue(new PayPalResponse(json(content), config(), 1).rows.isEmpty());
        assertTrue(
                new PayPalResponse(
                                json(content.replace("\"total_pages\":0", "\"total_pages\":1")),
                                config(),
                                1)
                        .rows.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "page",
                "total_items",
                "total_pages",
                "start_date",
                "end_date",
                "account_number",
                "transaction_details"
            })
    void rejectsMissingEnvelopeFields(String field) {
        ObjectNode root = (ObjectNode) json(page(1, 1, record("1", "USD")));
        root.remove(field);
        assertThrows(RuntimeException.class, () -> new PayPalResponse(root, config(), 1));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "\"transaction_info\":null",
                "\"transaction_info\":[]",
                "\"transaction_info\":{\"transaction_amount\":{}}",
                "\"transaction_info\":{\"transaction_status\":123}",
                "\"transaction_info\":{\"fee_amount\":{\"value\":1,\"currency_code\":\"USD\"}}",
                "\"transaction_info\":{\"transaction_initiation_date\":\"secret-malformed-date\"}"
            })
    void rejectsMalformedRecordsWithoutLeakingValues(String content) {
        RuntimeException error =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                new PayPalResponse(
                                        json(page(1, 1, "{" + content + "}")), config(), 1));
        assertFalse(error.toString().contains("secret-malformed-date"));
        assertNull(error.getCause());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {"1e3", "NaN", "0.0000000001", "123456789012345678901234567890", "1.0secret"})
    void refusesLossyOrInvalidMoney(String amount) {
        assertThrows(
                RuntimeException.class,
                () -> new PayPalResponse(json(page(1, 1, record(amount, "USD"))), config(), 1));
    }

    @Test
    void rejectsShortCoverageAndTruncationAndCap() {
        assertThrows(
                RuntimeException.class,
                () ->
                        new PayPalResponse(
                                json(
                                        page(1, 0, "")
                                                .replace(
                                                        "2026-01-02T00:00:00Z",
                                                        "2026-01-01T23:59:59Z")),
                                config(),
                                1));
        assertThrows(
                RuntimeException.class,
                () -> new PayPalResponse(json(page(1, 2, record("1", "USD"))), config(), 1));
        assertThrows(
                RuntimeException.class,
                () -> new PayPalResponse(json(page(1, 10000, "")), config(), 1));
        assertThrows(
                RuntimeException.class,
                () ->
                        new PayPalResponse(
                                json(
                                        page(1, 0, "")
                                                .replace(
                                                        "\"total_items\":0",
                                                        "\"total_items\":0.0")),
                                config(),
                                1));
    }

    @Test
    void detectsApiErrorsAndMalformedJson() {
        for (String error :
                new String[] {
                    "{\"name\":\"RESULTSET_TOO_LARGE\",\"message\":\"secret\"}",
                    "{\"details\":[{\"issue\":\"RESULTSET_TOO_LARGE\"}]}"
                }) {
            IllegalStateException exception =
                    assertThrows(
                            IllegalStateException.class,
                            () -> PayPalResponse.rejectError(json(error)));
            assertTrue(exception.getMessage().contains("narrow"));
            assertFalse(exception.toString().contains("secret"));
        }
        for (String malformed :
                new String[] {"{\"secret\":", "{}", "{} {}", "{\"a\":1,\"a\":2}", "[]", "null"}) {
            assertThrows(
                    RuntimeException.class, () -> new PayPalResponse(json(malformed), config(), 1));
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "2026-01-01",
                "today",
                "2026-02-30T00:00:00Z",
                "2026-01-01T00:00:00",
                "2026-01-01T00:00Z"
            })
    void requiresAbsoluteDatesWithSeconds(String value) {
        assertThrows(IllegalArgumentException.class, () -> PayPalConfig.date(value));
    }

    @Test
    void validatesConfigAndMockCredentialsBeforeHttp() {
        for (String key :
                new String[] {
                    "page_size",
                    "max_retries",
                    "retry_delay_ms",
                    "request_timeout_ms",
                    "max_response_bytes"
                }) {
            Map<String, Object> values = options();
            values.put(key, -1);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new PayPalConfig(ReadonlyConfig.fromMap(values)));
        }
        for (String endpoint :
                new String[] {
                    "http://localhost:8080",
                    "https://api-m.paypal.com/",
                    "https://secret@api-m.paypal.com",
                    "https://evil.example",
                    "https://api-m.paypal.com?secret=yes"
                }) {
            Map<String, Object> values = options();
            values.put("api_base_url", endpoint);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new PayPalConfig(ReadonlyConfig.fromMap(values)));
        }
        Map<String, Object> values = options();
        values.put("mock_mode", true);
        values.put("api_base_url", "http://localhost:8080");
        assertNotNull(new PayPalConfig(ReadonlyConfig.fromMap(values)));
        values.put("client_secret", "real-secret");
        assertThrows(
                IllegalArgumentException.class,
                () -> new PayPalConfig(ReadonlyConfig.fromMap(values)));
        values.put("client_secret", "mock-secret");
        values.put("client_id", "real-id");
        assertThrows(
                IllegalArgumentException.class,
                () -> new PayPalConfig(ReadonlyConfig.fromMap(values)));
    }

    @Test
    void validatesIntervalAndHasNoClockDependentAgeRestriction() {
        Map<String, Object> values = options();
        values.put("end_date", "2026-02-02T00:00:00Z");
        assertThrows(
                IllegalArgumentException.class,
                () -> new PayPalConfig(ReadonlyConfig.fromMap(values)));
        values.put("end_date", values.get("start_date"));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PayPalConfig(ReadonlyConfig.fromMap(values)));
        values.put("start_date", "2099-01-01T00:00:00Z");
        values.put("end_date", "2099-02-01T00:00:00Z");
        assertNotNull(new PayPalConfig(ReadonlyConfig.fromMap(values)));
    }

    @Test
    void factorySpiAndSourceSerialization() throws Exception {
        boolean found = false;
        for (Factory factory : ServiceLoader.load(Factory.class)) {
            if (factory.factoryIdentifier().equals("PayPal")) {
                found = true;
                assertTrue(factory instanceof PayPalSourceFactory);
            }
        }
        assertTrue(found);
        PayPalSource source = new PayPalSource(ReadonlyConfig.fromMap(options()));
        assertEquals(Boundedness.BOUNDED, source.getBoundedness());
        assertEquals(
                11,
                source.getProducedCatalogTables().get(0).getSeaTunnelRowType().getTotalFields());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(source);
        }
        assertTrue(bytes.size() > 0);
    }
}
