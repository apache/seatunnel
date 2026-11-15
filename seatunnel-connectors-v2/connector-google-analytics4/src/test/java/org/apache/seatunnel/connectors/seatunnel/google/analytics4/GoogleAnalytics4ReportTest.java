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

package org.apache.seatunnel.connectors.seatunnel.google.analytics4;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

class GoogleAnalytics4ReportTest {
    static final ObjectMapper JSON = new ObjectMapper();

    static Map<String, Object> options() {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("property_id", "123");
        options.put("start_date", "2024-01-01");
        options.put("end_date", "2024-01-31");
        options.put("dimensions", Collections.singletonList("country"));
        options.put("metrics", Arrays.asList("activeUsers", "purchaseRevenue"));
        options.put("metric_types", Arrays.asList("TYPE_INTEGER", "TYPE_CURRENCY"));
        options.put("emulator_url", "http://localhost:12345");
        options.put("page_size", 2);
        options.put("max_retries", 0);
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("country", "string");
        fields.put("activeUsers", "bigint");
        fields.put("purchaseRevenue", "double");
        options.put("schema", Collections.singletonMap("fields", fields));
        return options;
    }

    static GoogleAnalytics4Config config() {
        return new GoogleAnalytics4Config(ReadonlyConfig.fromMap(options()));
    }

    static ObjectNode page(int count, String... countries) {
        ObjectNode page = JSON.createObjectNode();
        page.putArray("dimensionHeaders").addObject().put("name", "country");
        page.putArray("metricHeaders")
                .addObject()
                .put("name", "activeUsers")
                .put("type", "TYPE_INTEGER");
        page.withArray("metricHeaders")
                .addObject()
                .put("name", "purchaseRevenue")
                .put("type", "TYPE_CURRENCY");
        page.put("rowCount", count);
        for (String country : countries) {
            ObjectNode row = page.withArray("rows").addObject();
            row.putArray("dimensionValues").addObject().put("value", country);
            row.putArray("metricValues").addObject().put("value", "9223372036854775807");
            row.withArray("metricValues").addObject().put("value", "1.25");
        }
        return page;
    }

    static byte[] bytes(ObjectNode node) {
        return node.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void typedPagesAndDeterministicRequest() throws Exception {
        GoogleAnalytics4Report report = new GoogleAnalytics4Report(config());
        ObjectNode request = (ObjectNode) JSON.readTree(report.request(2));
        Assertions.assertEquals("2", request.path("offset").asText());
        Assertions.assertEquals(
                "ALPHANUMERIC",
                request.path("orderBys").get(0).path("dimension").path("orderType").asText());
        Assertions.assertEquals(
                "2024-01-01", request.path("dateRanges").get(0).path("startDate").asText());
        Assertions.assertTrue(request.path("returnPropertyQuota").asBoolean());
        GoogleAnalytics4Report.Page first = report.parse(bytes(page(3, "CA", "DE")), 0);
        Assertions.assertFalse(first.finished);
        Assertions.assertEquals(Long.MAX_VALUE, first.rows.get(0).getField(1));
        Assertions.assertEquals(1.25, first.rows.get(0).getField(2));
        GoogleAnalytics4Report.Page last = report.parse(bytes(page(3, "US")), 2);
        Assertions.assertTrue(last.finished);
        Assertions.assertEquals("US", last.rows.get(0).getField(0));
    }

    @Test
    void shortPageAdvancesByActualRows() throws Exception {
        GoogleAnalytics4Report report = new GoogleAnalytics4Report(config());
        Assertions.assertFalse(report.parse(bytes(page(2, "CA")), 0).finished);
        Assertions.assertTrue(report.parse(bytes(page(2, "US")), 1).finished);
    }

    @Test
    void acceptsEmptyReportAndOptionalMetadataOmission() throws Exception {
        ObjectNode response = page(0);
        response.remove("rowCount");
        Assertions.assertTrue(
                new GoogleAnalytics4Report(config()).parse(bytes(response), 0).finished);
    }

    @Test
    void acceptsExplicitEmptyAndLiteralOtherDimension() throws Exception {
        List<SeaTunnelRow> rows =
                new GoogleAnalytics4Report(config()).parse(bytes(page(2, "", "(other)")), 0).rows;
        Assertions.assertEquals("", rows.get(0).getField(0));
        Assertions.assertEquals("(other)", rows.get(1).getField(0));
    }

    @Test
    void rejectsChangedCountsRepeatedAndOutOfOrderPages() throws Exception {
        GoogleAnalytics4Report report = new GoogleAnalytics4Report(config());
        report.parse(bytes(page(3, "CA", "DE")), 0);
        Assertions.assertTrue(
                Assertions.assertThrows(
                                IOException.class, () -> report.parse(bytes(page(4, "US")), 2))
                        .getMessage()
                        .contains("rowCount"));
        Assertions.assertTrue(
                Assertions.assertThrows(
                                IOException.class, () -> report.parse(bytes(page(3, "DE")), 2))
                        .getMessage()
                        .contains("duplicate"));
        assertBad(page(2, "US", "CA"), "out-of-order");
    }

    @Test
    void rejectsCountsWidthsHeadersMissingValuesAndMalformedNumbers() {
        assertBad(page(3), "page length");
        assertBad(page(1, "CA", "US"), "page length");
        assertBad(page(3, "CA", "DE", "US"), "page length");
        ObjectNode p = page(1, "CA");
        p.put("rowCount", -1);
        assertBad(p, "rowCount");
        p = page(1, "CA");
        p.put("rowCount", "1");
        assertBad(p, "rowCount");
        p = page(1, "CA");
        ((ObjectNode) p.path("metricHeaders").get(0)).put("name", "unexpected");
        assertBad(p, "header");
        p = page(1, "CA");
        ((ObjectNode) p.path("metricHeaders").get(1)).put("type", "TYPE_FLOAT");
        assertBad(p, "metric type");
        p = page(1, "CA");
        ((ObjectNode) p.path("rows").get(0)).remove("metricValues");
        assertBad(p, "width");
        p = page(1, "CA");
        ((ObjectNode) p.path("rows").get(0).path("dimensionValues").get(0)).remove("value");
        assertBad(p, "dimension value");
        for (String value : Arrays.asList("1.2", "9223372036854775808", "secret-number")) {
            p = page(1, "CA");
            ((ObjectNode) p.path("rows").get(0).path("metricValues").get(0)).put("value", value);
            assertBad(p, "metric value");
        }
        for (String value : Arrays.asList("NaN", "Infinity", "1e309", "0x1.0p0")) {
            p = page(1, "CA");
            ((ObjectNode) p.path("rows").get(0).path("metricValues").get(1)).put("value", value);
            assertBad(p, "metric value");
        }
    }

    @Test
    void failsClosedForDetectedLimitationsNotAbsentMetadata() throws Exception {
        for (String flag : Arrays.asList("subjectToThresholding", "dataLossFromOtherRow")) {
            ObjectNode p = page(1, "CA");
            p.putObject("metadata").put(flag, true);
            assertBad(p, flag);
            p.with("metadata").put(flag, false);
            Assertions.assertTrue(new GoogleAnalytics4Report(config()).parse(bytes(p), 0).finished);
        }
        ObjectNode p = page(1, "CA");
        p.putObject("metadata")
                .putArray("samplingMetadatas")
                .addObject()
                .put("samplesReadCount", "1")
                .put("samplingSpaceSize", "2");
        assertBad(p, "sampled");
        p = page(1, "CA");
        p.putObject("metadata")
                .putObject("schemaRestrictionResponse")
                .putArray("activeMetricRestrictions")
                .addObject()
                .put("metricName", "purchaseRevenue");
        assertBad(p, "restricted");
        p = page(0);
        p.putObject("metadata").put("emptyReason", "private-report-detail");
        assertBad(p, "emptyReason");
    }

    @Test
    void rejectsExhaustedQuotaBeforeAnotherPageButAllowsCompletedReport() throws Exception {
        ObjectNode p = page(2, "CA");
        p.putObject("propertyQuota").putObject("tokensPerHour").put("remaining", 0);
        assertBad(p, "quota exhausted");
        p.put("rowCount", 1);
        Assertions.assertTrue(new GoogleAnalytics4Report(config()).parse(bytes(p), 0).finished);
    }

    @Test
    void rejectsInvalidJsonWithoutPayloadAndDuplicateFields() {
        for (String json :
                Arrays.asList("private-secret", "{\"rowCount\":1,\"rowCount\":2}", "{} {}")) {
            IOException error =
                    Assertions.assertThrows(
                            IOException.class,
                            () ->
                                    new GoogleAnalytics4Report(config())
                                            .parse(json.getBytes(StandardCharsets.UTF_8), 0));
            Assertions.assertNull(error.getCause());
            Assertions.assertFalse(error.getMessage().contains("private-secret"));
        }
    }

    @Test
    void comparesUnicodeCodePoints() {
        Assertions.assertTrue(
                GoogleAnalytics4Report.compare(
                                Collections.singletonList("a"), Collections.singletonList("ab"))
                        < 0);
        Assertions.assertTrue(
                GoogleAnalytics4Report.compare(
                                Collections.singletonList("ab"), Collections.singletonList("a"))
                        > 0);
        Assertions.assertEquals(
                0,
                GoogleAnalytics4Report.compare(
                        Collections.singletonList(""), Collections.singletonList("")));
        Assertions.assertTrue(
                GoogleAnalytics4Report.compare(
                                Collections.singletonList("\uE000"),
                                Collections.singletonList("\uD800\uDC00"))
                        < 0);
    }

    @Test
    void validatesOptionsBeforeIo() {
        invalid("property_id", "../123", "property_id");
        invalid("start_date", "today", "dates");
        invalid("start_date", "2024-02-30", "calendar");
        invalid("end_date", "2023-01-01", "start_date");
        invalid("dimensions", Arrays.asList("country", "country"), "unique");
        invalid("metrics", Collections.singletonList("country"), "overlap");
        invalid("metric_types", Arrays.asList("TYPE_INTEGER", "UNKNOWN"), "metric_types");
        invalid("page_size", 0, "page_size");
        invalid("max_report_rows", 0, "max_report_rows");
        invalid("max_response_bytes", 16777217, "max_response_bytes");
        invalid("max_retries", 6, "max_retries");
        invalid("request_timeout_ms", 0, "request_timeout_ms");
        invalid("emulator_url", "http://user:secret@localhost", "emulator_url");
        invalid("emulator_url", "http://analyticsdata.googleapis.com", "emulator_url");
        invalid("service_account_key_file", "/not/read", "exactly one");
        invalid(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("country", "string")),
                "schema");
    }

    @Test
    void factorySpiOptionRuleAndSourceSerialization() throws Exception {
        GoogleAnalytics4SourceFactory factory = new GoogleAnalytics4SourceFactory();
        ConfigValidator.of(ReadonlyConfig.fromMap(options())).validate(factory.optionRule());
        Map<String, Object> missingAuth = options();
        missingAuth.remove("emulator_url");
        Assertions.assertThrows(
                Exception.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(missingAuth))
                                .validate(factory.optionRule()));
        boolean found = false;
        for (Factory candidate : ServiceLoader.load(Factory.class)) {
            found |= candidate instanceof GoogleAnalytics4SourceFactory;
        }
        Assertions.assertTrue(found);
        GoogleAnalytics4Source source =
                new GoogleAnalytics4Source(ReadonlyConfig.fromMap(options()));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(source);
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            GoogleAnalytics4Source restored = (GoogleAnalytics4Source) in.readObject();
            Assertions.assertEquals(
                    source.getProducedCatalogTables().get(0).getSeaTunnelRowType(),
                    restored.getProducedCatalogTables().get(0).getSeaTunnelRowType());
        }
    }

    private void invalid(String key, Object value, String message) {
        Map<String, Object> options = options();
        options.put(key, value);
        IllegalArgumentException failure =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new GoogleAnalytics4Config(ReadonlyConfig.fromMap(options)));
        Assertions.assertTrue(failure.getMessage().contains(message), failure.getMessage());
    }

    private void assertBad(ObjectNode response, String message) {
        IOException error =
                Assertions.assertThrows(
                        IOException.class,
                        () -> new GoogleAnalytics4Report(config()).parse(bytes(response), 0));
        Assertions.assertTrue(error.getMessage().contains(message), error.getMessage());
        Assertions.assertNull(error.getCause());
        Assertions.assertFalse(error.getMessage().contains("private-report-detail"));
    }
}
