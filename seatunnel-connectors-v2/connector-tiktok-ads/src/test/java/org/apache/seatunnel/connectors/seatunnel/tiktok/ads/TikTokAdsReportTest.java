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

package org.apache.seatunnel.connectors.seatunnel.tiktok.ads;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.multitable.MultiTableFailureHelper;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.MultiTableCommonOptions;
import org.apache.seatunnel.api.options.MultiTableFailurePolicy;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TikTokAdsReportTest {
    static final String SECRET = "sensitive-test-token-never-output";

    static Map<String, Object> options() {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("token", SECRET);
        options.put("advertiser_id", "123456789");
        options.put("data_level", "AUCTION_AD");
        options.put("start_date", "2026-09-01");
        options.put("end_date", "2026-09-02");
        options.put("dimensions", Arrays.asList("ad_id", "stat_time_day"));
        options.put("metrics", Arrays.asList("spend", "impressions", "clicks"));
        options.put("page_size", 2);
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("clicks", "bigint");
        fields.put("spend", "decimal(38, 6)");
        fields.put("ad_id", "string");
        fields.put("stat_time_day", "string");
        fields.put("impressions", "bigint");
        options.put("schema", Collections.singletonMap("fields", fields));
        return options;
    }

    static String row(String id) {
        return "{\"dimensions\":{\"ad_id\":\""
                + id
                + "\",\"stat_time_day\":\"2026-09-01 00:00:00\"},\"metrics\":{"
                + "\"spend\":\"123456789012345678901234567890.123456\","
                + "\"impressions\":\"9223372036854775807\",\"clicks\":\"2\"}}";
    }

    static String page(int page, int total, String... rows) {
        return "{\"code\":0,\"data\":{\"page_info\":{\"page\":"
                + page
                + ",\"page_size\":2,\"total_number\":"
                + total
                + ",\"total_page\":"
                + ((total + 1) / 2)
                + "},\"list\":["
                + String.join(",", rows)
                + "]}}";
    }

    static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    static TikTokAdsReport.Page parse(String value) throws IOException {
        return TikTokAdsReport.parse(
                bytes(value),
                1,
                new TikTokAdsConfig(ReadonlyConfig.fromMap(options())),
                new HashSet<>());
    }

    @Test
    void preservesSchemaOrderDecimalPrecisionAndAdvertiserDayLabel() throws Exception {
        SeaTunnelRow result = parse(page(1, 1, row("100"))).rows.get(0);
        assertEquals(2L, result.getField(0));
        assertEquals(new BigDecimal("123456789012345678901234567890.123456"), result.getField(1));
        assertEquals("100", result.getField(2));
        assertEquals("2026-09-01 00:00:00", result.getField(3));
        assertEquals(Long.MAX_VALUE, result.getField(4));
    }

    @Test
    void acceptsEmptyReportWithoutInventingARow() throws Exception {
        assertTrue(parse(page(1, 0)).rows.isEmpty());
        assertTrue(
                parse(page(1, 0).replace("\"total_page\":0", "\"total_page\":1")).rows.isEmpty());
    }

    @Test
    void boundsJsonNestingWithoutExposingParserCause() {
        String nested =
                String.join("", Collections.nCopies(30, "["))
                        + "\"TOKEN\""
                        + String.join("", Collections.nCopies(30, "]"));
        IOException error = assertThrows(IOException.class, () -> parse(nested));
        assertNull(error.getCause());
        assertFalse(error.getMessage().contains("TOKEN"));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "{}",
                "null",
                "[]",
                "{\"code\":40100,\"message\":\"TOKEN\"}",
                "{\"code\":0,\"code\":0}",
                "{\"code\":\"0\"}",
                "{\"code\":0,\"data\":null}",
                "{broken TOKEN",
                "{\"code\":0} {}"
            })
    void rejectsInvalidEnvelopesWithoutLeakingResponse(String invalid) {
        IOException error =
                assertThrows(IOException.class, () -> parse(invalid.replace("TOKEN", SECRET)));
        assertFalse(error.toString().contains(SECRET));
        assertNull(error.getCause());
    }

    @ParameterizedTest
    @ValueSource(strings = {"page", "page_size", "total_number", "total_page"})
    void rejectsMissingPaginationFields(String key) {
        String valid = page(1, 1, row("100"));
        assertThrows(
                IOException.class, () -> parse(valid.replace("\"" + key + "\":", "\"missing\":")));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "\"page\":0",
                "\"page\":2",
                "\"page\":1.0",
                "\"page_size\":0",
                "\"page_size\":3",
                "\"total_number\":3",
                "\"total_number\":-1",
                "\"total_page\":2",
                "\"total_number\":2147483648"
            })
    void rejectsInconsistentCounts(String replacement) {
        String key = replacement.substring(0, replacement.indexOf(':') + 1);
        String valid = page(1, 1, row("100"));
        String changed =
                valid.replaceAll(
                        java.util.regex.Pattern.quote(key) + "[0-9]+",
                        java.util.regex.Matcher.quoteReplacement(replacement));
        assertThrows(IOException.class, () -> parse(changed));
    }

    @ParameterizedTest
    @ValueSource(strings = {"ad_id", "stat_time_day", "spend", "impressions", "clicks"})
    void rejectsMissingAndNullRequestedFields(String field) {
        String valid = page(1, 1, row("100"));
        assertThrows(
                IOException.class,
                () -> parse(valid.replace("\"" + field + "\":", "\"missing\":")));
        String changed = valid.replaceAll("\"" + field + "\":\"[^\"]*\"", "\"" + field + "\":null");
        assertThrows(IOException.class, () -> parse(changed));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "NaN",
                "1e1000000",
                "1.0000001",
                "10000000000000000000000000000000000",
                "-Infinity"
            })
    void rejectsUnrepresentableSpend(String amount) {
        assertThrows(
                IOException.class,
                () ->
                        parse(
                                page(
                                        1,
                                        1,
                                        row("100")
                                                .replace(
                                                        "123456789012345678901234567890.123456",
                                                        amount))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"9223372036854775808", "-1", "1.0", "NaN", ""})
    void rejectsInvalidCounts(String count) {
        assertThrows(
                IOException.class,
                () -> parse(page(1, 1, row("100").replace("9223372036854775807", count))));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "2026-08-31 00:00:00",
                "2026-09-31 00:00:00",
                "2026-09-01T00:00:00Z",
                "2026-09-01 01:00:00"
            })
    void rejectsInvalidReportDay(String day) {
        assertThrows(
                IOException.class,
                () -> parse(page(1, 1, row("100").replace("2026-09-01 00:00:00", day))));
    }

    @Test
    void rejectsRepeatedDimensionKeysAcrossPages() throws Exception {
        HashSet<List<String>> seen = new HashSet<>();
        TikTokAdsConfig config = new TikTokAdsConfig(ReadonlyConfig.fromMap(options()));
        TikTokAdsReport.parse(bytes(page(1, 3, row("100"), row("101"))), 1, config, seen);
        assertThrows(
                IOException.class,
                () -> TikTokAdsReport.parse(bytes(page(2, 3, row("100"))), 2, config, seen));
    }

    @Test
    void acceptsAdOnlyReportAndExplicitMetricSubset() {
        Map<String, Object> config = options();
        config.put("dimensions", Collections.singletonList("ad_id"));
        config.put("metrics", Collections.singletonList("clicks"));
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("ad_id", "string");
        fields.put("clicks", "bigint");
        config.put("schema", Collections.singletonMap("fields", fields));
        assertNotNull(new TikTokAdsSource(ReadonlyConfig.fromMap(config)));
    }

    @Test
    void validatesInclusiveDateRangeLimits() {
        Map<String, Object> config = options();
        config.put("start_date", "2026-01-01");
        config.put("end_date", "2026-01-30");
        assertNotNull(new TikTokAdsSource(ReadonlyConfig.fromMap(config)));
        config.put("end_date", "2026-01-31");
        assertTrue(
                assertThrows(
                                IllegalArgumentException.class,
                                () -> new TikTokAdsSource(ReadonlyConfig.fromMap(config)))
                        .getMessage()
                        .contains("30 inclusive"));
        config.put("dimensions", Collections.singletonList("ad_id"));
        config.put("metrics", Collections.singletonList("clicks"));
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("ad_id", "string");
        fields.put("clicks", "bigint");
        config.put("schema", Collections.singletonMap("fields", fields));
        config.put("end_date", "2026-12-31");
        assertNotNull(new TikTokAdsSource(ReadonlyConfig.fromMap(config)));
        config.put("end_date", "2027-01-01");
        assertThrows(
                IllegalArgumentException.class,
                () -> new TikTokAdsSource(ReadonlyConfig.fromMap(config)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"data_level", "dimensions", "metrics", "start_date", "token", "schema"})
    void rejectsMissingRequiredOptions(String key) {
        Map<String, Object> config = options();
        config.remove(key);
        assertThrows(
                IllegalArgumentException.class,
                () -> new TikTokAdsSource(ReadonlyConfig.fromMap(config)));
    }

    @Test
    void rejectsUnsupportedOptionsAndPreservesSafeDiagnostics() {
        Map<String, Object> config = options();
        config.put("data_level", "AUCTION_CAMPAIGN");
        assertTrue(
                assertThrows(
                                IllegalArgumentException.class,
                                () -> new TikTokAdsSource(ReadonlyConfig.fromMap(config)))
                        .getMessage()
                        .contains("data_level"));
        config.put("data_level", "AUCTION_AD");
        config.put("query_mode", "CHUNK");
        assertTrue(
                assertThrows(
                                IllegalArgumentException.class,
                                () -> new TikTokAdsSource(ReadonlyConfig.fromMap(config)))
                        .getMessage()
                        .contains("unsupported"));
    }

    @Test
    void rejectsSchemaMismatchAndDecimalRounding() {
        Map<String, Object> config = options();
        config.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("raw", "string")));
        assertTrue(
                assertThrows(
                                IllegalArgumentException.class,
                                () -> new TikTokAdsSource(ReadonlyConfig.fromMap(config)))
                        .getMessage()
                        .contains("schema"));
    }

    @Test
    void configMaskingUsesParsedNativeTokenKey() {
        Map<String, Object> parsed =
                ConfigFactory.parseString("source {TikTokAds {token = \"" + SECRET + "\"}}")
                        .root()
                        .unwrapped();
        Map<String, Object> masked =
                ConfigBuilder.configDesensitization(
                        parsed, ConfigShadeUtils.getLogDesensitizationOptions(null));
        assertFalse(masked.toString().contains(SECRET));
        assertTrue(masked.toString().contains("******"));
        Map<String, Object> invalid = options();
        invalid.put("page_size", SECRET);
        Throwable error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new TikTokAdsSource(ReadonlyConfig.fromMap(invalid)));
        assertFalse(error.toString().contains(SECRET));
        assertNull(error.getCause());
    }

    @Test
    void preventsRealTokenOnMockEndpointAndHeaderInjection() {
        Map<String, Object> config = options();
        config.put("mock_url", "http://127.0.0.1:1080");
        assertTrue(
                assertThrows(
                                IllegalArgumentException.class,
                                () -> new TikTokAdsSource(ReadonlyConfig.fromMap(config)))
                        .getMessage()
                        .contains("mock-token"));
        config.put("token", SECRET + "\r\nInjected: header");
        assertFalse(
                assertThrows(
                                IllegalArgumentException.class,
                                () -> new TikTokAdsSource(ReadonlyConfig.fromMap(config)))
                        .toString()
                        .contains(SECRET));
    }

    @Test
    void discoversFactoryAndSerializesSourceWithoutClient() throws Exception {
        List<Factory> factories = new ArrayList<>();
        ServiceLoader.load(Factory.class).forEach(factories::add);
        assertTrue(factories.stream().anyMatch(f -> f instanceof TikTokAdsSourceFactory));
        TikTokAdsSource source = new TikTokAdsSource(ReadonlyConfig.fromMap(options()));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(source);
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            TikTokAdsSource restored = (TikTokAdsSource) in.readObject();
            assertEquals(Boundedness.BOUNDED, restored.getBoundedness());
            assertEquals(
                    source.getProducedCatalogTables().get(0).getSeaTunnelRowType(),
                    restored.getProducedCatalogTables().get(0).getSeaTunnelRowType());
        }
        JobContext job = new JobContext();
        job.setJobMode(JobMode.STREAMING);
        assertThrows(IllegalArgumentException.class, () -> source.setJobContext(job));
        SourceReader.Context context = mock(SourceReader.Context.class);
        when(context.getIndexOfSubtask()).thenReturn(1);
        assertThrows(IllegalArgumentException.class, () -> source.createReader(context));
    }

    @Test
    void acceptsEngineInjectedFailurePolicyAndStillRejectsUnknownOptions() {
        ReadonlyConfig parsed = ReadonlyConfig.fromConfig(ConfigFactory.parseMap(options()));
        ReadonlyConfig injected =
                MultiTableFailureHelper.withMultiTableFailurePolicy(
                        parsed, ReadonlyConfig.fromConfig(ConfigFactory.empty()));
        assertEquals(
                MultiTableFailurePolicy.FAIL_FAST,
                injected.get(MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY));
        assertNotNull(new TikTokAdsSource(injected));
        Map<String, Object> invalid = new LinkedHashMap<>(injected.getSourceMap());
        invalid.put("unsupported_option", "value");
        assertThrows(
                IllegalArgumentException.class,
                () -> new TikTokAdsSource(ReadonlyConfig.fromMap(invalid)));
    }

    @Test
    void acceptsNativeCommonOptionsAndNestedDagParsingMode() {
        Map<String, Object> config = options();
        config.put("metadata_datasource_id", "metadata-source");
        config.put("dag-parsing", Collections.singletonMap("mode", "STATIC"));
        config.put("plugin_output", "report");
        assertNotNull(new TikTokAdsSource(ReadonlyConfig.fromMap(config)));
    }
}
