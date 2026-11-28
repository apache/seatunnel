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
package org.apache.seatunnel.e2e.connector.microsoftadvertising;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingRecoveryProbe;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises OAuth, all fixed reads and asynchronous files through the real Zeta engine.
 *
 * <p>The independent HTTPS peer checks the complete REST request before replying. Assertions read
 * committed transactional JSON files, including raw provider values and composite record identity.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MicrosoftAdvertisingIT extends TestSuiteBase {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final ZoneId LONDON = ZoneId.of("Europe/London");
    private static final LocalDate DAY = LocalDate.now(LONDON).minusDays(2);
    private static final String USER = "9007199254740997";
    private static final String CAMPAIGN_ID = "9007199254740999";
    private static final String GROUP_ID = "9007199254741001";
    private static final String AD_ID = "9007199254741003";
    private static final String USER_PATH = "/CustomerManagement/v13/User/Query";
    private static final String SEARCH = "/CustomerManagement/v13/Accounts/Search";
    private static final String SUBMIT = "/Reporting/v13/GenerateReport/Submit";
    private static final String POLL = "/Reporting/v13/GenerateReport/Poll";
    private static final String BULK_SUBMIT = "/Bulk/v13/Campaigns/DownloadByAccountIds";
    private static final String BULK_POLL = "/Bulk/v13/BulkDownloadStatus/Query";
    private static final String EXTRA =
            ",\"extra\":{\"unicode\":\"数据🌏\",\"items\":[null,false,[1,2]],\"integer\":9223372036854775808,\"decimal\":123456789012345678.123456789}";
    @TempDir static Path directory;
    private static String trust;
    private static String wrongTrust;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startsAnOwnedJava11EngineAndIndependentTlsPeer() throws Exception {
        trust = identity("trusted");
        wrongTrust = identity("untrusted");
        Files.writeString(directory.resolve("trusted.pem"), trust);
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::peer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void rejectsUnexpectedOrCredentialBearingRequests() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"), active);
        }
    }

    @AfterAll
    void stopsOnlyOwnedResources() throws Exception {
        try {
            if (engine != null) {
                try {
                    if (peer != null) {
                        engine.touch("stop");
                        success(peer.get(20, TimeUnit.SECONDS));
                    }
                } finally {
                    engine.tearDown();
                }
            }
        } finally {
            tasks.shutdownNow();
        }
    }

    @Test
    void readsAll77ResourcesWithExactColumnsScopesAndLosslessFileValues() throws Exception {
        String name = "all-77";
        List<ObjectNode> steps = discovery(account(MicrosoftAdvertisingPeer.ACCOUNT));
        Map<String, JsonNode> expected = new LinkedHashMap<>();
        expect(
                expected,
                "accounts",
                tuple(MicrosoftAdvertisingPeer.ACCOUNT),
                account(MicrosoftAdvertisingPeer.ACCOUNT));
        steps.add(campaigns("{\"Campaigns\":[" + entity(CAMPAIGN_ID) + "]}"));
        expect(
                expected,
                "campaigns",
                tuple(CAMPAIGN_ID),
                enriched(
                        entity(CAMPAIGN_ID),
                        "AccountId",
                        MicrosoftAdvertisingPeer.ACCOUNT,
                        "CustomerId",
                        MicrosoftAdvertisingPeer.CUSTOMER_ID));
        steps.add(groups("{\"AdGroups\":[" + entity(GROUP_ID) + "]}"));
        expect(
                expected,
                "ad_groups",
                tuple(GROUP_ID),
                enriched(
                        entity(GROUP_ID),
                        "AccountId",
                        MicrosoftAdvertisingPeer.ACCOUNT,
                        "CustomerId",
                        MicrosoftAdvertisingPeer.CUSTOMER_ID,
                        "CampaignId",
                        CAMPAIGN_ID));
        steps.add(ads("{\"Ads\":[" + entity(AD_ID) + "]}"));
        expect(
                expected,
                "ads",
                tuple(AD_ID),
                enriched(
                        entity(AD_ID),
                        "AccountId",
                        MicrosoftAdvertisingPeer.ACCOUNT,
                        "CustomerId",
                        MicrosoftAdvertisingPeer.CUSTOMER_ID,
                        "AdGroupId",
                        GROUP_ID));
        int reportIndex = 0;
        for (Report report : reports()) {
            ObjectNode record = report.record();
            String id = "job " + reportIndex++;
            steps.add(submit(report, id));
            steps.add(poll(id, "Pending", null));
            steps.add(poll(id, "Success", download(report.name)));
            steps.add(file(report.name, "zip", report.csv()));
            expect(expected, report.name, report.key(record), record.toString());
        }
        String[] resources = {
            "labels",
            "ad_group_labels",
            "app_install_ad_labels",
            "app_install_ads",
            "keywords",
            "keyword_labels",
            "campaign_labels",
            "budget"
        };
        String[] entities = {
            "Labels",
            "AdGroupLabels",
            "AppInstallAdLabels",
            "AppInstallAds",
            "Keywords",
            "KeywordLabels",
            "CampaignLabels",
            "Budgets"
        };
        String[] types = {
            "Label",
            "Ad Group Label",
            "App Install Ad Label",
            "App Install Ad",
            "Keyword",
            "Keyword Label",
            "Campaign Label",
            "Budget"
        };
        for (int index = 0; index < resources.length; index++) {
            String resource = resources[index];
            steps.add(bulkSubmit(entities[index], "bulk " + index, null));
            steps.add(bulkPoll("bulk " + index, "InProgress", null));
            steps.add(bulkPoll("bulk " + index, "Completed", download(resource)));
            // Bulk file compression is detected from bytes, not a Content-Encoding header.
            steps.add(file(resource, index % 2 == 0 ? "gzip" : "plain", bulkCsv(types[index])));
            expect(
                    expected,
                    resource,
                    resource.endsWith("_labels")
                            ? tuple("9007199254741005", GROUP_ID)
                            : tuple("9007199254741005"),
                    bulkRecord(types[index]).toString());
        }
        program(name, steps);
        success(run("all", new Settings(name, "accounts")));
        assertEquals(77, expected.size());
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void followsParentsForAnAdsOnlySelectionWithoutEmittingUnselectedParents() throws Exception {
        String name = "ads-only";
        List<ObjectNode> steps = discovery(account(MicrosoftAdvertisingPeer.ACCOUNT));
        steps.add(campaigns("{\"Campaigns\":[" + entity(CAMPAIGN_ID) + "]}"));
        steps.add(groups("{\"AdGroups\":[" + entity(GROUP_ID) + "]}"));
        steps.add(ads("{\"Ads\":[" + entity(AD_ID) + "]}"));
        program(name, steps);
        success(run("one", new Settings(name, "ads")));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(
                expected,
                "ads",
                tuple(AD_ID),
                enriched(
                        entity(AD_ID),
                        "AccountId",
                        MicrosoftAdvertisingPeer.ACCOUNT,
                        "CustomerId",
                        MicrosoftAdvertisingPeer.CUSTOMER_ID,
                        "AdGroupId",
                        GROUP_ID));
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void deduplicatesOverlappingNamePredicatesUsingOneProviderPredicatePerSearch()
            throws Exception {
        String name = "overlapping-accounts";
        List<ObjectNode> steps = new ArrayList<>();
        steps.add(oauth(3600));
        steps.add(user(USER));
        steps.add(
                search(
                        0,
                        "AccountName",
                        "Contains",
                        "retail",
                        "[" + account(MicrosoftAdvertisingPeer.ACCOUNT) + "]"));
        steps.add(
                search(
                        0,
                        "AccountName",
                        "Equals",
                        "retail west",
                        "["
                                + account(MicrosoftAdvertisingPeer.ACCOUNT)
                                + ","
                                + account("9007199254741007")
                                + "]"));
        program(name, steps);
        success(run("predicates", new Settings(name, "accounts")));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(
                expected,
                "accounts",
                tuple(MicrosoftAdvertisingPeer.ACCOUNT),
                account(MicrosoftAdvertisingPeer.ACCOUNT));
        expect(expected, "accounts", tuple("9007199254741007"), account("9007199254741007"));
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void continuesAfterExactly1000AccountsAndKeepsNumericIdentifiersAboveDoublePrecision()
            throws Exception {
        String name = "account-page-boundary";
        List<String> accounts = new ArrayList<>();
        Map<String, JsonNode> expected = new HashMap<>();
        for (int index = 0; index < 1001; index++) {
            String id = Long.toString(9007199254740993L + index);
            accounts.add(account(id));
            expect(expected, "accounts", tuple(id), account(id));
        }
        List<ObjectNode> steps = new ArrayList<>();
        steps.add(oauth(3600));
        steps.add(user(USER));
        steps.add(
                search(
                        0,
                        "UserId",
                        "Equals",
                        USER,
                        "[" + String.join(",", accounts.subList(0, 1000)) + "]"));
        steps.add(search(1, "UserId", "Equals", USER, "[" + accounts.get(1000) + "]"));
        program(name, steps);
        success(run("one", new Settings(name, "accounts")));
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void treatsEmptyAccountsAndSuccessfulReportsWithoutDownloadUrlsAsEmptyData() throws Exception {
        String empty = "empty-accounts";
        List<ObjectNode> steps = discovery();
        steps.get(2).put("mode", "json-gzip");
        program(empty, steps);
        success(run("all", new Settings(empty, "accounts")));
        assertRows(empty, Collections.emptyMap());
        consumed(empty, steps.size());
        String name = "no-report-data";
        steps = discovery(account(MicrosoftAdvertisingPeer.ACCOUNT));
        Report report = accountReport();
        steps.add(submit(report, "no data"));
        steps.add(poll("no data", "Success", null));
        program(name, steps);
        success(run("one", new Settings(name, report.name)));
        assertRows(name, Collections.emptyMap());
        consumed(name, steps.size());
    }

    @Test
    void failsMalformedOrOversizedJsonAndKeepsProviderAuthenticationErrorsPrivate()
            throws Exception {
        String[] responses = {
            "[]",
            "{\"User\":{\"Id\":1},\"User\":{\"Id\":2}}",
            "{\"User\":{\"Id\":1}}{}",
            "{\"User\":{\"Id\":null}}",
            "{\"User\":{\"Id\":9223372036854775808}}"
        };
        for (int index = 0; index < responses.length; index++) {
            String name = "invalid-json-" + index;
            List<ObjectNode> steps =
                    List.of(oauth(3600), user(USER).put("response", responses[index]));
            program(name, steps);
            Settings settings = new Settings(name, "accounts");
            settings.retries = 0;
            failure(run("one", settings), "MicrosoftAdvertising");
            consumed(name, steps.size());
            assertRows(name, Collections.emptyMap());
        }
        String name = "json-byte-limit";
        List<ObjectNode> steps = discovery(account(MicrosoftAdvertisingPeer.ACCOUNT));
        program(name, steps);
        Settings settings = new Settings(name, "accounts");
        settings.json = 256;
        settings.retries = 0;
        failure(run("one", settings), "max_response_bytes");
        consumed(name, steps.size());
        assertRows(name, Collections.emptyMap());
        name = "tenant-mfa-rejection";
        steps =
                List.of(
                        oauth(3600)
                                .put("status", 400)
                                .put(
                                        "response",
                                        "{\"error\":\"invalid_grant\",\"error_codes\":[50078],\"error_description\":\"private-provider-message\"}"));
        program(name, steps);
        failure(run("one", new Settings(name, "accounts")), "interactive sign-in");
        consumed(name, steps.size());
        assertRows(name, Collections.emptyMap());
    }

    @Test
    void detectsRepeatedFullAccountPagesAndEnforcesAccountAndPageBudgets() throws Exception {
        List<String> rows = new ArrayList<>();
        for (int index = 0; index < 1000; index++) {
            rows.add(account(Long.toString(9007199254740993L + index)));
        }
        String page = "[" + String.join(",", rows) + "]";
        for (String mode : List.of("cycle", "page-budget", "account-budget")) {
            String name = "discovery-" + mode;
            List<ObjectNode> steps = new ArrayList<>();
            steps.add(oauth(3600));
            steps.add(user(USER));
            steps.add(search(0, "UserId", "Equals", USER, page));
            Settings settings = new Settings(name, "ads");
            if (mode.equals("cycle")) {
                steps.add(search(1, "UserId", "Equals", USER, page));
            } else if (mode.equals("page-budget")) {
                settings.pages = 1;
            } else {
                settings.accounts = 1;
            }
            program(name, steps);
            failure(
                    run("one", settings),
                    mode.equals("cycle")
                            ? "repeated a full page"
                            : mode.equals("page-budget") ? "max_account_pages" : "max_accounts");
            assertRows(name, Collections.emptyMap());
            consumed(name, steps.size());
        }
    }

    @Test
    void renewsAnExpiredAccessTokenOnceAndRetriesOnlyDefiniteRateLimitRejections()
            throws Exception {
        String name = "oauth-and-rate-limit";
        List<ObjectNode> steps = new ArrayList<>();
        steps.add(oauth(3600));
        ObjectNode denied =
                user(USER)
                        .put("status", 401)
                        .put("response", "{\"error\":\"private-provider-message\"}");
        steps.add(denied);
        steps.add(oauth(3600));
        steps.add(user(USER));
        ObjectNode limited =
                search(0, "UserId", "Equals", USER, "[]").put("status", 429).put("retry", "0");
        steps.add(limited);
        steps.add(
                search(
                        0,
                        "UserId",
                        "Equals",
                        USER,
                        "[" + account(MicrosoftAdvertisingPeer.ACCOUNT) + "]"));
        program(name, steps);
        success(run("one", new Settings(name, "accounts")));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(
                expected,
                "accounts",
                tuple(MicrosoftAdvertisingPeer.ACCOUNT),
                account(MicrosoftAdvertisingPeer.ACCOUNT));
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void refreshesAnExpiredSignedUrlByPollingTheSameJobWithoutCredentialsOnDownloads()
            throws Exception {
        String name = "expired-signed-url";
        Report report = accountReport();
        List<ObjectNode> steps = discovery(account(MicrosoftAdvertisingPeer.ACCOUNT));
        steps.add(submit(report, "opaque +/job=1"));
        steps.add(poll("opaque +/job=1", "Success", download("expired")));
        steps.add(file("expired", "plain", "private-provider-message").put("status", 403));
        steps.add(poll("opaque +/job=1", "Success", download("renewed")));
        steps.add(file("renewed", "zip", report.csv()));
        program(name, steps);
        success(run("one", new Settings(name, report.name)));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, report.name, report.key(report.record()), report.record().toString());
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void readsCustomSummaryDailyAndGroupedReportsWithDistinctNamesAndExplicitPeriodBounds()
            throws Exception {
        String name = "custom-report-modes";
        List<ObjectNode> steps = discovery(account(MicrosoftAdvertisingPeer.ACCOUNT));
        Map<String, JsonNode> expected = new LinkedHashMap<>();
        String[] names = {"custom_day", "Custom Exact", "custom_summary", "custom_hour"};
        String[] aggregations = {"DayOfWeek", "Daily", "Summary", "HourOfDay"};
        for (int index = 0; index < names.length; index++) {
            String aggregation = aggregations[index];
            boolean grouped = aggregation.equals("DayOfWeek") || aggregation.equals("HourOfDay");
            List<String> columns =
                    aggregation.equals("Summary")
                            ? List.of("AccountId", "Clicks")
                            : List.of("AccountId", "Clicks", "TimePeriod");
            Report report =
                    new Report(
                            new String[] {
                                names[index],
                                "AccountPerformanceReportRequest",
                                aggregation,
                                String.join(",", columns),
                                ""
                            });
            ObjectNode request = submit(report, "custom " + index);
            if (grouped) {
                ObjectNode reportBody = (ObjectNode) request.path("body").path("ReportRequest");
                reportBody.set(
                        "Time",
                        JSON.createObjectNode()
                                .put("PredefinedTime", "Yesterday")
                                .put(
                                        "ReportTimeZone",
                                        "GreenwichMeanTimeDublinEdinburghLisbonLondon"));
            }
            steps.add(request);
            steps.add(poll("custom " + index, "Success", download("custom-" + index)));
            List<String> values = new ArrayList<>(List.of(MicrosoftAdvertisingPeer.ACCOUNT, "42"));
            ObjectNode record =
                    JSON.createObjectNode()
                            .put("AccountId", MicrosoftAdvertisingPeer.ACCOUNT)
                            .put("Clicks", "42");
            if (!aggregation.equals("Summary")) {
                String value = grouped ? "1" : DAY.toString();
                values.add(value);
                record.put("TimePeriod", value);
            }
            if (grouped) {
                String yesterday = DAY.plusDays(1).toString();
                record.put(aggregation, "1")
                        .put("StartOfTimePeriod", yesterday)
                        .put("EndOfTimePeriod", yesterday)
                        .put("TimePeriod", yesterday);
            }
            steps.add(file("custom-" + index, "zip", csv(columns) + csv(values)));
            expect(expected, names[index], null, record.toString());
        }
        program(name, steps);
        success(run("custom", new Settings(name, "accounts")));
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void preservesNullReportValuesWhileKeepingAllIdentityDimensionsDistinct() throws Exception {
        String name = "report-identity-dimensions";
        Report report = accountReport();
        ObjectNode first = report.record();
        ObjectNode second = first.deepCopy();
        first.putNull("AccountId");
        second.putNull("AccountId");
        first.put("DeviceType", "Computer");
        second.put("DeviceType", "Smartphone");
        List<String> firstValues = new ArrayList<>();
        List<String> secondValues = new ArrayList<>();
        for (String column : report.columns) {
            firstValues.add(first.path(column).isNull() ? "" : first.path(column).asText());
            secondValues.add(second.path(column).isNull() ? "--" : second.path(column).asText());
        }
        program(
                name,
                reportFile(
                        report,
                        "identity",
                        "zip",
                        csv(report.columns) + csv(firstValues) + csv(secondValues)));
        success(run("one", new Settings(name, report.name)));
        Map<String, JsonNode> expected = new HashMap<>();
        // AccountId uses a zero only in the key. Raw JSON retains the original null.
        ObjectNode firstKey = first.deepCopy().put("AccountId", "0");
        ObjectNode secondKey = second.deepCopy().put("AccountId", "0");
        expect(expected, report.name, report.key(firstKey), first.toString());
        expect(expected, report.name, report.key(secondKey), second.toString());
        assertRows(name, expected);
        consumed(name, 6);
    }

    @Test
    void rejectsUnknownOrFailedJobsAndNeverRetriesAnUncertainSubmission() throws Exception {
        Report report = accountReport();
        String[] outcomes = {
            "Error", "UnexpectedState", "submit-503", "missing-job-id", "account-denied"
        };
        for (String outcome : outcomes) {
            String name = "job-failure-" + outcome;
            List<ObjectNode> steps = discovery(account(MicrosoftAdvertisingPeer.ACCOUNT));
            ObjectNode submit = submit(report, "failed job");
            if (outcome.equals("submit-503")) {
                submit.put("status", 503)
                        .put("response", "{\"error\":\"private-provider-message\"}");
            } else if (outcome.equals("missing-job-id")) {
                submit.put("response", "{}");
            } else if (outcome.equals("account-denied")) {
                submit.put("status", 400)
                        .put(
                                "response",
                                "{\"OperationErrors\":[{\"Code\":2003,\"Message\":\"private-provider-message\"}]}");
            }
            steps.add(submit);
            if (outcome.equals("Error") || outcome.equals("UnexpectedState")) {
                steps.add(poll("failed job", outcome, null));
            }
            program(name, steps);
            failure(run("one", new Settings(name, report.name)), "MicrosoftAdvertising");
            assertRows(name, Collections.emptyMap());
            consumed(name, steps.size());
        }
    }

    @Test
    void distinguishesBulkFullSyncRequiredAndDoesNotTurnItIntoAnEmptySuccess() throws Exception {
        for (String state : List.of("Failed", "FailedFullSyncRequired", "Unknown")) {
            String name = "bulk-status-" + state;
            List<ObjectNode> steps = discovery(account(MicrosoftAdvertisingPeer.ACCOUNT));
            steps.add(bulkSubmit("Labels", "bulk failure", null));
            steps.add(bulkPoll("bulk failure", state, null));
            program(name, steps);
            failure(run("one", new Settings(name, "labels")), "MicrosoftAdvertising");
            consumed(name, steps.size());
            assertRows(name, Collections.emptyMap());
        }
    }

    @Test
    void retriesAnExplicitDeltaFullSyncRequiredWithOnlyOneNewFullSubmission() throws Exception {
        String name = "bulk-confirmed-full-fallback";
        String delta = Instant.now().minusSeconds(86400).truncatedTo(ChronoUnit.MILLIS).toString();
        List<ObjectNode> steps = discovery(account(MicrosoftAdvertisingPeer.ACCOUNT));
        steps.add(bulkSubmit("Labels", "delta job", delta));
        steps.add(bulkPoll("delta job", "FailedFullSyncRequired", null));
        steps.add(bulkSubmit("Labels", "full job", null));
        steps.add(bulkPoll("full job", "Completed", download("full-fallback")));
        steps.add(file("full-fallback", "gzip", bulkCsv("Label")));
        program(name, steps);
        Settings settings = new Settings(name, "labels");
        settings.bulkFrom = delta;
        success(run("one", settings));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, "labels", tuple("9007199254741005"), bulkRecord("Label").toString());
        assertRows(name, expected);
        consumed(name, steps.size());
    }

    @Test
    void rejectsMalformedOrOversizedFilesBeforeCommittingPartialRows() throws Exception {
        Report report = accountReport();
        String[] modes = {
            "duplicate-header",
            "extra-cell",
            "unterminated-quote",
            "two-zip",
            "corrupt-zip",
            "expanded-limit",
            "compressed-limit",
            "row-limit",
            "column-limit",
            "record-limit"
        };
        for (String mode : modes) {
            String name = "file-" + mode;
            String csv = report.csv();
            Settings settings = new Settings(name, report.name);
            if (mode.equals("duplicate-header")) {
                csv = "AccountId,AccountId\n1,2\n";
            } else if (mode.equals("extra-cell")) {
                csv = csv.stripTrailing() + ",unexpected\n";
            } else if (mode.equals("unterminated-quote")) {
                csv = String.join(",", report.columns) + "\n\"unterminated\n";
            } else if (mode.equals("expanded-limit")) {
                settings.expanded = 256;
            } else if (mode.equals("compressed-limit")) {
                settings.compressed = 256;
            } else if (mode.equals("row-limit")) {
                settings.row = 64;
            } else if (mode.equals("column-limit")) {
                settings.columns = 1;
            } else if (mode.equals("record-limit")) {
                settings.rows = 1;
                csv = report.csv() + report.csv().substring(report.csv().indexOf('\n') + 1);
            }
            List<ObjectNode> steps =
                    reportFile(
                            report,
                            "file failure",
                            mode.equals("two-zip") || mode.equals("corrupt-zip") ? mode : "zip",
                            csv);
            program(name, steps);
            failure(run("one", settings), "MicrosoftAdvertising");
            assertRows(name, Collections.emptyMap());
            consumed(name, steps.size());
        }
    }

    @Test
    void enforcesTlsAndRefusesRedirectsBeforeSendingCredentialsToAnotherHost() throws Exception {
        String name = "wrong-anchor";
        program(name, List.of());
        Settings options = new Settings(name, "accounts");
        options.trust = wrongTrust;
        failure(run("one", options), "MicrosoftAdvertising");
        consumed(name, 0);
        name = "api-redirect";
        List<ObjectNode> steps = new ArrayList<>();
        steps.add(oauth(3600));
        steps.add(user(USER).put("status", 302).put("mode", "redirect"));
        program(name, steps);
        failure(run("one", new Settings(name, "accounts")), "MicrosoftAdvertising");
        consumed(name, steps.size());
    }

    @Test
    void enforcesWholeJsonReadDeadlineEvenWhenThePeerKeepsSendingBytes() throws Exception {
        String name = "trickling-discovery";
        List<ObjectNode> steps = new ArrayList<>();
        steps.add(oauth(3600));
        steps.add(user(USER).put("mode", "trickle"));
        program(name, steps);
        Settings options = new Settings(name, "accounts");
        options.requestTimeout = 800;
        options.retries = 0;
        failure(run("one", options), "MicrosoftAdvertising");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(name + ".disconnect"));
        long started = Long.parseLong(engine.lines(name + ".started").get(0));
        long closed = Long.parseLong(engine.lines(name + ".disconnect").get(0));
        assertTrue(closed - started < 7000, "deadline must close a trickling response");
        consumed(name, steps.size());
    }

    @Test
    void savesAndRestoresAConfirmedPendingJobWithoutSubmittingItTwice() throws Exception {
        String name = "pending-savepoint";
        Report report = accountReport();
        List<ObjectNode> steps = discovery(account(MicrosoftAdvertisingPeer.ACCOUNT));
        steps.add(submit(report, "confirmed savepoint job"));
        steps.add(poll("confirmed savepoint job", "Pending", null).put("mode", "partial"));
        program(name, steps);
        Settings settings = new Settings(name, report.name);
        settings.requestTimeout = 180000;
        settings.pollTimeout = 240000;
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () ->
                                engine.executeJob(
                                        "/microsoft-advertising-one.conf",
                                        job,
                                        settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".started"));
            long baseline = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(35, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > baseline);
            success(engine.savepointJob(job));
            running.get(30, TimeUnit.SECONDS);
        } finally {
            engine.touch(name + ".release");
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
        consumed(name, 5);
        String restored = "pending-restored";
        List<ObjectNode> resume =
                List.of(
                        oauth(3600),
                        user(USER),
                        poll("confirmed savepoint job", "Success", download("restored")),
                        file("restored", "zip", report.csv()));
        program(restored, resume);
        Settings resumedSettings = new Settings(name, report.name);
        resumedSettings.requestTimeout = settings.requestTimeout;
        resumedSettings.pollTimeout = settings.pollTimeout;
        success(
                engine.restoreJob(
                        "/microsoft-advertising-one.conf", job, resumedSettings.variables()));
        Map<String, JsonNode> expected = new HashMap<>();
        expect(expected, report.name, report.key(report.record()), report.record().toString());
        assertRows(name, expected);
        consumed(restored, resume.size());
    }

    @Test
    void closesATricklingSignedDownloadAtTheWholeDownloadDeadline() throws Exception {
        String name = "download-deadline";
        Report report = accountReport();
        List<ObjectNode> steps =
                reportFile(report, "download deadline", "zip-trickle", report.csv());
        program(name, steps);
        Settings settings = new Settings(name, report.name);
        settings.downloadTimeout = 800;
        settings.retries = 0;
        failure(run("one", settings), "MicrosoftAdvertising");
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> engine.exists(name + ".disconnect"));
        long started = Long.parseLong(engine.lines(name + ".started").get(0));
        long closed = Long.parseLong(engine.lines(name + ".disconnect").get(0));
        assertTrue(closed - started < 7000, "the file deadline must close a live response");
        assertRows(name, Collections.emptyMap());
        consumed(name, steps.size());
    }

    @Test
    void cancelsAnActiveFileDownloadWithoutPublishingAPartialReport() throws Exception {
        String name = "download-cancel";
        Report report = accountReport();
        List<ObjectNode> steps = reportFile(report, "download cancel", "zip-trickle", report.csv());
        program(name, steps);
        Settings settings = new Settings(name, report.name);
        settings.downloadTimeout = 180000;
        settings.retries = 0;
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(
                        () ->
                                engine.executeJob(
                                        "/microsoft-advertising-one.conf",
                                        job,
                                        settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".started"));
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".disconnect"));
            assertRows(name, Collections.emptyMap());
            consumed(name, steps.size());
        } finally {
            if (!running.isDone()) {
                engine.cancelJob(job);
                running.cancel(true);
            }
        }
    }

    @Test
    void restoresAnExactCsvRowRejectsChangedFilesAndChecksTheOAuthUserAgain() throws Exception {
        Report report =
                new Report(
                        new String[] {
                            "probe",
                            "AccountPerformanceReportRequest",
                            "Daily",
                            "AccountId,Clicks,TimePeriod",
                            ""
                        });
        String csv =
                "AccountId,Clicks,TimePeriod\n"
                        + MicrosoftAdvertisingPeer.ACCOUNT
                        + ",1,"
                        + DAY
                        + "\n"
                        + MicrosoftAdvertisingPeer.ACCOUNT
                        + ",2,"
                        + DAY
                        + "\n";
        for (String mode : List.of("resume", "changed-file", "user-change")) {
            String name = "reader-" + mode;
            List<ObjectNode> steps = reportFile(report, "reader confirmed job", "zip", csv);
            steps.add(oauth(3600));
            steps.add(user(mode.equals("user-change") ? "9007199254741111" : USER));
            if (!mode.equals("user-change")) {
                steps.add(poll("reader confirmed job", "Success", download("reader-replay")));
                steps.add(
                        file(
                                "reader-replay",
                                "zip",
                                mode.equals("changed-file") ? csv.replace(",2,", ",9,") : csv));
            }
            program(name, steps);
            success(engine.probe(mode));
            consumed(name, steps.size());
        }
        String zero = "reader-terminal-and-invalid";
        program(zero, List.of());
        success(engine.probe("zero"));
        consumed(zero, 0);
    }

    private static Report accountReport() throws Exception {
        for (Report report : reports()) {
            if (report.name.equals("account_performance_report_daily")) {
                return report;
            }
        }
        throw new IllegalStateException("Missing independent account report example");
    }

    private static List<Report> reports() throws IOException {
        try (InputStream input =
                MicrosoftAdvertisingIT.class.getResourceAsStream("/report-contracts.tsv")) {
            if (input == null) {
                throw new IOException("Missing independent report request examples");
            }
            List<Report> result = new ArrayList<>();
            for (String line :
                    new String(input.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                if (!line.isBlank() && !line.startsWith("#")) {
                    result.add(new Report(line.split("\t", -1)));
                }
            }
            assertEquals(65, result.size());
            return result;
        }
    }

    private static List<ObjectNode> discovery(String... accounts) throws Exception {
        List<ObjectNode> result = new ArrayList<>();
        result.add(oauth(3600));
        result.add(user(USER));
        result.add(search(0, "UserId", "Equals", USER, "[" + String.join(",", accounts) + "]"));
        return result;
    }

    private static ObjectNode oauth(int expires) {
        return step(
                MicrosoftAdvertisingPeer.AUTH,
                "/common/oauth2/v2.0/token",
                null,
                "{\"access_token\":\""
                        + MicrosoftAdvertisingPeer.TOKEN
                        + "\",\"token_type\":\"Bearer\",\"expires_in\":"
                        + expires
                        + "}");
    }

    private static ObjectNode user(String id) throws Exception {
        return step(
                MicrosoftAdvertisingPeer.CUSTOMER,
                USER_PATH,
                JSON.readTree("{\"UserId\":null}"),
                "{\"User\":{\"Id\":" + id + "}}");
    }

    private static ObjectNode search(
            int page, String field, String operator, String value, String accounts) {
        ObjectNode body = JSON.createObjectNode();
        body.putObject("PageInfo").put("Index", page).put("Size", 1000);
        body.putArray("Predicates")
                .addObject()
                .put("Field", field)
                .put("Operator", operator)
                .put("Value", value);
        body.putArray("Ordering").addObject().put("Field", "Id").put("Order", "Ascending");
        body.put("ReturnAdditionalFields", "TaxCertificate,AccountMode");
        return step(
                MicrosoftAdvertisingPeer.CUSTOMER, SEARCH, body, "{\"Accounts\":" + accounts + "}");
    }

    private static ObjectNode campaigns(String response) {
        ObjectNode body =
                JSON.createObjectNode()
                        .put("AccountId", MicrosoftAdvertisingPeer.ACCOUNT)
                        .put(
                                "CampaignType",
                                "App,Audience,DynamicSearchAds,Hotel,ObjectiveBased,PerformanceMax,Search,Shopping")
                        .put(
                                "ReturnAdditionalFields",
                                "AdScheduleUseSearcherTimeZone,BidStrategyId,CpvCpmBiddingScheme,DynamicDescriptionSetting,DynamicFeedSetting,MaxConversionValueBiddingScheme,MultimediaAdsBidAdjustment,TargetImpressionShareBiddingScheme,TargetSetting,VerifiedTrackingSetting");
        return scoped(
                MicrosoftAdvertisingPeer.CAMPAIGN,
                "/CampaignManagement/v13/Campaigns/QueryByAccountId",
                body,
                response);
    }

    private static ObjectNode groups(String response) {
        ObjectNode body =
                JSON.createObjectNode()
                        .put("CampaignId", CAMPAIGN_ID)
                        .put(
                                "ReturnAdditionalFields",
                                "AdGroupType,AdScheduleUseSearcherTimeZone,CpmBid,CpvBid,MultimediaAdsBidAdjustment");
        return scoped(
                MicrosoftAdvertisingPeer.CAMPAIGN,
                "/CampaignManagement/v13/AdGroups/QueryByCampaignId",
                body,
                response);
    }

    private static ObjectNode ads(String response) {
        ObjectNode body =
                JSON.createObjectNode()
                        .put("AdGroupId", GROUP_ID)
                        .put(
                                "ReturnAdditionalFields",
                                "ImpressionTrackingUrls,Videos,LongHeadlines");
        body.putArray("AdTypes")
                .add("AppInstall")
                .add("DynamicSearch")
                .add("ExpandedText")
                .add("Hotel")
                .add("Image")
                .add("Product")
                .add("ResponsiveAd")
                .add("ResponsiveSearch")
                .add("Text");
        return scoped(
                MicrosoftAdvertisingPeer.CAMPAIGN,
                "/CampaignManagement/v13/Ads/QueryByAdGroupId",
                body,
                response);
    }

    private static ObjectNode submit(Report report, String id) {
        ObjectNode body = JSON.createObjectNode();
        ObjectNode request = body.putObject("ReportRequest");
        request.put("Type", report.type)
                .put("Format", "Csv")
                .put("FormatVersion", "2.0")
                .put(
                        "ReportName",
                        report.type.substring(0, report.type.length() - "Request".length()))
                .put("ExcludeReportHeader", true)
                .put("ExcludeReportFooter", true)
                .put("ExcludeColumnHeaders", false)
                .put("ReturnOnlyCompleteData", false);
        if (!report.aggregation.equals("-")) {
            request.put("Aggregation", report.aggregation);
        }
        ArrayNode columns = request.putArray("Columns");
        report.columns.forEach(columns::add);
        request.putObject("Scope").putArray("AccountIds").add(MicrosoftAdvertisingPeer.ACCOUNT);
        ObjectNode time = request.putObject("Time");
        date(time.putObject("CustomDateRangeStart"), DAY);
        date(time.putObject("CustomDateRangeEnd"), DAY);
        time.put("ReportTimeZone", "GreenwichMeanTimeDublinEdinburghLisbonLondon");
        return scoped(
                MicrosoftAdvertisingPeer.REPORT,
                SUBMIT,
                body,
                "{\"ReportRequestId\":\"" + id + "\"}");
    }

    private static void date(ObjectNode value, LocalDate date) {
        value.put("Day", date.getDayOfMonth())
                .put("Month", date.getMonthValue())
                .put("Year", date.getYear());
    }

    private static ObjectNode poll(String id, String status, String url) {
        ObjectNode response = JSON.createObjectNode();
        ObjectNode state = response.putObject("ReportRequestStatus").put("Status", status);
        if (url != null) {
            state.put("ReportDownloadUrl", url);
        }
        return scoped(
                MicrosoftAdvertisingPeer.REPORT,
                POLL,
                JSON.createObjectNode().put("ReportRequestId", id),
                response.toString());
    }

    private static ObjectNode bulkSubmit(String entity, String id, String from) {
        ObjectNode body = JSON.createObjectNode();
        body.putArray("AccountIds").add(MicrosoftAdvertisingPeer.ACCOUNT);
        body.put("DataScope", "EntityData")
                .put("DownloadFileType", "Csv")
                .put("FormatVersion", "6.0")
                .put("CompressionType", "GZip");
        body.putArray("DownloadEntities").add(entity);
        if (from == null) {
            body.putNull("LastSyncTimeInUTC");
        } else {
            body.put("LastSyncTimeInUTC", from);
        }
        return scoped(
                MicrosoftAdvertisingPeer.BULK,
                BULK_SUBMIT,
                body,
                "{\"DownloadRequestId\":\"" + id + "\"}");
    }

    private static ObjectNode bulkPoll(String id, String status, String url) {
        ObjectNode response =
                JSON.createObjectNode()
                        .put("RequestStatus", status)
                        .put("PercentComplete", status.equals("Completed") ? 100 : 0);
        if (url != null) {
            response.put("ResultFileUrl", url);
        }
        return scoped(
                MicrosoftAdvertisingPeer.BULK,
                BULK_POLL,
                JSON.createObjectNode().put("RequestId", id),
                response.toString());
    }

    private static ObjectNode scoped(String host, String path, JsonNode body, String response) {
        return step(host, path, body, response)
                .put("account", MicrosoftAdvertisingPeer.ACCOUNT)
                .put("customer", MicrosoftAdvertisingPeer.CUSTOMER_ID);
    }

    private static ObjectNode step(String host, String path, JsonNode body, String response) {
        ObjectNode step =
                JSON.createObjectNode()
                        .put("host", host)
                        .put("path", path)
                        .put("method", "POST")
                        .put("status", 200)
                        .put("mode", "json")
                        .put("response", response);
        if (body != null) {
            step.set("body", body);
        }
        return step;
    }

    private static String download(String name) {
        return "https://"
                + MicrosoftAdvertisingPeer.DOWNLOAD
                + "/"
                + name
                + ".csv?sig=opaque%2B%2F%3D";
    }

    private static ObjectNode file(String name, String mode, String csv) {
        return step(MicrosoftAdvertisingPeer.DOWNLOAD, "/" + name + ".csv", null, csv)
                .put("method", "GET")
                .put("query", "sig=opaque%2B%2F%3D")
                .put("mode", mode);
    }

    private static List<ObjectNode> reportFile(Report report, String id, String mode, String csv)
            throws Exception {
        List<ObjectNode> steps = discovery(account(MicrosoftAdvertisingPeer.ACCOUNT));
        steps.add(submit(report, id));
        steps.add(poll(id, "Success", download("file")));
        steps.add(file("file", mode, csv));
        return steps;
    }

    private static String entity(String id) {
        return "{\"Id\":" + id + ",\"Name\":\"exact data\"" + EXTRA + "}";
    }

    private static String account(String id) {
        return "{\"Id\":"
                + id
                + ",\"ParentCustomerId\":"
                + MicrosoftAdvertisingPeer.CUSTOMER_ID
                + ",\"Name\":\"retail west\""
                + EXTRA
                + "}";
    }

    private static String enriched(String value, String... fields) throws Exception {
        ObjectNode record = (ObjectNode) JSON.readTree(value);
        for (int index = 0; index < fields.length; index += 2) {
            record.put(fields[index], fields[index + 1]);
        }
        return record.toString();
    }

    private static ObjectNode bulkRecord(String type) {
        return JSON.createObjectNode()
                .put("Type", type)
                .put("Status", "Deleted")
                .put("Id", "9007199254741005")
                .put("Parent Id", GROUP_ID)
                .put("Name", "quoted, \"value\"\n数据🌏")
                .put("Modified Time", "2024-04-27T18:00:14.970+00:00")
                .put("Account Id", MicrosoftAdvertisingPeer.ACCOUNT);
    }

    private static String bulkCsv(String type) {
        return "\ufeffType,Status,Id,Parent Id,Name,Modified Time\r\nFormat Version,,,,6.0,\r\n"
                + csv(
                        List.of(
                                type,
                                "Deleted",
                                "9007199254741005",
                                GROUP_ID,
                                "quoted, \"value\"\n数据🌏",
                                "04/27/2024 18:00:14.970"));
    }

    private static String csv(List<String> values) {
        List<String> cells = new ArrayList<>();
        for (String value : values) {
            cells.add("\"" + value.replace("\"", "\"\"") + "\"");
        }
        return String.join(",", cells) + "\r\n";
    }

    private static String tuple(String... values) {
        ArrayNode result = JSON.createArrayNode();
        for (String value : values) {
            result.add(value);
        }
        return result.toString();
    }

    private static void expect(
            Map<String, JsonNode> records, String resource, String key, String record)
            throws Exception {
        JsonNode parsed = JSON.readTree(record);
        ObjectNode row =
                JSON.createObjectNode()
                        .put("resource", resource)
                        .put(
                                "account_id",
                                resource.equals("accounts")
                                        ? parsed.path("Id").asText()
                                        : MicrosoftAdvertisingPeer.ACCOUNT)
                        .put("customer_id", MicrosoftAdvertisingPeer.CUSTOMER_ID);
        if (key == null) {
            row.putNull("record_key");
        } else {
            row.put("record_key", key);
        }
        row.set("record", parsed);
        assertNull(
                records.put(resource + "/" + key, row),
                "independent expected identity must be unique");
    }

    private void assertRows(String name, Map<String, JsonNode> expected) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            ObjectNode row = (ObjectNode) JSON.readTree(line);
            Set<String> fields = new HashSet<>();
            row.fieldNames().forEachRemaining(fields::add);
            assertEquals(
                    Set.of("resource", "account_id", "customer_id", "record_key", "record"),
                    fields);
            assertTrue(
                    row.path("record").isTextual(),
                    "raw provider JSON must remain a STRING column");
            row.set("record", JSON.readTree(row.path("record").asText()));
            String key = row.path("record_key").isNull() ? null : row.path("record_key").asText();
            assertNull(
                    actual.put(row.path("resource").asText() + "/" + key, row),
                    "duplicate committed row");
        }
        assertEquals(expected, actual);
    }

    private void program(String name, List<ObjectNode> steps) throws Exception {
        rejectsUnexpectedOrCredentialBearingRequests();
        ArrayNode script = JSON.createArrayNode();
        steps.forEach(script::add);
        Path file = Files.createTempFile(directory, "script-", ".json");
        Files.writeString(file, script.toString());
        engine.install(file, name + ".json");
        Path current = Files.createTempFile(directory, "active-", ".txt");
        Files.writeString(current, name);
        engine.install(current, "active");
        active = name;
    }

    private void consumed(String name, int count) throws Exception {
        assertFalse(engine.exists(name + ".invalid"), name);
        assertEquals(count, engine.lines(name + ".requests").size(), name);
    }

    private Container.ExecResult run(String kind, Settings settings) throws Exception {
        return engine.executeJob(
                "/microsoft-advertising-" + kind + ".conf", jobId(), settings.variables());
    }

    private static String jobId() {
        return Long.toString(
                ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L));
    }

    private static String text(Container.ExecResult result) {
        return result.getStdout() + result.getStderr();
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> text(result));
    }

    private static void failure(Container.ExecResult result, String reason) {
        assertNotEquals(0, result.getExitCode(), () -> text(result));
        assertTrue(text(result).contains(reason), () -> text(result));
        for (String secret :
                List.of(
                        MicrosoftAdvertisingPeer.TOKEN,
                        MicrosoftAdvertisingPeer.DEVELOPER,
                        MicrosoftAdvertisingPeer.SECRET,
                        MicrosoftAdvertisingPeer.REFRESH,
                        "private-provider-message",
                        "opaque%2B%2F%3D")) {
            assertFalse(text(result).contains(secret), "credential or signed URL leaked");
        }
    }

    private static String identity(String name) throws Exception {
        Path store = directory.resolve(name + ".p12");
        String hosts =
                String.join(
                        ",dns:",
                        List.of(
                                MicrosoftAdvertisingPeer.AUTH,
                                MicrosoftAdvertisingPeer.CUSTOMER,
                                MicrosoftAdvertisingPeer.CAMPAIGN,
                                MicrosoftAdvertisingPeer.REPORT,
                                MicrosoftAdvertisingPeer.BULK,
                                MicrosoftAdvertisingPeer.DOWNLOAD));
        Process process =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-alias",
                                "api",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "7",
                                "-dname",
                                "CN=" + MicrosoftAdvertisingPeer.CUSTOMER,
                                "-ext",
                                "SAN=dns:" + hosts,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                store.toString(),
                                "-storepass",
                                MicrosoftAdvertisingPeer.PASSWORD,
                                "-keypass",
                                MicrosoftAdvertisingPeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), output);
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keyStore.load(input, MicrosoftAdvertisingPeer.PASSWORD.toCharArray());
        }
        return Base64.getEncoder().encodeToString(keyStore.getCertificate("api").getEncoded());
    }

    /**
     * Independent reporting request example, CSV and expected lossless record.
     *
     * <p>Its values are provider wire examples, separate from production resource descriptors.
     */
    private static final class Report {
        private final String name;
        private final String type;
        private final String aggregation;
        private final List<String> columns;
        private final List<String> identity;

        private Report(String[] row) {
            name = row[0];
            type = row[1];
            aggregation = row[2];
            columns = List.of(row[3].split(","));
            identity =
                    row[4].isEmpty() || row[4].equals("-") ? List.of() : List.of(row[4].split(","));
        }

        private String value(String column) {
            if (column.equals("TimePeriod")) {
                LocalDate period = DAY;
                if (aggregation.equals("Weekly")) {
                    period = DAY.with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY));
                } else if (aggregation.equals("Monthly")) {
                    period = DAY.withDayOfMonth(1);
                }
                return period + (aggregation.equals("Hourly") ? "|02" : "");
            }
            if (column.equals("Date")) {
                return DAY.format(DateTimeFormatter.ofPattern("MM/dd/yyyy"));
            }
            if (column.endsWith("Id")) {
                return MicrosoftAdvertisingPeer.ACCOUNT;
            }
            if (column.equals("Clicks") || column.equals("Spend")) {
                return "123456789012345678.123456789";
            }
            if (column.equals("Revenue")) {
                return "--";
            }
            if (column.equals("Conversions")) {
                return "";
            }
            return "dimension, \"exact\"\n数据🌏";
        }

        private String csv() {
            List<String> values = new ArrayList<>();
            columns.forEach(column -> values.add(value(column)));
            return "\ufeff"
                    + MicrosoftAdvertisingIT.csv(columns)
                    + MicrosoftAdvertisingIT.csv(values);
        }

        private ObjectNode record() {
            ObjectNode record = JSON.createObjectNode();
            for (String column : columns) {
                String value = value(column);
                if (value.isEmpty() || value.equals("--")) {
                    record.putNull(column);
                } else if (column.equals("Date")) {
                    record.put(column, DAY.toString());
                } else if (column.equals("TimePeriod") && aggregation.equals("Hourly")) {
                    // ReportTimeZone chooses the relative period; downloaded report hours are UTC.
                    record.put(column, DAY + "T02:00:00+00:00");
                } else {
                    record.put(column, value);
                }
            }
            return record;
        }

        private String key(ObjectNode record) {
            if (identity.isEmpty()) {
                return null;
            }
            ArrayNode key = JSON.createArrayNode();
            identity.forEach(column -> key.add(record.path(column).asText("")));
            return key.toString();
        }
    }

    /**
     * Per-job limits are explicit so each failure crosses a single visible boundary.
     *
     * <p>All remaining budgets retain ordinary defaults during the chosen boundary test.
     */
    private static final class Settings {
        private final String name;
        private final String resource;
        private String trust = MicrosoftAdvertisingIT.trust;
        private String bulkFrom = "2000-01-01T00:00:00Z";
        private int retries = 2;
        private int requestTimeout = 30000;
        private int downloadTimeout = 30000;
        private int pollTimeout = 30000;
        private int json = 8388608;
        private int compressed = 8388608;
        private int expanded = 16777216;
        private int row = 262144;
        private int columns = 512;
        private int rows = 100000;
        private int accounts = 10000;
        private int pages = 10000;

        private Settings(String name, String resource) {
            this.name = name;
            this.resource = resource;
        }

        private String[] variables() {
            return new String[] {
                "api.output=" + name,
                "api.resource=" + resource,
                "api.developer=" + MicrosoftAdvertisingPeer.DEVELOPER,
                "api.client=" + MicrosoftAdvertisingPeer.CLIENT,
                "api.secret=" + MicrosoftAdvertisingPeer.SECRET,
                "api.refresh=" + MicrosoftAdvertisingPeer.REFRESH,
                "api.trust=" + trust,
                "api.start=" + DAY,
                "api.end=" + DAY,
                "api.bulk_from=" + bulkFrom,
                "api.retries=" + retries,
                "api.retry_delay=1000",
                "api.request_timeout=" + requestTimeout,
                "api.download_timeout=" + downloadTimeout,
                "api.poll_timeout=" + pollTimeout,
                "api.json_bytes=" + json,
                "api.compressed_bytes=" + compressed,
                "api.expanded_bytes=" + expanded,
                "api.csv_row=" + row,
                "api.csv_columns=" + columns,
                "api.csv_rows=" + rows,
                "api.accounts=" + accounts,
                "api.account_pages=" + pages
            };
        }
    }

    /**
     * Stages the independent peer and existing shaded JSON libraries in the test engine.
     *
     * <p>Host aliases and test certificates belong only to this owned container.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/microsoft-advertising-peer";
        private final Path files;

        private Engine(Path files) {
            this.files = files;
        }

        /**
         * Preserves reserved OAuth characters as individual argv values without command logging.
         */
        @Override
        protected Container.ExecResult executeCommand(
                GenericContainer<?> container, List<String> command)
                throws IOException, InterruptedException {
            return container.execInContainer(command.toArray(new String[0]));
        }

        @Override
        protected void executeExtraCommands(GenericContainer<?> container)
                throws IOException, InterruptedException {
            super.executeExtraCommands(container);
            for (String host :
                    List.of(
                            MicrosoftAdvertisingPeer.AUTH,
                            MicrosoftAdvertisingPeer.CUSTOMER,
                            MicrosoftAdvertisingPeer.CAMPAIGN,
                            MicrosoftAdvertisingPeer.REPORT,
                            MicrosoftAdvertisingPeer.BULK,
                            MicrosoftAdvertisingPeer.DOWNLOAD,
                            "credential-trap.invalid")) {
                container.withExtraHost(host, "127.0.0.1");
            }
            container.withCopyFileToContainer(
                    MountableFile.forHostPath(files.resolve("trusted.p12")), ROOT + "/trusted.p12");
            container.withCopyFileToContainer(
                    MountableFile.forHostPath(files.resolve("trusted.pem")), ROOT + "/trusted.pem");
            try {
                stageClasses(container, MicrosoftAdvertisingPeer.class);
                stageClasses(container, MicrosoftAdvertisingRecoveryProbe.class);
                Set<Path> libraries = new HashSet<>();
                for (Class<?> type :
                        List.of(ObjectMapper.class, JsonFactory.class, JsonProperty.class)) {
                    Path jar =
                            Path.of(
                                    type.getProtectionDomain()
                                            .getCodeSource()
                                            .getLocation()
                                            .toURI());
                    if (libraries.add(jar)) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(jar),
                                ROOT + "/libs/" + jar.getFileName());
                    }
                }
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the independent Java HTTPS peer", e);
            }
        }

        private static void stageClasses(GenericContainer<?> container, Class<?> type)
                throws IOException, URISyntaxException {
            Path parent = Path.of(type.getResource("").toURI());
            try (DirectoryStream<Path> files =
                    Files.newDirectoryStream(parent, type.getSimpleName() + "*.class")) {
                for (Path file : files) {
                    container.withCopyFileToContainer(
                            MountableFile.forHostPath(file),
                            ROOT
                                    + "/classes/"
                                    + type.getPackage().getName().replace('.', '/')
                                    + "/"
                                    + file.getFileName());
                }
            }
        }

        private Container.ExecResult peer() throws Exception {
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT + "/classes:" + ROOT + "/libs/*",
                    MicrosoftAdvertisingPeer.class.getName(),
                    ROOT);
        }

        private Container.ExecResult probe(String mode) throws Exception {
            ContainerUtil.copyAllConnectorJarToContainer(
                    server,
                    getConnectorModulePath(),
                    getConnectorNamePrefix(),
                    getConnectorType(),
                    SEATUNNEL_HOME);
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT
                            + "/classes:"
                            + ROOT
                            + "/libs/*:/tmp/seatunnel/lib/*:/tmp/seatunnel/connectors/*",
                    MicrosoftAdvertisingRecoveryProbe.class.getName(),
                    ROOT + "/trusted.pem",
                    mode,
                    DAY.toString());
        }

        private void install(Path file, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(file), ROOT + "/" + name);
        }

        private void touch(String name) throws Exception {
            success(server.execInContainer("touch", ROOT + "/" + name));
        }

        private boolean exists(String name) throws Exception {
            return server.execInContainer("test", "-f", ROOT + "/" + name).getExitCode() == 0;
        }

        private List<String> lines(String name) throws Exception {
            if (!exists(name)) {
                return Collections.emptyList();
            }
            Container.ExecResult result = server.execInContainer("cat", ROOT + "/" + name);
            success(result);
            return linesOf(result.getStdout());
        }

        private List<String> rows(String name) throws Exception {
            String path = "/tmp/seatunnel_mnt/microsoft-advertising-" + name;
            Container.ExecResult result =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d "
                                    + path
                                    + " ]; then find "
                                    + path
                                    + " -maxdepth 1 -type f -name 'receipt_*' -exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
            success(result);
            return linesOf(result.getStdout());
        }

        private static List<String> linesOf(String value) {
            List<String> lines = new ArrayList<>();
            for (String line : value.split("\\r?\\n")) {
                if (!line.isBlank()) {
                    lines.add(line);
                }
            }
            return lines;
        }
    }
}
