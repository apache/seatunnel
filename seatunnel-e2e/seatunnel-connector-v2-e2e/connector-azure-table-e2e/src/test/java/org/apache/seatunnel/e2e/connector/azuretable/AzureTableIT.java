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

package org.apache.seatunnel.e2e.connector.azuretable;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableRecoveryProbe;
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
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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
 * Runs provider-contract scenarios through the real Zeta engine and transactional JSON files.
 *
 * <p>The independent Java HTTPS peer verifies every path, query and credential header. Assertions
 * inspect committed rows rather than diagnostic console output or connector internals.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AzureTableIT extends TestSuiteBase {
    private static final String ONE = "/azure-table-one.conf";
    private static final String DISCOVERY = "/azure-table-discovery.conf";
    private static final String KEYSET = "/azure-table-keyset.conf";
    private static final String TERMINAL = "/azure-table-terminal.conf";
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    @TempDir static Path directory;
    private static String trust;
    private static String untrusted;
    private final ExecutorService tasks = Executors.newCachedThreadPool();
    private Engine engine;
    private Future<Container.ExecResult> peer;
    private String active;

    @BeforeAll
    void startEngineAndIndependentHttpsPeer() throws Exception {
        KeyStore trusted = identity("trusted");
        KeyStore other = identity("untrusted");
        untrusted = Base64.getEncoder().encodeToString(other.getCertificate("api").getEncoded());
        trust = Base64.getEncoder().encodeToString(trusted.getCertificate("api").getEncoded());
        String certificate =
                Base64.getMimeEncoder(64, new byte[] {'\n'})
                        .encodeToString(trusted.getCertificate("api").getEncoded());
        Files.writeString(
                directory.resolve("trusted.pem"),
                "-----BEGIN CERTIFICATE-----\n" + certificate + "\n-----END CERTIFICATE-----\n");
        engine = new Engine(directory);
        engine.startUp();
        peer = tasks.submit(engine::runPeer);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> engine.exists("ready"));
    }

    @AfterEach
    void rejectsUnexpectedProviderTraffic() throws Exception {
        if (active != null) {
            assertFalse(engine.exists(active + ".invalid"), active);
        }
    }

    @AfterAll
    void stopOwnedPeerAndEngine() throws Exception {
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
    void discoversEveryTableAndPreservesTypedEntitiesWithTheirOriginalMetadata() throws Exception {
        String name = "discovery-types",
                one = entity("partition", "row"),
                two = entity("partition", "row");
        program(
                name,
                List.of(
                        nextTable(ok("/Tables", top(2), values(table("Logs"))), "table+/=%opaque"),
                        ok(
                                "/Tables",
                                top(2) + "&NextTableName=table%2B%2F%3D%25opaque",
                                values(table("People"))),
                        nextEntity(ok("/Logs()", top(2), values(one)), "shared", "row"),
                        ok("/Logs()", top(2) + "&NextPartitionKey=shared&NextRowKey=row", values()),
                        nextEntity(ok("/People()", top(2), values(two)), "shared", "row"),
                        ok(
                                "/People()",
                                top(2) + "&NextPartitionKey=shared&NextRowKey=row",
                                values())));
        success(run(DISCOVERY, new Settings(name)));
        assertRows(
                name,
                Map.of(
                        key("Logs", "partition", "row"),
                        one,
                        key("People", "partition", "row"),
                        two));
        assertEquals(6, requests(name).size());
    }

    @Test
    void readsMoreThanOneThousandRowsSharingAPartitionWithoutSkippingRowKeys() throws Exception {
        String name = "same-partition";
        List<String> first = new ArrayList<>();
        Map<String, String> expected = new HashMap<>();
        for (int index = 0; index < 1001; index++) {
            String row = String.format(Locale.ROOT, "%04d", index), record = entity("same", row);
            expected.put(key("Logs", "same", row), record);
            if (index < 1000) {
                first.add(record);
            }
        }
        program(
                name,
                List.of(
                        nextEntity(
                                ok("/Logs()", top(1000), values(first.toArray(new String[0]))),
                                "1!opaque+/%=",
                                "1!row+/%="),
                        ok(
                                "/Logs()",
                                top(1000)
                                        + "&NextPartitionKey=1!opaque%2B%2F%25%3D&NextRowKey=1!row%2B%2F%25%3D",
                                values(entity("same", "1000")))));
        Settings settings = new Settings(name);
        settings.pageSize = 1000;
        success(run(ONE, settings));
        assertRows(name, expected);
        assertEquals(2, requests(name).size());
    }

    @Test
    void followsEmptyAndShortEntityPagesWithAnAbsentRowContinuation() throws Exception {
        String name = "empty-continuation", row = entity("one", "two");
        program(
                name,
                List.of(
                        nextEntity(ok("/Logs()", top(2), values()), "part-first", ""),
                        nextEntity(
                                ok("/Logs()", top(2) + "&NextPartitionKey=part-first", values(row)),
                                "part-next",
                                "null"),
                        ok(
                                "/Logs()",
                                top(2) + "&NextPartitionKey=part-next&NextRowKey=null",
                                values())));
        success(run(ONE, new Settings(name)));
        assertRows(name, Map.of(key("Logs", "one", "two"), row));
        assertEquals(3, requests(name).size());
    }

    @Test
    void continuesAnEmptyTableDiscoveryPageAndAcceptsAnEmptyAccount() throws Exception {
        String name = "empty-discovery";
        program(
                name,
                List.of(
                        nextTable(ok("/Tables", top(2), values()), "next"),
                        ok("/Tables", top(2) + "&NextTableName=next", values(table("Logs"))),
                        ok("/Logs()", top(2), values())));
        success(run(DISCOVERY, new Settings(name)));
        assertRows(name, Map.of());
        assertEquals(3, requests(name).size());
        name = "empty-account";
        program(name, List.of(ok("/Tables", top(2), values())));
        success(run(DISCOVERY, new Settings(name)));
        assertRows(name, Map.of());
        assertEquals(1, requests(name).size());
    }

    @Test
    void keepsTheEscapedCompositeKeyFilterOnEveryContinuationRequest() throws Exception {
        String name = "composite-keyset";
        Settings settings = new Settings(name);
        settings.partition = "P'o&+%=数据";
        settings.row = "r'o&+%=🌏";
        String query =
                top(2)
                        + "&$filter=(PartitionKey gt 'P''o&+%=数据') or (PartitionKey eq 'P''o&+%=数据' and RowKey gt 'r''o&+%=🌏')";
        String one = entity(settings.partition, settings.row + "1"),
                two = entity(settings.partition, settings.row + "2"),
                three = entity("Z", "after");
        program(
                name,
                List.of(
                        nextEntity(
                                ok("/Logs()", encodedQuery(query), values(one, two)),
                                "opaque",
                                "row"),
                        ok(
                                "/Logs()",
                                encodedQuery(query) + "&NextPartitionKey=opaque&NextRowKey=row",
                                values(three))));
        success(run(KEYSET, settings));
        assertRows(
                name,
                Map.of(
                        key("Logs", settings.partition, settings.row + "1"),
                        one,
                        key("Logs", settings.partition, settings.row + "2"),
                        two,
                        key("Logs", "Z", "after"),
                        three));
        assertEquals(2, requests(name).size());
    }

    @Test
    void preservesValidEmptyKeysAndNonFiniteTypedDoubleValues() throws Exception {
        String name = "empty-keys";
        String first = entity("", ""),
                second =
                        entity("", "next")
                                .replace(
                                        "\"DoubleValue\":1234.25",
                                        "\"DoubleValue@odata.type\":\"Edm.Double\",\"DoubleValue\":\"NaN\"");
        String third =
                entity("partition", "row")
                        .replace(
                                "\"DoubleValue\":1234.25",
                                "\"DoubleValue@odata.type\":\"Edm.Double\",\"DoubleValue\":\"Infinity\"");
        program(
                name,
                List.of(
                        nextEntity(ok("/Logs()", top(2), values(first, second)), "part", "row"),
                        ok(
                                "/Logs()",
                                top(2) + "&NextPartitionKey=part&NextRowKey=row",
                                values(third))));
        success(run(ONE, new Settings(name)));
        assertRows(
                name,
                Map.of(
                        key("Logs", "", ""),
                        first,
                        key("Logs", "", "next"),
                        second,
                        key("Logs", "partition", "row"),
                        third));
    }

    @Test
    void usesTheChinaEndpointWithoutChangingTheAccountInItsSignature() throws Exception {
        String name = "china-cloud",
                row = entity("one", "two").replace("\"odata.etag\":\"W/\\\"0x123\\\"\",", "");
        program(name, List.of(host(ok("/Logs()", top(2), values(row)), AzureTablePeer.CHINA_HOST)));
        Settings settings = new Settings(name);
        settings.suffix = "core.chinacloudapi.cn";
        success(run(ONE, settings));
        assertRows(name, Map.of(key("Logs", "one", "two"), row));
        assertEquals(1, requests(name).size());
    }

    @Test
    void neverConvertsPermissionOrMissingTableErrorsIntoSuccessfulEmptyReads() throws Exception {
        for (int status : List.of(400, 401, 403, 404)) {
            String name = "permanent-" + status;
            program(name, List.of(step("/Logs()", top(2), status, "fixed", "", privateError())));
            failure(run(ONE, new Settings(name)), "Azure");
            assertRows(name, Map.of());
            assertEquals(1, requests(name).size());
        }
        String name = "discovery-forbidden";
        program(name, List.of(step("/Tables", top(2), 403, "fixed", "", privateError())));
        failure(run(DISCOVERY, new Settings(name)), "Azure");
        assertRows(name, Map.of());
        assertEquals(1, requests(name).size());
    }

    @Test
    void respectsProviderThrottlingAndFailsAfterTheReadRetryBudget() throws Exception {
        String name = "rate-limit", row = entity("one", "two");
        program(
                name,
                List.of(
                        step("/Logs()", top(2), 429, "fixed", "1", privateError()),
                        ok("/Logs()", top(2), values(row))));
        success(run(ONE, new Settings(name)));
        assertRows(name, Map.of(key("Logs", "one", "two"), row));
        List<String> times = engine.lines(name + ".times");
        assertTrue(Long.parseLong(times.get(1)) - Long.parseLong(times.get(0)) >= 900);
        name = "retry-budget";
        program(
                name,
                List.of(
                        step("/Logs()", top(2), 503, "fixed", "0", privateError()),
                        step("/Logs()", top(2), 503, "fixed", "0", privateError())));
        Settings settings = new Settings(name);
        settings.retries = 1;
        failure(run(ONE, settings), "Azure");
        assertRows(name, Map.of());
        assertEquals(2, requests(name).size());
    }

    @Test
    void rejectsMalformedEntitiesAndUnsupportedTypedValuesBeforePublishingRows() throws Exception {
        String good = entity("one", "two");
        for (String body :
                List.of(
                        "{}",
                        "[]",
                        "null",
                        "{} trailing",
                        "{\"value\":[null]}",
                        values(good.replace("\"PartitionKey\":\"one\",", "")),
                        values(good.replace("\"RowKey\":\"two\"", "\"RowKey\":123")),
                        values(
                                good.replace(
                                        "\"Timestamp\":\"2024-01-01T00:00:00.1234567Z\"",
                                        "\"Timestamp\":\"invalid\"")),
                        values(
                                good.replace(
                                        "\"Int64Value\":\"9223372036854775807\"",
                                        "\"Int64Value\":\"9223372036854775808\"")),
                        values(good.replace("Edm.Int64", "Edm.Decimal")),
                        values(
                                good.replace(
                                        "\"RowKey\":\"two\"",
                                        "\"RowKey\":\"two\",\"RowKey\":\"other\"")))) {
            String name = "bad-entity-" + jobId();
            program(name, List.of(ok("/Logs()", top(2), body)));
            failure(run(ONE, new Settings(name)), "Azure");
            assertRows(name, Map.of());
            assertEquals(1, requests(name).size());
        }
    }

    @Test
    void failsOnRepeatedContinuationsRatherThanLoopingOrReturningDuplicateData() throws Exception {
        String name = "continuation-cycle";
        program(
                name,
                List.of(
                        nextEntity(ok("/Logs()", top(2), values()), "same", "same"),
                        nextEntity(
                                ok(
                                        "/Logs()",
                                        top(2) + "&NextPartitionKey=same&NextRowKey=same",
                                        values()),
                                "same",
                                "same")));
        failure(run(ONE, new Settings(name)), "Azure");
        assertRows(name, Map.of());
        assertEquals(2, requests(name).size());
    }

    @Test
    void failsRatherThanTruncatingTheConfiguredTableAndPageBudgets() throws Exception {
        String name = "table-budget";
        program(name, List.of(ok("/Tables", top(2), values(table("Logs"), table("People")))));
        Settings limited = new Settings(name);
        limited.maxTables = 1;
        failure(run(DISCOVERY, limited), "Azure");
        assertRows(name, Map.of());
        assertEquals(1, requests(name).size());
        name = "page-budget";
        program(name, List.of(nextEntity(ok("/Logs()", top(2), values()), "next", "")));
        limited = new Settings(name);
        limited.maxPages = 1;
        failure(run(ONE, limited), "Azure");
        assertRows(name, Map.of());
        assertEquals(1, requests(name).size());
    }

    @Test
    void boundsPlainAndCompressedBodiesAndRefusesRedirects() throws Exception {
        for (String mode : List.of("fixed", "gzip", "redirect")) {
            String name = "body-bound-" + mode;
            String record =
                    entity("partition", "row")
                            .replace("\"preserve\"", "\"" + "x".repeat(20000) + "\"");
            program(
                    name,
                    List.of(
                            step(
                                    "/Logs()",
                                    top(2),
                                    "redirect".equals(mode) ? 302 : 200,
                                    mode,
                                    "",
                                    values(record))));
            Settings limited = new Settings(name);
            limited.bytes = 8192;
            failure(run(ONE, limited), "Azure");
            assertRows(name, Map.of());
            assertEquals(1, requests(name).size());
        }
    }

    @Test
    void rejectsAnUntrustedStorageCertificateBeforeSendingSharedKeyAuthorization()
            throws Exception {
        String name = "untrusted-storage";
        program(name, List.of());
        Settings settings = new Settings(name);
        settings.certificate = untrusted;
        settings.retries = 0;
        failure(run(ONE, settings), "Azure");
        assertRows(name, Map.of());
        assertEquals(0, requests(name).size());
    }

    @Test
    void enforcesItsDeadlineWhileAStorageResponseKeepsSendingBytes() throws Exception {
        String name = "trickle-deadline";
        program(name, List.of(step("/Logs()", top(2), 200, "stream", "", "{}")));
        Settings settings = new Settings(name);
        settings.timeout = 1200;
        settings.deadline = 1600;
        settings.retries = 0;
        failure(run(ONE, settings), "Azure");
        awaitDisconnect(name);
        assertRows(name, Map.of());
        assertEquals(1, requests(name).size());
    }

    @Test
    void cancelsAnActiveStorageReadAndClosesItsSocket() throws Exception {
        String name = "cancel-read";
        program(name, List.of(step("/Logs()", top(2), 200, "stream", "", "{}")));
        Settings settings = held(name);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, settings.variables()));
        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.exists(name + ".started"));
            success(engine.cancelJob(job));
            running.get(30, TimeUnit.SECONDS);
            awaitDisconnect(name);
            assertRows(name, Map.of());
            assertEquals(1, requests(name).size());
        } finally {
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresBothOpaqueContinuationTokensWithoutReplayingCommittedRows() throws Exception {
        String name = "resume-entity", one = entity("same", "one"), two = entity("same", "two");
        String continued = top(2) + "&NextPartitionKey=opaque%2B%2F%3D&NextRowKey=row%2B%2F%3D";
        program(
                name,
                List.of(
                        nextEntity(ok("/Logs()", top(2), values(one)), "opaque+/=", "row+/="),
                        step("/Logs()", continued, 200, "hold", "", values(two)),
                        ok("/Logs()", continued, values(two))));
        Settings settings = held(name);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(ONE, job, settings.variables()));
        try {
            awaitRequests(name, 2);
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(ONE, job, settings.variables()));
            assertRows(
                    name, Map.of(key("Logs", "same", "one"), one, key("Logs", "same", "two"), two));
            assertEquals(3, requests(name).size());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresAnUnfinishedDiscoveryPageWithoutRestartingCompletedDiscovery() throws Exception {
        String name = "resume-discovery", row = entity("one", "two");
        String continued = top(2) + "&NextTableName=next";
        program(
                name,
                List.of(
                        nextTable(ok("/Tables", top(2), values(table("Logs"))), "next"),
                        step("/Tables", continued, 200, "hold", "", values(table("People"))),
                        ok("/Tables", continued, values(table("People"))),
                        ok("/Logs()", top(2), values(row)),
                        ok("/People()", top(2), values(row))));
        Settings settings = held(name);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(DISCOVERY, job, settings.variables()));
        try {
            awaitRequests(name, 2);
            savepoint(job, running);
            engine.touch(name + ".release");
            success(engine.restoreJob(DISCOVERY, job, settings.variables()));
            assertRows(
                    name, Map.of(key("Logs", "one", "two"), row, key("People", "one", "two"), row));
            assertEquals(5, requests(name).size());
        } finally {
            engine.touch(name + ".release");
            stopIfRunning(job, running);
        }
    }

    @Test
    void rejectsChangedAccountAndKeysetScopeBeforeMakingRestoredRequests() throws Exception {
        for (boolean account : List.of(false, true)) {
            String name = "restore-scope-" + account;
            Settings settings = held(name);
            String query =
                    top(2)
                            + "&$filter="
                            + encode(
                                    "(PartitionKey gt 'P''o&+%=data') or (PartitionKey eq 'P''o&+%=data' and RowKey gt 'r''o&+%=data')");
            program(name, List.of(step("/Logs()", query, 200, "hold", "", values())));
            String job = jobId();
            Future<Container.ExecResult> running =
                    tasks.submit(() -> engine.executeJob(KEYSET, job, settings.variables()));
            try {
                awaitRequests(name, 1);
                savepoint(job, running);
                engine.touch(name + ".release");
                if (account) {
                    settings.account = "otherfixtureaccount";
                } else {
                    settings.row = "changed";
                }
                failure(engine.restoreJob(KEYSET, job, settings.variables()), "checkpoint");
                assertRows(name, Map.of());
                assertEquals(1, requests(name).size());
            } finally {
                engine.touch(name + ".release");
                stopIfRunning(job, running);
            }
        }
    }

    @Test
    void restoresTerminalScansWithoutIssuingAnotherStorageRequest() throws Exception {
        String name = "resume-terminal", row = entity("one", "two");
        program(name, List.of(ok("/Logs()", top(2), values(row))));
        Settings settings = new Settings(name);
        String job = jobId();
        Future<Container.ExecResult> running =
                tasks.submit(() -> engine.executeJob(TERMINAL, job, settings.variables()));
        try {
            awaitRequests(name, 1);
            savepoint(job, running);
            Future<Container.ExecResult> restored =
                    tasks.submit(() -> engine.restoreJob(TERMINAL, job, settings.variables()));
            try {
                long baseline = engine.getCompletedCheckpointCount(job);
                Awaitility.await()
                        .atMost(35, TimeUnit.SECONDS)
                        .until(() -> engine.getCompletedCheckpointCount(job) > baseline + 1);
                assertRows(name, Map.of(key("Logs", "one", "two"), row));
                assertEquals(1, requests(name).size());
            } finally {
                stopIfRunning(job, restored);
            }
        } finally {
            stopIfRunning(job, running);
        }
    }

    @Test
    void restoresASerializedPageInternalRowPositionUsingPackagedArtifacts() throws Exception {
        String name = "probe-row";
        String page = probePage(false);
        program(name, List.of(ok("/Logs()", top(3), page), ok("/Logs()", top(3), page)));
        success(engine.runProbe("row"));
        assertEquals(2, requests(name).size());
    }

    @Test
    void rejectsAChangedPageBeforeAnUnverifiedRestoredRowCanEscape() throws Exception {
        String name = "probe-changed";
        program(
                name,
                List.of(
                        ok("/Logs()", top(3), probePage(false)),
                        ok("/Logs()", top(3), probePage(true))));
        success(engine.runProbe("changed"));
        assertEquals(2, requests(name).size());
    }

    @Test
    void keepsPreassignmentCancellationAndTerminalSnapshotsFreeOfStorageTraffic() throws Exception {
        String name = "probe-zero";
        program(name, List.of());
        success(engine.runProbe("zero"));
        assertEquals(0, requests(name).size());
    }

    private static String probePage(boolean changed) {
        String first = entity("partition", "one").replace("preserve", "private-entity-value-one");
        String second =
                entity("partition", "two")
                        .replace(
                                "preserve",
                                changed
                                        ? "changed-private-entity-value"
                                        : "private-entity-value-two");
        return values(first, second);
    }

    private static String entity(String partition, String row) {
        return "{\"PartitionKey\":\""
                + partition
                + "\",\"RowKey\":\""
                + row
                + "\",\"Timestamp\":\"2024-01-01T00:00:00.1234567Z\",\"odata.etag\":\"W/\\\"0x123\\\"\","
                + "\"Int64Value@odata.type\":\"Edm.Int64\",\"Int64Value\":\"9223372036854775807\","
                + "\"Int32Value\":2147483647,\"BooleanValue\":false,\"DoubleValue\":1234.25,"
                + "\"BinaryValue@odata.type\":\"Edm.Binary\",\"BinaryValue\":\"AAH+/w==\","
                + "\"GuidValue@odata.type\":\"Edm.Guid\",\"GuidValue\":\"4185404a-5818-48c3-b9be-f217df0dba6f\","
                + "\"DateValue@odata.type\":\"Edm.DateTime\",\"DateValue\":\"2024-01-01T00:00:00.1234567Z\","
                + "\"StringValue\":\"数据🌏\",\"UnknownProperty\":\"preserve\"}";
    }

    private static String table(String name) {
        return "{\"TableName\":\"" + name + "\"}";
    }

    private static String values(String... rows) {
        return "{\"value\":[" + String.join(",", rows) + "]}";
    }

    private static String top(int size) {
        return "$top=" + size;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String encodedQuery(String query) {
        int at = query.indexOf("&$filter=");
        return query.substring(0, at) + "&$filter=" + encode(query.substring(at + 9));
    }

    private static String key(String table, String partition, String row) {
        return table.length()
                + ":"
                + table
                + partition.length()
                + ":"
                + partition
                + row.length()
                + ":"
                + row;
    }

    private static Settings held(String name) {
        Settings settings = new Settings(name);
        settings.timeout = 180000;
        settings.deadline = 240000;
        return settings;
    }

    private static String privateError() {
        return "{\"odata.error\":{\"code\":\"AuthenticationFailed\",\"message\":{\"value\":\"private-provider-message\"}}}";
    }

    private static String ok(String path, String query, String body) {
        return step(path, query, 200, "fixed", "", body);
    }

    private static String step(
            String path, String query, int status, String mode, String retry, String body) {
        return String.join(
                "\t",
                path,
                query,
                Integer.toString(status),
                mode,
                retry,
                Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8)),
                "",
                "",
                "",
                AzureTablePeer.HOST);
    }

    private static String nextTable(String step, String token) {
        return with(step, 6, token);
    }

    private static String nextEntity(String step, String partition, String row) {
        return with(with(step, 7, partition), 8, row);
    }

    private static String host(String step, String value) {
        return with(step, 9, value);
    }

    private static String with(String step, int field, String value) {
        String[] fields = step.split("\t", -1);
        fields[field] = value;
        return String.join("\t", fields);
    }

    private void assertRows(String name, Map<String, String> expected) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        for (String line : engine.rows(name)) {
            JsonNode row = JSON.readTree(line);
            Set<String> fields = new HashSet<>();
            row.fieldNames().forEachRemaining(fields::add);
            assertEquals(
                    Set.of("table_name", "partition_key", "row_key", "timestamp", "etag", "record"),
                    fields);
            assertNull(
                    actual.put(
                            key(
                                    row.path("table_name").asText(),
                                    row.path("partition_key").asText(),
                                    row.path("row_key").asText()),
                            row),
                    "duplicate committed compound identity");
        }
        assertEquals(expected.keySet(), actual.keySet());
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            JsonNode row = actual.get(entry.getKey()), original = JSON.readTree(entry.getValue());
            assertEquals(original.path("Timestamp"), row.path("timestamp"));
            if (original.hasNonNull("odata.etag")) {
                assertEquals(original.path("odata.etag"), row.path("etag"));
            } else {
                assertTrue(row.path("etag").isNull());
            }
            assertEquals(original, JSON.readTree(row.path("record").asText()));
        }
    }

    private Container.ExecResult run(String config, Settings options) throws Exception {
        return engine.executeJob(config, jobId(), options.variables());
    }

    private void program(String name, List<String> lines) throws Exception {
        rejectsUnexpectedProviderTraffic();
        Path script = Files.createTempFile(directory, "steps-", ".txt");
        Files.write(script, lines, StandardCharsets.UTF_8);
        engine.install(script, name + ".program");
        Path current = Files.createTempFile(directory, "active-", ".txt");
        Files.writeString(current, name);
        engine.install(current, "active");
        active = name;
    }

    private List<String> requests(String name) throws Exception {
        return engine.lines(name + ".requests");
    }

    private void awaitRequests(String name, int count) {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> requests(name).size() == count);
    }

    private void awaitDisconnect(String name) {
        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .until(() -> engine.exists(name + ".disconnect"));
    }

    private void savepoint(String job, Future<Container.ExecResult> running) throws Exception {
        long baseline = engine.getCompletedCheckpointCount(job);
        Awaitility.await()
                .atMost(35, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(job) > baseline);
        success(engine.savepointJob(job));
        running.get(30, TimeUnit.SECONDS);
    }

    private void stopIfRunning(String job, Future<Container.ExecResult> running) throws Exception {
        if (!running.isDone()) {
            engine.cancelJob(job);
            running.cancel(true);
        }
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

    private static void failure(Container.ExecResult result, String fragment) {
        assertNotEquals(0, result.getExitCode(), () -> text(result));
        assertTrue(text(result).contains(fragment), () -> text(result));
        assertFalse(text(result).contains(AzureTablePeer.KEY));
        assertFalse(text(result).contains("SharedKey "));
        assertFalse(text(result).contains("private-provider-message"));
    }

    private static KeyStore identity(String name) throws Exception {
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Path file = directory.resolve(name + ".p12");
        Process process =
                new ProcessBuilder(
                                keytool,
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
                                "CN=" + AzureTablePeer.HOST,
                                "-ext",
                                "SAN=dns:"
                                        + AzureTablePeer.HOST
                                        + ",dns:"
                                        + AzureTablePeer.CHINA_HOST,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                AzureTablePeer.PASSWORD,
                                "-keypass",
                                AzureTablePeer.PASSWORD)
                        .redirectErrorStream(true)
                        .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), output);
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(file)) {
            store.load(input, AzureTablePeer.PASSWORD.toCharArray());
        }
        return store;
    }

    /**
     * Selects one isolated account, cloud suffix and read budget per job.
     *
     * <p>The real production endpoint rules are retained in the test container.
     */
    private static final class Settings {
        private final String name;
        private String suffix = "core.windows.net";
        private String certificate = trust;
        private String account = AzureTablePeer.ACCOUNT;
        private String partition = "P'o&+%=data";
        private String row = "r'o&+%=data";
        private int pageSize = 2;
        private int maxPages = 10000;
        private int maxTables = 10000;
        private int bytes = 4194304;
        private int retries = 2;
        private int timeout = 30000;
        private int deadline = 120000;

        private Settings(String name) {
            this.name = name;
        }

        private String[] variables() {
            return new String[] {
                "api.output=" + name, "api.account=" + account,
                "api.key=" + AzureTablePeer.KEY, "api.suffix=" + suffix,
                "api.trust=" + certificate, "api.partition=" + partition,
                "api.row=" + row, "api.page_size=" + pageSize,
                "api.max_pages=" + maxPages, "api.max_tables=" + maxTables,
                "api.max_bytes=" + bytes, "api.retries=" + retries,
                "api.timeout=" + timeout, "api.deadline=" + deadline
            };
        }
    }

    /**
     * Stages the independently compiled peer in the ordinary Java 11 engine container.
     *
     * <p>The connector receives an explicit PEM anchor; the JVM's global trust store is unchanged.
     */
    private static final class Engine extends SeaTunnelContainer {
        private static final String ROOT = "/tmp/azure-table-peer";
        private final Path fixtures;

        private Engine(Path fixtures) {
            this.fixtures = fixtures;
        }

        /**
         * Preserves reserved credential characters as argv without logging command values.
         *
         * <p>The inherited shell join also logs the joined command and its credential values.
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
            container.withExtraHost(AzureTablePeer.HOST, "127.0.0.1");
            container.withExtraHost(AzureTablePeer.CHINA_HOST, "127.0.0.1");
            container.withExtraHost("otherfixtureaccount.table.core.windows.net", "127.0.0.1");
            for (String name : List.of("trusted.p12", "untrusted.p12", "trusted.pem")) {
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(fixtures.resolve(name)), ROOT + "/" + name);
            }
            try {
                String file = "AzureTablePeer.class";
                Path source = Path.of(AzureTablePeer.class.getResource(file).toURI());
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(source),
                        ROOT
                                + "/classes/"
                                + AzureTablePeer.class.getPackage().getName().replace('.', '/')
                                + "/"
                                + file);
                Path probe = Path.of(AzureTableRecoveryProbe.class.getResource("").toURI());
                try (DirectoryStream<Path> classes =
                        Files.newDirectoryStream(probe, "AzureTableRecoveryProbe*.class")) {
                    for (Path compiled : classes) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(compiled),
                                ROOT
                                        + "/classes/"
                                        + AzureTableRecoveryProbe.class
                                                .getPackage()
                                                .getName()
                                                .replace('.', '/')
                                        + "/"
                                        + compiled.getFileName());
                    }
                }
            } catch (URISyntaxException e) {
                throw new IOException("Cannot stage the Java HTTPS fixture", e);
            }
        }

        private Container.ExecResult runPeer() throws Exception {
            return server.execInContainer(
                    "java", "-cp", ROOT + "/classes", AzureTablePeer.class.getName(), ROOT);
        }

        private Container.ExecResult runProbe(String mode) throws Exception {
            ContainerUtil.copyAllConnectorJarToContainer(
                    server,
                    getConnectorModulePath(),
                    getConnectorNamePrefix(),
                    getConnectorType(),
                    SEATUNNEL_HOME);
            return server.execInContainer(
                    "java",
                    "-cp",
                    ROOT + "/classes:/tmp/seatunnel/lib/*:/tmp/seatunnel/connectors/*",
                    AzureTableRecoveryProbe.class.getName(),
                    ROOT + "/trusted.pem",
                    AzureTablePeer.KEY,
                    mode);
        }

        /**
         * Includes checkpoints from the still-running anchor pipeline after the source finishes.
         *
         * <p>The base helper reads only the first pipeline, which is terminal in this scenario.
         */
        @Override
        public long getCompletedCheckpointCount(String jobId) {
            HttpURLConnection connection = null;
            try {
                URL url =
                        new URL(
                                "http",
                                server.getHost(),
                                server.getMappedPort(8080),
                                "/jobs/checkpoints/" + jobId);
                connection = (HttpURLConnection) url.openConnection();
                connection.setConnectTimeout(5000);
                connection.setReadTimeout(5000);
                if (connection.getResponseCode() != 200) {
                    return 0L;
                }
                try (InputStream input = connection.getInputStream()) {
                    long completed = 0L;
                    for (JsonNode pipeline : JSON.readTree(input).path("pipelines")) {
                        completed += pipeline.path("counts").path("completed").asLong();
                    }
                    return completed;
                }
            } catch (IOException e) {
                throw new IllegalStateException("Cannot inspect the test job's checkpoints", e);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }

        private void install(Path source, String name) {
            server.copyFileToContainer(MountableFile.forHostPath(source), ROOT + "/" + name);
        }

        private boolean exists(String name) throws Exception {
            return server.execInContainer("test", "-f", ROOT + "/" + name).getExitCode() == 0;
        }

        private void touch(String name) throws Exception {
            success(server.execInContainer("touch", ROOT + "/" + name));
        }

        private List<String> lines(String name) throws Exception {
            if (!exists(name)) {
                return Collections.emptyList();
            }
            Container.ExecResult result = server.execInContainer("cat", ROOT + "/" + name);
            success(result);
            return split(result.getStdout());
        }

        private List<String> rows(String name) throws Exception {
            String path = "/tmp/seatunnel_mnt/azure-table-" + name;
            Container.ExecResult result =
                    server.execInContainer(
                            "sh",
                            "-c",
                            "if [ -d "
                                    + path
                                    + " ]; then find "
                                    + path
                                    + " -maxdepth 1 -type f -name 'receipt_*'"
                                    + " -exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
            success(result);
            return split(result.getStdout());
        }

        private static List<String> split(String value) {
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
