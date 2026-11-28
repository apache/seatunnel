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

package org.apache.seatunnel.e2e.connector.http;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.ClearType;
import org.mockserver.model.Format;
import org.mockserver.model.HttpRequest;
import org.postgresql.Driver;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * Exercises installed HTTP-based connectors against the shared controlled server. The Matomo
 * extension also observes real checkpoint progress while one validated daily response drains.
 */
@Slf4j
public class HttpIT extends TestSuiteBase implements TestResource {

    private static final String TMP_DIR = "/tmp";

    private static final String IMAGE = "mockserver/mockserver:5.14.0";

    private GenericContainer<?> mockserverContainer;

    private static final List<Record> records = new ArrayList<>();

    private MockServerClient mockServerClient;

    private static final String POSTGRESQL_SCHEMA = "public";
    private static final String SINK_TABLE_1 = "sink";
    private static final Integer MAX_COUNT = 15;
    private static final String COUNT_QUERY = "select count(*) from sink";

    private static final String PG_IMAGE = "postgres:14-alpine";
    private static final String PG_DRIVER_CONTAINER_PATH =
            "/tmp/seatunnel/plugins/Jdbc/lib/postgresql.jar";
    private PostgreSQLContainer<?> postgreSQLContainer;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.of(Driver.class)
                            .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib", "postgresql.jar");

    @BeforeAll
    @Override
    public void startUp() throws ClassNotFoundException {
        Optional<URL> resource =
                Optional.ofNullable(HttpIT.class.getResource(getMockServerConfig()));
        this.mockserverContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("mockserver")
                        .withExposedPorts(1080)
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(
                                        new File(
                                                        resource.orElseThrow(
                                                                        () ->
                                                                                new IllegalArgumentException(
                                                                                        "Can not get config file of mockServer"))
                                                                .getPath())
                                                .getAbsolutePath()),
                                TMP_DIR + getMockServerConfig())
                        .withEnv(
                                "MOCKSERVER_INITIALIZATION_JSON_PATH",
                                TMP_DIR + getMockServerConfig())
                        .withEnv("MOCKSERVER_LOG_LEVEL", "WARN")
                        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                        .waitingFor(new HttpWaitStrategy().forPath("/").forStatusCode(404));
        Startables.deepStart(Stream.of(mockserverContainer)).join();
        mockServerClient =
                new MockServerClient(
                        mockserverContainer.getHost(), mockserverContainer.getMappedPort(1080));
        fillMockRecords();

        postgreSQLContainer =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases("postgresql")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        Startables.deepStart(Stream.of(postgreSQLContainer)).join();
        log.info("PostgreSQL container started");
        Class.forName(postgreSQLContainer.getDriverClassName());
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);
    }

    private static void fillMockRecords() {
        Record recordFirst = new Record();
        RequestBody requestBodyFirst = new RequestBody();
        JsonBody jsonBodyFirst = new JsonBody();
        jsonBodyFirst.setId(1);
        jsonBodyFirst.setVal_bool(true);
        jsonBodyFirst.setVal_int8(new Byte("1"));
        jsonBodyFirst.setVal_int16((short) 2);
        jsonBodyFirst.setVal_int32(3);
        jsonBodyFirst.setVal_int64(4);
        jsonBodyFirst.setVal_float(4.3F);
        jsonBodyFirst.setVal_double(5.3);
        jsonBodyFirst.setVal_decimal(BigDecimal.valueOf(6.3));
        jsonBodyFirst.setVal_string("NEW");
        jsonBodyFirst.setVal_unixtime_micros("2020-02-02T02:02:02");
        requestBodyFirst.setJson(jsonBodyFirst);
        recordFirst.setBody(requestBodyFirst);

        Record recordSec = new Record();
        RequestBody requestBodySec = new RequestBody();
        JsonBody jsonBodySec = new JsonBody();
        jsonBodySec.setId(2);
        jsonBodySec.setVal_bool(true);
        jsonBodySec.setVal_int8(new Byte("1"));
        jsonBodySec.setVal_int16((short) 2);
        jsonBodySec.setVal_int32(3);
        jsonBodySec.setVal_int64(4);
        jsonBodySec.setVal_float(4.3F);
        jsonBodySec.setVal_double(5.3);
        jsonBodySec.setVal_decimal(BigDecimal.valueOf(6.3));
        jsonBodySec.setVal_string("NEW");
        jsonBodySec.setVal_unixtime_micros("2020-02-02T02:02:02");
        requestBodySec.setJson(jsonBodySec);
        recordSec.setBody(requestBodySec);
        records.add(recordFirst);
        records.add(recordSec);
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (mockserverContainer != null) {
            mockserverContainer.stop();
        }
        if (mockServerClient != null) {
            mockServerClient.close();
        }
        if (postgreSQLContainer != null) {
            postgreSQLContainer.stop();
        }
    }

    @TestTemplate
    public void testStreamingSourceToPostgresqlSink(TestContainer container) {
        try {
            CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            Container.ExecResult execResult1 =
                                    container.executeJob("/http_streaming_json_to_postgresql.conf");
                        } catch (Exception e) {
                            log.error("Commit task exception :" + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return null;
                    });
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long count = queryCount(COUNT_QUERY);
                                Assertions.assertTrue(
                                        count >= MAX_COUNT,
                                        "Actual value should be greater than expected value");
                            });
        } finally {
            log.info("clear schema:{}", SINK_TABLE_1);
            clearTable(POSTGRESQL_SCHEMA, SINK_TABLE_1);
        }
    }

    private Long queryCount(String sql) {
        try (Connection connection = getJdbcConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            if (resultSet.next()) {

                return resultSet.getLong(1);
            }
            return 0L;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                postgreSQLContainer.getJdbcUrl(),
                postgreSQLContainer.getUsername(),
                postgreSQLContainer.getPassword());
    }

    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SET search_path TO inventory;");
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void clearTable(String database, String tableName) {
        executeSql("truncate table " + database + "." + tableName);
    }

    @TestTemplate
    public void testSourceToAssertSink(TestContainer container)
            throws IOException, InterruptedException {
        // dynamic param for body
        Container.ExecResult execResult0 =
                container.executeJob("/http_post_param_json_to_assert.conf");
        Assertions.assertEquals(0, execResult0.getExitCode());

        // normal http
        Container.ExecResult execResult1 = container.executeJob("/http_json_to_assert.conf");
        Assertions.assertEquals(0, execResult1.getExitCode());

        // http github
        Container.ExecResult execResult2 = container.executeJob("/github_json_to_assert.conf");
        Assertions.assertEquals(0, execResult2.getExitCode());

        // http gitlab
        Container.ExecResult execResult3 = container.executeJob("/gitlab_json_to_assert.conf");
        Assertions.assertEquals(0, execResult3.getExitCode());

        // http content json
        Container.ExecResult execResult4 = container.executeJob("/http_contentjson_to_assert.conf");
        Assertions.assertEquals(0, execResult4.getExitCode());

        // http jsonpath
        Container.ExecResult execResult5 = container.executeJob("/http_jsonpath_to_assert.conf");
        Assertions.assertEquals(0, execResult5.getExitCode());

        // http jira
        Container.ExecResult execResult6 = container.executeJob("/jira_json_to_assert.conf");
        Assertions.assertEquals(0, execResult6.getExitCode());

        // http klaviyo
        Container.ExecResult execResult7 = container.executeJob("/klaviyo_json_to_assert.conf");
        Assertions.assertEquals(0, execResult7.getExitCode());

        // http lemlist
        Container.ExecResult execResult8 = container.executeJob("/lemlist_json_to_assert.conf");
        Assertions.assertEquals(0, execResult8.getExitCode());

        // http notion
        Container.ExecResult execResult9 = container.executeJob("/notion_json_to_assert.conf");
        Assertions.assertEquals(0, execResult9.getExitCode());

        // http onesignal
        Container.ExecResult execResult10 = container.executeJob("/onesignal_json_to_assert.conf");
        Assertions.assertEquals(0, execResult10.getExitCode());

        // http persistiq
        Container.ExecResult execResult11 = container.executeJob("/persistiq_json_to_assert.conf");
        Assertions.assertEquals(0, execResult11.getExitCode());

        // http shopify
        Container.ExecResult execResult24 = container.executeJob("/shopify_json_to_assert.conf");
        Assertions.assertEquals(0, execResult24.getExitCode());

        // http httpMultiLine
        Container.ExecResult execResult12 =
                container.executeJob("/http_multilinejson_to_assert.conf");
        Assertions.assertEquals(0, execResult12.getExitCode());

        // http httpFormRequestbody
        Container.ExecResult execResult13 =
                container.executeJob("/http_formrequestbody_to_assert.conf");
        Assertions.assertEquals(0, execResult13.getExitCode());

        Container.ExecResult execResult20 =
                container.executeJob("/http_formrequestbody_to_assert2.conf");
        Assertions.assertEquals(0, execResult20.getExitCode());

        // http httpJsonRequestBody
        Container.ExecResult execResult14 =
                container.executeJob("/http_jsonrequestbody_to_assert.conf");
        Assertions.assertEquals(0, execResult14.getExitCode());

        Container.ExecResult execResult15 =
                container.executeJob("/http_page_increase_page_num.conf");
        Assertions.assertEquals(0, execResult15.getExitCode());

        Container.ExecResult execResult16 =
                container.executeJob("/http_page_increase_no_page_num.conf");
        Assertions.assertEquals(0, execResult16.getExitCode());

        Container.ExecResult execResult17 =
                container.executeJob("/http_jsonrequestbody_to_feishu.conf");
        Assertions.assertEquals(0, execResult17.getExitCode());

        Container.ExecResult execResult18 = container.executeJob("/httpnoschema_to_http.conf");
        Assertions.assertEquals(0, execResult18.getExitCode());

        Container.ExecResult execResult19 =
                container.executeJob("/http_page_increase_start_num.conf");
        Assertions.assertEquals(0, execResult19.getExitCode());

        Container.ExecResult execResult21 =
                container.executeJob("/http_page_cursor_num_assert.conf");
        Assertions.assertEquals(0, execResult21.getExitCode());

        // http airtable source
        Container.ExecResult execResult22 = container.executeJob("/airtable_json_to_assert.conf");
        Assertions.assertEquals(0, execResult22.getExitCode());

        // http posthog source
        Container.ExecResult execResult23 = container.executeJob("/posthog_json_to_assert.conf");
        Assertions.assertEquals(0, execResult23.getExitCode());
    }

    /**
     * Run the binary download case in a fresh container so the longest HTTP source path does not
     * inherit accumulated runtime pressure from the broader source-to-assert matrix.
     */
    @TestTemplate
    public void testBinarySourceToAssertSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/http_binary_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testMatomoDailyReports(TestContainer container)
            throws IOException, InterruptedException {
        HttpRequest reportRequest =
                request()
                        .withMethod("GET")
                        .withPath("/matomo-report")
                        .withQueryStringParameter("module", "API")
                        .withQueryStringParameter("method", Pattern.quote("VisitsSummary.get"))
                        .withQueryStringParameter("idSite", "1")
                        .withQueryStringParameter("period", "day")
                        .withQueryStringParameter("date", "2026-06-01,2026-06-02")
                        .withQueryStringParameter("format", "JSON");
        mockServerClient
                .when(reportRequest)
                .respond(
                        response()
                                .withHeader("Content-Type", "application/json")
                                .withBody(
                                        "{\"2026-06-01\":{\"nb_visits\":9,\"nb_actions\":17},"
                                                + "\"2026-06-02\":[]}"));
        Container.ExecResult result = container.executeJob("/matomo_daily_to_assert.conf");
        Assertions.assertEquals(0, result.getExitCode());
        mockServerClient.verify(reportRequest);
        if (container.identifier().getEngineType() == EngineType.SEATUNNEL) {
            verifyMatomoCheckpointHandoff(container);
        }
    }

    @TestTemplate
    public void testFakeToAirtableSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/fake_to_airtable.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        mockServerClient.verify(request().withPath("/v0/appTEST123/SinkTable").withMethod("POST"));
    }

    @TestTemplate
    public void testMultiTableHttp(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/fake_to_multitable.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        String mockResponse =
                mockServerClient.retrieveRecordedRequests(
                        request().withPath("/example/httpMultiTableContentSink").withMethod("POST"),
                        Format.JSON);
        mockServerClient.clear(
                request().withPath("/example/httpMultiTableContentSink").withMethod("POST"),
                ClearType.LOG);
        List<Record> recordResponse =
                objectMapper.readValue(mockResponse, new TypeReference<List<Record>>() {});
        recordResponse =
                recordResponse.stream()
                        .sorted(
                                (r1, r2) ->
                                        r1.getBody().getJson().getId()
                                                - r2.getBody().getJson().getId())
                        .collect(Collectors.toList());
        Assertions.assertIterableEquals(records, recordResponse);
    }

    private void initializeJdbcTable() {
        try (Connection connection =
                DriverManager.getConnection(
                        postgreSQLContainer.getJdbcUrl(),
                        postgreSQLContainer.getUsername(),
                        postgreSQLContainer.getPassword())) {
            Statement statement = connection.createStatement();
            String sink =
                    "create table sink(\n"
                            + "c_String varchar(255) NOT NULL PRIMARY KEY,\n"
                            + "c_int INT\n"
                            + ")";
            statement.execute(sink);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    @Getter
    @Setter
    @EqualsAndHashCode
    static class Record {
        private RequestBody body;
    }

    @Getter
    @Setter
    @EqualsAndHashCode
    static class RequestBody {
        private JsonBody json;
    }

    @Getter
    @Setter
    @EqualsAndHashCode
    static class JsonBody {
        private int id;
        private boolean val_bool;
        private byte val_int8;
        private short val_int16;
        private int val_int32;
        private long val_int64;
        private float val_float;
        private double val_double;
        private BigDecimal val_decimal;
        private String val_string;
        private String val_unixtime_micros;
    }

    public String getMockServerConfig() {
        return "/mockserver-config.json";
    }
    /**
     * Reuses only the existing Zeta invocation. Console dates observe accepted rows; independently
     * completed checkpoints, one HTTP request and final exact output prove the decoded-page path.
     */
    private void verifyMatomoCheckpointHandoff(TestContainer container)
            throws IOException, InterruptedException {
        LocalDate first = LocalDate.of(2026, 1, 1);
        List<String> expected = new ArrayList<>();
        StringBuilder body = new StringBuilder("{");
        for (int i = 0; i < 100; i++) {
            String date = first.plusDays(i).toString();
            expected.add(date);
            if (i > 0) {
                body.append(',');
            }
            body.append('"').append(date).append("\":{\"nb_visits\":9,\"nb_actions\":17}");
        }
        body.append('}');
        HttpRequest report =
                request()
                        .withMethod("GET")
                        .withPath("/matomo-throttled-report")
                        .withQueryStringParameter("module", "API")
                        .withQueryStringParameter("method", Pattern.quote("VisitsSummary.get"))
                        .withQueryStringParameter("idSite", "1")
                        .withQueryStringParameter("period", "day")
                        .withQueryStringParameter("date", first + "," + first.plusDays(99))
                        .withQueryStringParameter("format", "JSON");
        mockServerClient
                .when(report)
                .respond(
                        response()
                                .withHeader("Content-Type", "application/json")
                                .withBody(body.toString()));
        String jobId = Long.toString(ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE));
        int logOffset = container.getServerLogs().length();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Container.ExecResult> job =
                executor.submit(
                        () -> container.executeJob("/matomo_throttled_to_assert.conf", jobId));
        try {
            await().atMost(45, TimeUnit.SECONDS)
                    .until(() -> !matomoReportDates(container, logOffset).isEmpty());
            long baseline = container.getCompletedCheckpointCount(jobId);
            // Two later completions exclude a checkpoint already injected before the first row.
            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        container.getCompletedCheckpointCount(jobId)
                                                >= baseline + 2);
                                Assertions.assertTrue(
                                        matomoReportDates(container, logOffset).size() < 100,
                                        "The one decoded response must still be draining");
                                Assertions.assertEquals(
                                        1,
                                        mockServerClient.retrieveRecordedRequests(report).length);
                            });
            Container.ExecResult result = job.get(45, TimeUnit.SECONDS);
            Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
            Assertions.assertEquals(expected, matomoReportDates(container, logOffset));
            Assertions.assertEquals(1, mockServerClient.retrieveRecordedRequests(report).length);
        } catch (ExecutionException | TimeoutException e) {
            throw new IOException("Checkpointed Matomo job did not finish", e);
        } finally {
            try {
                if (!job.isDone()) {
                    container.cancelJob(jobId);
                }
            } finally {
                executor.shutdownNow();
                Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
            }
        }
    }

    /*
     * Reads this job's Console acceptance trace after a captured log boundary. This trace is not a
     * checkpoint commit receipt; the caller queries actual completed checkpoint counters too.
     */
    private static List<String> matomoReportDates(TestContainer container, int offset) {
        String logs = container.getServerLogs();
        Assertions.assertTrue(
                logs.length() >= offset, "Server log must not rotate during the case");
        String marker = "SeaTunnelRow#kind=INSERT : 1, ";
        List<String> dates = new ArrayList<>();
        for (String line : logs.substring(offset).split("\\R")) {
            int at = line.indexOf(marker);
            if (at >= 0) {
                dates.add(line.substring(at + marker.length(), at + marker.length() + 10));
            }
        }
        return dates;
    }
}
