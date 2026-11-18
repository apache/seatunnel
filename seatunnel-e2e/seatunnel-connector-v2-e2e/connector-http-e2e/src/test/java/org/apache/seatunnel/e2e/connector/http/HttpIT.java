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
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.flink.AbstractTestFlinkContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.mockserver.client.MockServerClient;
import org.mockserver.matchers.TimeToLive;
import org.mockserver.matchers.Times;
import org.mockserver.model.ClearType;
import org.mockserver.model.Format;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

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

    private static final String ALWAYS_FAILING_PATH = "/example/restart/always-failing";
    private static final String FAILING_AFTER_CHECKPOINT_PATH =
            "/example/restart/failing-after-checkpoint";
    private static final String RECOVERING_PATH = "/example/restart/recovering";
    /** The Flink job name: the test containers name a job after its config file. */
    private static final String FAILING_AFTER_CHECKPOINT_JOB_NAME =
            "http_source_failing_after_checkpoint.conf";
    /** The first attempt and the two restarts that the job configs allow on Flink. */
    private static final int FLINK_ATTEMPTS = 3;

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

    /**
     * A single-split source whose server rejects every request (#13253). On Flink the job is
     * restarted twice, and each restart hands the split back to {@code SingleSplitEnumerator}:
     * before the fix, the split was not assigned again and the second restart failed with an
     * IndexOutOfBoundsException of the enumerator, which replaced the error of the source. The job
     * must fail with the error of the source.
     */
    @TestTemplate
    public void testRestartedSingleSplitSourceFailsWithItsOwnError(TestContainer container)
            throws IOException, InterruptedException {
        mockServerClient
                .when(request().withPath(ALWAYS_FAILING_PATH))
                .respond(response().withStatusCode(500).withBody("always failing"));
        try {
            Container.ExecResult execResult =
                    container.executeJob("/http_always_failing_source_restarted.conf");
            assertFailedWithTheErrorOfTheSource(container, execResult, "always failing");
            if (container.identifier().getEngineType() == EngineType.FLINK) {
                // The HTTP client retries only on an IOException, so each attempt makes one
                // request.
                Assertions.assertEquals(FLINK_ATTEMPTS, requestsTo(ALWAYS_FAILING_PATH));
            }
        } finally {
            mockServerClient.clear(request().withPath(ALWAYS_FAILING_PATH));
        }
    }

    /**
     * The same source failing after a completed checkpoint (#13253). The server answers until the
     * test has seen, through the Flink REST API, two checkpoints of the job complete after the
     * reader read a row; only then does it reject every request. The reader got its split before
     * those checkpoints, so on a restart Flink hands an empty list back to {@code
     * SingleSplitEnumerator}, which threw before the fix. The restored reader takes its split from
     * the checkpoint, fails again, and the job must end with the error of the source after its two
     * restarts.
     */
    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SEATUNNEL, EngineType.SPARK},
            disabledReason =
                    "Only Flink hands back the splits assigned since the last checkpoint; the Zeta"
                            + " restore order is covered by SingleSplitEnumeratorTest")
    public void testSingleSplitSourceFailingAfterACheckpointFailsWithItsOwnError(
            TestContainer container) throws Exception {
        AbstractTestFlinkContainer flink = (AbstractTestFlinkContainer) container;
        mockServerClient
                .when(request().withPath(FAILING_AFTER_CHECKPOINT_PATH))
                .respond(
                        response()
                                .withStatusCode(200)
                                .withBody("[{\"c_string\":\"a\",\"c_int\":1}]"));
        try {
            CompletableFuture<Container.ExecResult> job =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return container.executeJob(
                                            "/http_source_failing_after_checkpoint.conf");
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
            String jobId = awaitRunningFlinkJob(flink, FAILING_AFTER_CHECKPOINT_JOB_NAME);
            await().atMost(2, TimeUnit.MINUTES)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            responsesTo(FAILING_AFTER_CHECKPOINT_PATH, 200) > 0,
                                            "the reader read no row yet"));
            // The job config allows one checkpoint at a time, so the second one to complete from
            // now on was triggered after the reader read a row with its split.
            long completedBefore = completedFlinkCheckpoints(flink, jobId);
            await().atMost(2, TimeUnit.MINUTES)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            completedFlinkCheckpoints(flink, jobId)
                                                    >= completedBefore + 2,
                                            "no checkpoint of job " + jobId + " completed yet"));
            // A higher priority than the expectation above, so every request from now on fails
            mockServerClient
                    .when(
                            request().withPath(FAILING_AFTER_CHECKPOINT_PATH),
                            Times.unlimited(),
                            TimeToLive.unlimited(),
                            1)
                    .respond(response().withStatusCode(500).withBody("failing after checkpoint"));
            Container.ExecResult execResult = job.get(5, TimeUnit.MINUTES);
            assertFailedWithTheErrorOfTheSource(container, execResult, "failing after checkpoint");
            Assertions.assertEquals(
                    FLINK_ATTEMPTS, responsesTo(FAILING_AFTER_CHECKPOINT_PATH, 500));
        } finally {
            mockServerClient.clear(request().withPath(FAILING_AFTER_CHECKPOINT_PATH));
        }
    }

    /**
     * A single-split source whose server rejects the first two requests and then answers (#13253).
     * Flink restarts the job twice and hands the split back to {@code SingleSplitEnumerator} each
     * time; before the fix the second restart failed in the enumerator. With the split assigned
     * again, the third attempt reads the one row, the job completes, and the Assert sink checks
     * that it got exactly that row.
     */
    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SEATUNNEL, EngineType.SPARK},
            disabledReason =
                    "Only the Flink restart strategy of this job restarts it; on Zeta and Spark the"
                            + " first failure ends the job")
    public void testSingleSplitSourceRecoveringAfterTwoRestartsCompletesWithItsRow(
            TestContainer container) throws IOException, InterruptedException {
        mockServerClient
                .when(request().withPath(RECOVERING_PATH), Times.exactly(FLINK_ATTEMPTS - 1))
                .respond(response().withStatusCode(500).withBody("failing twice"));
        mockServerClient
                .when(request().withPath(RECOVERING_PATH))
                .respond(
                        response()
                                .withStatusCode(200)
                                .withBody("[{\"c_string\":\"recovered\",\"c_int\":7}]"));
        try {
            Container.ExecResult execResult =
                    container.executeJob("/http_source_recovering_after_restarts.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
            Assertions.assertEquals(FLINK_ATTEMPTS, requestsTo(RECOVERING_PATH));
            Assertions.assertFalse(
                    container.getServerLogs().contains("SingleSplitEnumerator.addSplitsBack"),
                    "the enumerator failed when a split was handed back");
        } finally {
            mockServerClient.clear(request().withPath(RECOVERING_PATH));
        }
    }

    /**
     * Asserts that the job failed, that its output or the server logs hold the error of the HTTP
     * source for {@code responseBody}, which only this job receives, and that the enumerator never
     * failed while a split was handed back. On Flink the client prints only one failure cause, so
     * the error of the source is looked for in the server logs as well. No other test of this class
     * restarts a job.
     *
     * @return the job output and the server logs
     */
    private static String assertFailedWithTheErrorOfTheSource(
            TestContainer container, Container.ExecResult execResult, String responseBody) {
        Assertions.assertNotEquals(0, execResult.getExitCode());
        String output = execResult.getStdout() + execResult.getStderr() + container.getServerLogs();
        Assertions.assertTrue(
                output.contains("http response status code:[500], content:[" + responseBody + "]"),
                "the error of the source is missing");
        Assertions.assertFalse(
                output.contains("SingleSplitEnumerator.addSplitsBack"),
                "the enumerator failed when a split was handed back");
        return output;
    }

    private int requestsTo(String path) {
        return mockServerClient.retrieveRecordedRequests(request().withPath(path)).length;
    }

    private long responsesTo(String path, int statusCode) {
        return Arrays.stream(
                        mockServerClient.retrieveRecordedRequestsAndResponses(
                                request().withPath(path)))
                .filter(r -> r.getHttpResponse().getStatusCode() == statusCode)
                .count();
    }

    /** Waits until the Flink job named {@code jobName} runs and returns its id. */
    private static String awaitRunningFlinkJob(AbstractTestFlinkContainer flink, String jobName) {
        AtomicReference<String> jobId = new AtomicReference<>();
        await().atMost(2, TimeUnit.MINUTES)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            JsonNode overview =
                                    flinkRest(flink, "http://localhost:8081/jobs/overview");
                            for (JsonNode job : overview.path("jobs")) {
                                if (jobName.equals(job.path("name").asText())
                                        && "RUNNING".equals(job.path("state").asText())) {
                                    jobId.set(job.path("jid").asText());
                                }
                            }
                            Assertions.assertNotNull(
                                    jobId.get(), "job " + jobName + " not running");
                        });
        return jobId.get();
    }

    private static long completedFlinkCheckpoints(AbstractTestFlinkContainer flink, String jobId)
            throws IOException, InterruptedException {
        return flinkRest(flink, "http://localhost:8081/jobs/" + jobId + "/checkpoints")
                .path("counts")
                .path("completed")
                .asLong();
    }

    private static JsonNode flinkRest(AbstractTestFlinkContainer flink, String url)
            throws IOException, InterruptedException {
        return new ObjectMapper().readTree(flink.executeJobManagerInnerCommand("curl -s " + url));
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
}
