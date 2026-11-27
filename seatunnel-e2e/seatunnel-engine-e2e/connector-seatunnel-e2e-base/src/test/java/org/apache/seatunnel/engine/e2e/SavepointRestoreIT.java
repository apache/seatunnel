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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import io.restassured.common.mapper.TypeRef;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class SavepointRestoreIT extends SeaTunnelEngineContainer {

    private static final String HOST = "http://localhost:";
    private static final String CONF_FILE =
            "/savepoint-restore/stream_checkpointable_sequence_to_localfile.conf";
    private static final String SINK_OUTPUT_DIR =
            HOST_VOLUME_MOUNT_PATH + "/savepoint-restore/sinkfile";
    private static final int PUBLICATION_REST_REQUEST_TIMEOUT_MILLIS = 5_000;
    private static final int LOCAL_UNRESPONSIVE_REST_TIMEOUT_MILLIS = 1_000;
    private static final int PUBLICATION_READER_SHUTDOWN_TIMEOUT_MILLIS =
            PUBLICATION_REST_REQUEST_TIMEOUT_MILLIS * 2 + 1_000;

    private boolean checkConnectorPublication;
    private int publicationChecks;
    private final AtomicInteger publicationRequests = new AtomicInteger();
    private final AtomicReference<Throwable> publicationFailure = new AtomicReference<>();

    @Test
    public void testPublicationRestRequestTimesOut() throws Exception {
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try (UnresponsiveRestServer http = new UnresponsiveRestServer()) {
            Future<?> request =
                    caller.submit(
                            () ->
                                    http.probe.checkJobInfoAvailable(
                                            "probe", LOCAL_UNRESPONSIVE_REST_TIMEOUT_MILLIS));
            Assertions.assertTrue(
                    http.received.await(5, TimeUnit.SECONDS), "Expected a REST request");
            request.get(3, TimeUnit.SECONDS);
            assertReadTimeout(http.probe.publicationFailure.get());
        } finally {
            caller.shutdownNow();
            Assertions.assertTrue(
                    caller.awaitTermination(5, TimeUnit.SECONDS), "REST caller leaked");
        }
    }

    @Test
    public void testPublicationRestRequestAllowsSlowHealthyResponse() throws Exception {
        try (DelayedHealthyRestServer http = new DelayedHealthyRestServer()) {
            http.probe.checkJobInfoAvailable("probe");
            Assertions.assertNull(
                    http.probe.publicationFailure.get(),
                    "A slow healthy response must not fail connector publication");
        }
    }

    private static void assertReadTimeout(Throwable failure) {
        Assertions.assertInstanceOf(SocketTimeoutException.class, failure);
        Assertions.assertTrue(failure.getMessage().contains("timed out"), failure.getMessage());
    }

    /** Accept the request but hold its response until the assertion or cleanup completes. */
    private static final class UnresponsiveRestServer implements AutoCloseable {
        private final CountDownLatch received = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final com.sun.net.httpserver.HttpServer http;
        private final SavepointRestoreIT probe = new SavepointRestoreIT();

        private UnresponsiveRestServer() throws IOException {
            http =
                    com.sun.net.httpserver.HttpServer.create(
                            new InetSocketAddress("127.0.0.1", 0), 0);
            http.setExecutor(executor);
            http.createContext(
                    "/job-info/probe",
                    exchange -> {
                        received.countDown();
                        try {
                            release.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            exchange.close();
                        }
                    });
            http.start();
            probe.server = mock(GenericContainer.class);
            when(probe.server.getMappedPort(8080)).thenReturn(http.getAddress().getPort());
        }

        @Override
        public void close() throws InterruptedException {
            release.countDown();
            http.stop(0);
            executor.shutdownNow();
            Assertions.assertTrue(
                    executor.awaitTermination(5, TimeUnit.SECONDS), "HTTP fixture leaked");
        }
    }

    private static final class DelayedHealthyRestServer implements AutoCloseable {
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final com.sun.net.httpserver.HttpServer http;
        private final SavepointRestoreIT probe = new SavepointRestoreIT();

        private DelayedHealthyRestServer() throws IOException {
            http =
                    com.sun.net.httpserver.HttpServer.create(
                            new InetSocketAddress("127.0.0.1", 0), 0);
            http.setExecutor(executor);
            http.createContext(
                    "/job-info/probe",
                    exchange -> {
                        try {
                            Thread.sleep(1500);
                            exchange.sendResponseHeaders(200, -1);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            exchange.close();
                        }
                    });
            http.start();
            probe.server = mock(GenericContainer.class);
            when(probe.server.getMappedPort(8080)).thenReturn(http.getAddress().getPort());
        }

        @Override
        public void close() throws InterruptedException {
            http.stop(0);
            executor.shutdownNow();
            Assertions.assertTrue(
                    executor.awaitTermination(5, TimeUnit.SECONDS), "HTTP fixture leaked");
        }
    }

    @Override
    protected Container.ExecResult restoreJob(
            GenericContainer<?> container,
            String confFile,
            String jobId,
            List<String> variables,
            String command)
            throws IOException, InterruptedException {
        if (!checkConnectorPublication) {
            return super.restoreJob(container, confFile, jobId, variables, command);
        }
        // Keep the real restore path, but expose a deterministic partial archive at its copy
        // target.
        class CopyingContainer extends GenericContainer<CopyingContainer> {
            private final boolean failCopy;
            private String staged;
            private String published;
            private String oldHash;
            private String newHash;

            private CopyingContainer(boolean failCopy) {
                super(DockerImageName.parse(container.getDockerImageName()));
                this.failCopy = failCopy;
            }

            @Override
            public void copyFileToContainer(MountableFile file, String target) {
                if (!file.getResolvedPath().contains("connector-file-local")) {
                    container.copyFileToContainer(file, target);
                    return;
                }
                try {
                    staged = target;
                    published = target.substring(0, target.lastIndexOf(".jar") + 4);
                    oldHash = containerHash(published);
                    Path replacement = connectorWithMarker(Paths.get(file.getResolvedPath()));
                    try (java.io.Closeable cleanup = () -> Files.deleteIfExists(replacement)) {
                        newHash = sha256(replacement);
                        Assertions.assertNotEquals(
                                oldHash, newHash, "Expected distinct old and new JAR content");
                        byte[] prefix = new byte[64 * 1024];
                        try (InputStream input = Files.newInputStream(replacement)) {
                            Assertions.assertEquals(
                                    prefix.length, input.readNBytes(prefix, 0, prefix.length));
                        }
                        container.copyFileToContainer(Transferable.of(prefix), target);
                        Assertions.assertEquals(
                                oldHash,
                                containerHash(published),
                                "Partial copy changed the published JAR");
                        checkJobInfoAvailable(jobId);
                        publicationChecks++;
                        if (failCopy) {
                            throw new IllegalStateException(
                                    "controlled partial connector copy failure");
                        }
                        try (PublicationRestReader reader =
                                new PublicationRestReader(
                                        () -> {
                                            if (publicationFailure.get() == null) {
                                                checkJobInfoAvailable(jobId);
                                            }
                                        })) {
                            container.copyFileToContainer(
                                    MountableFile.forHostPath(replacement), target);
                        }
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("Could not check connector publication", e);
                }
            }

            private String containerHash(String path) {
                try {
                    Container.ExecResult result = container.execInContainer("sha256sum", path);
                    Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
                    return result.getStdout().split("\\s+")[0];
                } catch (IOException e) {
                    throw new IllegalStateException("Could not hash published connector JAR", e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while hashing connector JAR", e);
                }
            }

            @Override
            public Container.ExecResult execInContainer(String... commands)
                    throws IOException, InterruptedException {
                Container.ExecResult result = container.execInContainer(commands);
                if (commands[0].equals("mv")
                        && commands[2].equals(staged)
                        && result.getExitCode() == 0) {
                    Assertions.assertEquals(
                            newHash,
                            containerHash(published),
                            "New connector JAR was not published");
                    log.info(
                            "Validated distinct connector JAR publication: old={} new={}",
                            oldHash,
                            newHash);
                }
                return result;
            }
        }
        CopyingContainer failedCopy = new CopyingContainer(true);
        IllegalStateException failure =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () -> super.restoreJob(failedCopy, confFile, jobId, variables, command));
        Assertions.assertEquals("controlled partial connector copy failure", failure.getMessage());
        Assertions.assertEquals(
                failedCopy.oldHash,
                failedCopy.containerHash(failedCopy.published),
                "Failed copy changed the old connector JAR");
        Container.ExecResult removed =
                container.execInContainer("test", "!", "-e", failedCopy.staged);
        Assertions.assertEquals(
                0, removed.getExitCode(), "Failed copy left a staged connector JAR");
        log.info("Validated failed partial copy preserves old connector JAR and removes staging");
        return super.restoreJob(new CopyingContainer(false), confFile, jobId, variables, command);
    }

    private static Path connectorWithMarker(Path original) throws IOException {
        Path replacement = Files.createTempFile("connector-publication-", ".jar");
        try (ZipFile source = new ZipFile(original.toFile());
                ZipOutputStream target = new ZipOutputStream(Files.newOutputStream(replacement))) {
            Enumeration<? extends ZipEntry> entries = source.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                target.putNextEntry(new ZipEntry(entry.getName()));
                try (InputStream input = source.getInputStream(entry)) {
                    input.transferTo(target);
                }
                target.closeEntry();
            }
            target.setComment("connector-publication-" + UUID.randomUUID());
        } catch (IOException | RuntimeException | Error failure) {
            try {
                Files.deleteIfExists(replacement);
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        return replacement;
    }

    private static String sha256(Path path) {
        try {
            StringBuilder result = new StringBuilder();
            for (byte value :
                    MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path))) {
                result.append(String.format("%02x", value & 0xff));
            }
            return result.toString();
        } catch (Exception e) {
            throw new IllegalStateException("Could not hash replacement connector JAR", e);
        }
    }

    private void checkJobInfoAvailable(String jobId) {
        checkJobInfoAvailable(jobId, PUBLICATION_REST_REQUEST_TIMEOUT_MILLIS);
    }

    private void checkJobInfoAvailable(String jobId, int requestTimeoutMillis) {
        publicationRequests.incrementAndGet();
        HttpURLConnection connection = null;
        try {
            connection =
                    (HttpURLConnection)
                            new URL(getRestBaseUrl() + "/job-info/" + jobId).openConnection();
            connection.setConnectTimeout(requestTimeoutMillis);
            connection.setReadTimeout(requestTimeoutMillis);
            Assertions.assertEquals(
                    200, connection.getResponseCode(), "REST failed during connector publication");
        } catch (Throwable failure) {
            publicationFailure.compareAndSet(null, failure);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /** Owns the sampler so neither a failed copy nor interruption can leave a reader running. */
    private static final class PublicationRestReader implements AutoCloseable {
        private final ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> new Thread(r, "connector-publication-rest-reader"));

        private PublicationRestReader(Runnable request) {
            executor.scheduleWithFixedDelay(request, 0, 50, TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() {
            boolean interrupted = Thread.interrupted();
            executor.shutdownNow();
            long deadline =
                    System.nanoTime()
                            + TimeUnit.MILLISECONDS.toNanos(
                                    PUBLICATION_READER_SHUTDOWN_TIMEOUT_MILLIS);
            try {
                while (!executor.isTerminated()) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        throw new IllegalStateException("REST publication reader did not stop");
                    }
                    try {
                        executor.awaitTermination(remaining, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Test
    public void testPublicationReaderStopsOnInterrupt() throws Exception {
        try (UnresponsiveRestServer http = new UnresponsiveRestServer()) {
            PublicationRestReader reader =
                    new PublicationRestReader(
                            () ->
                                    http.probe.checkJobInfoAvailable(
                                            "probe", LOCAL_UNRESPONSIVE_REST_TIMEOUT_MILLIS));
            try {
                Assertions.assertTrue(
                        http.received.await(5, TimeUnit.SECONDS),
                        "Expected an in-flight REST request");
                Thread.currentThread().interrupt();
                reader.close();
                Assertions.assertTrue(
                        Thread.currentThread().isInterrupted(), "Lost interrupt status");
                Assertions.assertTrue(reader.executor.isTerminated(), "REST reader leaked");
                assertReadTimeout(http.probe.publicationFailure.get());
            } finally {
                Thread.interrupted();
                reader.close();
            }
        }
    }

    @Test
    public void testPublicationReaderStopsAfterCopyFailure() throws Exception {
        try (UnresponsiveRestServer http = new UnresponsiveRestServer()) {
            PublicationRestReader reader =
                    new PublicationRestReader(
                            () ->
                                    http.probe.checkJobInfoAvailable(
                                            "probe", LOCAL_UNRESPONSIVE_REST_TIMEOUT_MILLIS));
            IOException expected = new IOException("controlled copy failure");
            IOException failure =
                    Assertions.assertThrows(
                            IOException.class,
                            () -> {
                                try (PublicationRestReader owned = reader) {
                                    Assertions.assertTrue(
                                            http.received.await(5, TimeUnit.SECONDS),
                                            "Expected an in-flight REST request");
                                    throw expected;
                                }
                            });
            Assertions.assertSame(expected, failure);
            Assertions.assertEquals("controlled copy failure", failure.getMessage());
            Assertions.assertTrue(
                    reader.executor.isTerminated(), "REST reader leaked after copy failure");
            assertReadTimeout(http.probe.publicationFailure.get());
        }
    }

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        super.startUp();
        copySavepointRestoreTestPluginsToContainer();
    }

    @Test
    public void testRestoreFromSavepoint()
            throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
        checkConnectorPublication = true;
        publicationChecks = 0;
        publicationRequests.set(0);
        publicationFailure.set(null);
        FileUtils.createNewDir(SINK_OUTPUT_DIR);
        try {
            long jobId = JobIdGenerator.newJobId();
            CompletableFuture<Container.ExecResult> sourceJobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return executeJob(CONF_FILE, String.valueOf(jobId));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            awaitJobStatus(jobId, "RUNNING");
            awaitCompletedCheckpoint(jobId);

            Container.ExecResult savepointResult = savepointJob(String.valueOf(jobId));
            Assertions.assertEquals(0, savepointResult.getExitCode(), savepointResult.getStderr());
            awaitJobStatus(jobId, "SAVEPOINT_DONE");

            List<Long> offsetsBeforeRestore = readObservedOffsets();
            long maxOffsetBeforeRestore = getMaxOffset(offsetsBeforeRestore);
            Assertions.assertFalse(
                    offsetsBeforeRestore.isEmpty(), "Expected committed offsets before restore");
            Assertions.assertEquals(0, sourceJobFuture.get().getExitCode());

            CompletableFuture<Container.ExecResult> restoreFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return restoreJob(CONF_FILE, String.valueOf(jobId));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            awaitJobStatus(jobId, "RUNNING");
            assertRestoreContinuesAfterBoundary(offsetsBeforeRestore, maxOffsetBeforeRestore);
            assertNoOffsetDuplicates();

            stopJob(String.valueOf(jobId));
            awaitJobStatus(jobId, "CANCELED");
            Container.ExecResult restoreResult = restoreFuture.get();
            Assertions.assertEquals(0, restoreResult.getExitCode(), restoreResult.getStderr());
            Assertions.assertTrue(publicationChecks > 0, "Expected a connector publication check");
            Assertions.assertNull(
                    publicationFailure.get(),
                    "REST failed during connector publication: " + publicationFailure.get());
            log.info(
                    "Validated {} REST requests during connector publication",
                    publicationRequests.get());
        } finally {
            checkConnectorPublication = false;
            FileUtils.deleteFile(SINK_OUTPUT_DIR);
        }
    }

    private void awaitJobStatus(long jobId, String expectedStatus) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus, getJobStatus(String.valueOf(jobId))));
    }

    private void awaitCompletedCheckpoint(long jobId) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> Assertions.assertTrue(getCompletedCheckpointCount(jobId) > 0));
    }

    private void copySavepointRestoreTestPluginsToContainer() throws IOException {
        URL url =
                FileUtils.searchJarFiles(
                                Paths.get(
                                        PROJECT_ROOT_PATH,
                                        "seatunnel-e2e",
                                        "seatunnel-e2e-common",
                                        "target"))
                        .stream()
                        .filter(jar -> jar.toString().endsWith("-tests.jar"))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Could not locate seatunnel-e2e-common test jar"));
        server.copyFileToContainer(
                MountableFile.forHostPath(Paths.get(url.getFile())),
                Paths.get(
                                SEATUNNEL_HOME,
                                "connectors",
                                Paths.get(url.getFile()).getFileName().toString())
                        .toString());
        server.copyFileToContainer(
                MountableFile.forHostPath(
                        Paths.get(
                                PROJECT_ROOT_PATH,
                                "seatunnel-e2e",
                                "seatunnel-engine-e2e",
                                "connector-seatunnel-e2e-base",
                                "src",
                                "test",
                                "resources",
                                "checkpoint-restore-with-stop",
                                "plugin-mapping.properties")),
                Paths.get(SEATUNNEL_HOME, "connectors", "plugin-mapping.properties").toString());
    }

    private void assertRestoreContinuesAfterBoundary(
            List<Long> offsetsBeforeRestore, long maxOffsetBeforeRestore) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            List<Long> offsetsAfterRestore = readObservedOffsets();
                            List<Long> restoredOffsets = new ArrayList<>();
                            for (Long offset : offsetsAfterRestore) {
                                if (offset > maxOffsetBeforeRestore) {
                                    restoredOffsets.add(offset);
                                }
                            }

                            Assertions.assertTrue(
                                    !restoredOffsets.isEmpty(),
                                    "Expected restored run to continue from savepoint boundary "
                                            + maxOffsetBeforeRestore);

                            Assertions.assertEquals(
                                    offsetsBeforeRestore.size() + restoredOffsets.size(),
                                    offsetsAfterRestore.size(),
                                    "Expected restore to append only new offsets after savepoint boundary "
                                            + maxOffsetBeforeRestore);
                        });
    }

    private void assertNoOffsetDuplicates() {
        Map<Long, Long> counts =
                readObservedOffsets().stream()
                        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        List<Long> duplicates =
                counts.entrySet().stream()
                        .filter(entry -> entry.getValue() > 1)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
        Assertions.assertTrue(
                duplicates.isEmpty(),
                "Found duplicate offsets (exactly-once violated): " + duplicates);
    }

    private long getMaxOffset(List<Long> offsets) {
        return offsets.stream().mapToLong(Long::longValue).max().orElse(-1L);
    }

    private List<Long> readObservedOffsets() {
        Path outputDir = Paths.get(SINK_OUTPUT_DIR);
        if (!Files.exists(outputDir)) {
            return Collections.emptyList();
        }
        try (Stream<Path> paths = Files.walk(outputDir)) {
            return paths.filter(Files::isRegularFile)
                    .flatMap(
                            path -> {
                                try {
                                    return Files.readAllLines(path).stream();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                    .map(String::trim)
                    .filter(line -> !line.isEmpty())
                    .map(Long::parseLong)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long getCompletedCheckpointCount(long jobId) {
        return getPipelineCounter(jobId, "completed");
    }

    private long getPipelineCounter(long jobId, String counterKey) {
        Map<String, Object> overview =
                given().get(
                                getRestBaseUrl()
                                        + RestConstant.REST_URL_CHECKPOINT_OVERVIEW
                                        + "/"
                                        + jobId)
                        .then()
                        .statusCode(200)
                        .extract()
                        .as(new TypeRef<Map<String, Object>>() {});
        List<Map<String, Object>> pipelines = castList(overview.get("pipelines"));
        if (pipelines == null || pipelines.isEmpty()) {
            return 0L;
        }
        Map<String, Object> counts = castMap(pipelines.get(0).get("counts"));
        if (counts == null) {
            return 0L;
        }
        Object counter = counts.get(counterKey);
        return counter instanceof Number ? ((Number) counter).longValue() : 0L;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> castList(Object value) {
        return (List<Map<String, Object>>) value;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castMap(Object value) {
        return (Map<String, Object>) value;
    }

    private String getRestBaseUrl() {
        return HOST + server.getMappedPort(8080);
    }
}
