package org.apache.seatunnel.e2e.connector.prometheus;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Optional;
import java.util.stream.Stream;

public class PrometheusIT extends TestSuiteBase implements TestResource {

    private static final String TMP_DIR = "/tmp";

    private static final String successCount = "Total Write Count         :                   2";

    private static final String IMAGE = "mockserver/mockserver:5.14.0";

    private GenericContainer<?> mockserverContainer;

    @BeforeAll
    @Override
    public void startUp() {
        Optional<URL> resource =
                Optional.ofNullable(PrometheusIT.class.getResource(getMockServerConfig()));
        this.mockserverContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("mockserver")
                        .withExposedPorts(9090)
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
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (mockserverContainer != null) {
            mockserverContainer.stop();
        }
    }

    @TestTemplate
    public void testSourceToAssertSink(TestContainer container)
            throws IOException, InterruptedException {

        // http github
        Container.ExecResult execResult2 =
                container.executeJob("/prometheus_instant_json_to_assert.conf");
        Assertions.assertEquals(0, execResult2.getExitCode());
    }

    public String getMockServerConfig() {
        return "/mockserver-config.json";
    }
}
