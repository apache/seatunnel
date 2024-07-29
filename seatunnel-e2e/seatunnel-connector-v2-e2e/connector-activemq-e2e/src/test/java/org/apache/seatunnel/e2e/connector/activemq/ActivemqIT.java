package org.apache.seatunnel.e2e.connector.activemq;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class ActivemqIT extends TestSuiteBase {

    public GenericContainer<?> activeMQContainer =
            new GenericContainer<>(DockerImageName.parse("apache/activemq:classic"))
                    .withExposedPorts(61616)
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("e2e.json"), "e2e.json");

    @AfterEach
    public void tearDown() {
        // Cleaning up resources
        activeMQContainer.close();
    }

    @TestTemplate
    public void testActivemqFakeSource(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("fake_source_to_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testActivemqLocalFileSource(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("localfile_source_to_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
