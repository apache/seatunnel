package org.apache.seatunnel.e2e.flink.v2.elasticsearch;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

public class FakeSourceToElasticsearchIT extends FlinkContainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(FakeSourceToElasticsearchIT.class);

    private ElasticsearchContainer container;

    @SuppressWarnings({"checkstyle:MagicNumber", "checkstyle:Indentation"})
    @BeforeEach
    public void startElasticsearchContainer() throws InterruptedException {
        container = new ElasticsearchContainer(DockerImageName.parse("elasticsearch:6.8.23").asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch")).withNetwork(NETWORK).withNetworkAliases("elasticsearch").withLogConsumer(new Slf4jLogConsumer(LOGGER));
        container.start();
        LOGGER.info("Elasticsearch container started");
        Thread.sleep(5000L);

    }

    @Test
    public void testFakeSourceToConsoleSink() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/elasticsearch/fakesource_to_elasticsearch.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        container.stop();
    }

    @AfterEach
    public void closeContainer() {
        if (container != null) {
            container.stop();
        }
    }
}
