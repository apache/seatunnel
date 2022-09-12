package org.apache.seatunnel.e2e.flink.v2.kafka;

import org.apache.seatunnel.e2e.flink.FlinkContainer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class KafkaIT extends FlinkContainer {

    /**
     * kafka source -> console sink
     */
    @Test
    public void testFakeSourceToLocalFileText() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/kafka/kafka_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
