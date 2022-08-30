package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.e2e.flink.local.FlinkLocalContainer;

import org.junit.jupiter.api.Test;

/**
 * Created 2022/8/30
 */
public class FakeSourceITTest {

    @Test
    public void testFakeConsole() throws CommandException {
        FlinkLocalContainer flinkLocalContainer = FlinkLocalContainer.builder()
            .build();

        flinkLocalContainer.executeSeaTunnelFlinkJob("fake/fakesource_to_console.conf");
    }
}
