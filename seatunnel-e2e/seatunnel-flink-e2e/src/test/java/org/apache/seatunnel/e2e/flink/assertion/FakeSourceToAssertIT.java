package org.apache.seatunnel.e2e.flink.assertion;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class FakeSourceToAssertIT extends FlinkContainer {

    @Test
    public void testFakeSourceToConsoleSink() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/assertion/fakesource_to_assert.conf");
        Assert.assertEquals(0, execResult.getExitCode());
    }
}
