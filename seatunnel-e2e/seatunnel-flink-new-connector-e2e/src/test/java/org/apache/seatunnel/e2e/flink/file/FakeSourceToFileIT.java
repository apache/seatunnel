package org.apache.seatunnel.e2e.flink.file;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class FakeSourceToFileIT extends FlinkContainer {
    @Test
    @SuppressWarnings("magicnumber")
    public void testFakeSourceToFileSink() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/file/fakesource_to_file.conf");
        Assert.assertEquals(0, execResult.getExitCode());
    }
}
