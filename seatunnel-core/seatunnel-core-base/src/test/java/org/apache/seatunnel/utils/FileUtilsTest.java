package org.apache.seatunnel.utils;

import org.apache.seatunnel.command.SparkCommandArgs;
import org.apache.seatunnel.common.config.DeployMode;

import org.junit.Assert;
import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtilsTest {

    @Test
    public void getConfigPath() throws URISyntaxException {
        // test client mode.
        SparkCommandArgs sparkCommandArgs = new SparkCommandArgs();
        sparkCommandArgs.setDeployMode(DeployMode.CLIENT);
        Path expectConfPath = Paths.get(FileUtilsTest.class.getResource("/flink.batch.conf").toURI());
        sparkCommandArgs.setConfigFile(expectConfPath.toString());
        Assert.assertEquals(expectConfPath, FileUtils.getConfigPath(sparkCommandArgs));

        // test cluster mode
        sparkCommandArgs.setDeployMode(DeployMode.CLUSTER);
        Assert.assertEquals("flink.batch.conf", FileUtils.getConfigPath(sparkCommandArgs).toString());
    }
}