package org.apache.seatunnel.flink.source;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;
import org.junit.Test;

import java.io.File;

class FileSourceTest {

    private FileSource fileSource = new FileSource();

    @Test
    public void getData() {
        FlinkEnvironment flinkEnvironment = createFlinkEnvironment();

        Config config = createConfig("");
        fileSource.setConfig(config);
        fileSource.prepare(flinkEnvironment);
        DataSet<Row> data = fileSource.getData(flinkEnvironment);
        // assert
    }

    private FlinkEnvironment createFlinkEnvironment() {
        return new FlinkEnvironment();
    }

    private Config createConfig(String configFilePath) {
        return ConfigFactory
                .parseFile(new File(configFilePath))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));
    }
}