package org.apache.seatunnel.core.starter.seatunnel.jvm;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.io.Resources;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Paths;
import java.util.List;

class JvmOptionsParserTest {

    @SneakyThrows
    @Test
    void parse() {
        URI configPath = Resources.getResource("").toURI();
        final JvmOptionsParser parser = new JvmOptionsParser();
        List<String> jvmOptions = parser.readJvmOptionsFiles(Paths.get(configPath));
        String[] expectJvmOptions = {
            "-XX:+UseConcMarkSweepGC",
            "-XX:CMSInitiatingOccupancyFraction=75",
            "-XX:+UseCMSInitiatingOccupancyOnly",
            "-XX:+PrintGCDetails",
            "-XX:+PrintGCDateStamps",
            "-XX:+PrintTenuringDistribution",
            "-XX:+PrintGCApplicationStoppedTime",
            "-XX:+UseGCLogFileRotation",
            "-XX:NumberOfGCLogFiles=32",
            "-XX:GCLogFileSize=64m"};
        assertArrayEquals(expectJvmOptions, jvmOptions.toArray(), "Expected and actual jvmOptions is not equal");
    }
}