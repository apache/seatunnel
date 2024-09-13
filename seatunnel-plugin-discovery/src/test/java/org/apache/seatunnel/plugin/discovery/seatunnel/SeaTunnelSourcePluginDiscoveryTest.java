package org.apache.seatunnel.plugin.discovery.seatunnel;

import com.google.common.collect.Lists;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class SeaTunnelSourcePluginDiscoveryTest {

    protected String originSeatunnelHome = null;

    protected static final String seatunnelHome =
            SeaTunnelSourcePluginDiscoveryTest.class.getResource("/duplicate").getPath();
    protected static final List<Path> pluginJars =
            Lists.newArrayList(
                    Paths.get(seatunnelHome, "connectors", "connector-http-jira.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-http.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-kafka.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-kafka-alcs.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-kafka-blcs.jar"));
}
