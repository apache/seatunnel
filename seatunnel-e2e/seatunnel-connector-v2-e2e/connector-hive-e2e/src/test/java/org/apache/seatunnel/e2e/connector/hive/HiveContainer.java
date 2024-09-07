package org.apache.seatunnel.e2e.connector.hive;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

public class HiveContainer extends GenericContainer<HiveContainer> {
    public static final String IMAGE = "apache/hive";
    public static final String DEFAULT_TAG = "3.1.3";

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse(IMAGE);

    public static final int HIVE_SERVER_PORT = 10000;

    public static final int HIVE_SERVER_WEBUI_PORT = 10002;

    public static final int HMS_PORT = 9083;

    public static final String HIVE_CUSTOM_CONF_DIR_ENV = "HIVE_CUSTOM_CONF_DIR";

    private static final String SERVICE_NAME_ENV = "SERVICE_NAME";

    public HiveContainer() {
        this(Role.HIVE_SERVER_WITH_EMBEDDING_HMS);
    }

    public HiveContainer(Role role) {
        super(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
        this.addExposedPorts(HMS_PORT);
        this.addEnv(SERVICE_NAME_ENV, role.serviceName);
        this.setWaitStrategy(role.waitStrategy);
        this.withLogConsumer(
                new Slf4jLogConsumer(
                        DockerLoggerFactory.getLogger(
                                DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG).toString())));
    }

    public HiveContainer withCustomHiveConfigPath(String location) {
        return withEnv(HIVE_CUSTOM_CONF_DIR_ENV, location);
    }

    public enum Role {
        HIVE_SERVER_WITH_EMBEDDING_HMS(
                "hiveserver2",
                new int[] {HIVE_SERVER_PORT, HIVE_SERVER_WEBUI_PORT},
                Wait.forLogMessage(".*Starting HiveServer2.*", 1)),
        HMS_STANDALONE(
                "metastore",
                new int[] {HMS_PORT},
                Wait.forLogMessage(".*Starting Hive Metastore Server.*", 1));

        private final String serviceName;
        private final int[] ports;
        private final WaitStrategy waitStrategy;

        Role(String serviceName, int[] ports, WaitStrategy waitStrategy) {
            this.serviceName = serviceName;
            this.ports = ports;
            this.waitStrategy = waitStrategy;
        }
    }
}
