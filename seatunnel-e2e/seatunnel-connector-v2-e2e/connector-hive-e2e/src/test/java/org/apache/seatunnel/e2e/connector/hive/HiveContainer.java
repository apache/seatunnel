package org.apache.seatunnel.e2e.connector.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public class HiveContainer extends GenericContainer<HiveContainer> {
    public static final String IMAGE = "apache/hive";
    public static final String DEFAULT_TAG = "3.1.3";

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse(IMAGE);

    public static final int HIVE_SERVER_PORT = 10000;

    public static final int HMS_PORT = 9083;

    private static final String SERVICE_NAME_ENV = "SERVICE_NAME";

    private static final String DRIVER_CLASS_NAME = "org.apache.hive.jdbc.HiveDriver";

    public HiveContainer(Role role) {
        super(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
        this.addExposedPorts(role.exposePort);
        this.addEnv(SERVICE_NAME_ENV, role.serviceName);
        this.setWaitStrategy(role.waitStrategy);
        this.withLogConsumer(
                new Slf4jLogConsumer(
                        DockerLoggerFactory.getLogger(
                                DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG).toString())));
    }

    public static HiveContainer hmsStandalone() {
        return new HiveContainer(Role.HMS_STANDALONE);
    }

    public static HiveContainer hiveServer() {
        return new HiveContainer(Role.HIVE_SERVER_WITH_EMBEDDING_HMS);
    }

    public String getMetastoreUri() {
        return String.format("thrift://%s:%s", getHost(), getMappedPort(HMS_PORT));
    }

    public String getHiveJdbcUri() {
        return String.format(
                "jdbc:hive2://%s:%s/default", getHost(), getMappedPort(HIVE_SERVER_PORT));
    }

    public HiveMetaStoreClient createMetaStoreClient() throws MetaException {
        HiveConf conf = new HiveConf();
        conf.set("hive.metastore.uris", getMetastoreUri());

        return new HiveMetaStoreClient(conf);
    }

    public Connection getConnection()
            throws ClassNotFoundException, InstantiationException, IllegalAccessException,
                    SQLException {
        Driver driver = loadHiveJdbcDriver();

        return driver.connect(getHiveJdbcUri(), getJdbcConnectionConfig());
    }

    public Driver loadHiveJdbcDriver()
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        return (Driver) Class.forName(DRIVER_CLASS_NAME).newInstance();
    }

    public Properties getJdbcConnectionConfig() {
        Properties props = new Properties();

        return props;
    }

    public enum Role {
        HIVE_SERVER_WITH_EMBEDDING_HMS(
                "hiveserver2", HIVE_SERVER_PORT, Wait.forLogMessage(".*Starting HiveServer2.*", 1)),
        HMS_STANDALONE(
                "metastore", HMS_PORT, Wait.forLogMessage(".*Starting Hive Metastore Server.*", 1));

        private final String serviceName;
        private final int exposePort;
        private final WaitStrategy waitStrategy;

        Role(String serviceName, int exposePort, WaitStrategy waitStrategy) {
            this.serviceName = serviceName;
            this.exposePort = exposePort;
            this.waitStrategy = waitStrategy;
        }
    }
}
