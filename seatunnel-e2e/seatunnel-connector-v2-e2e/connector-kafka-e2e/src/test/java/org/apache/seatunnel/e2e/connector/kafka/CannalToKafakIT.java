package org.apache.seatunnel.e2e.connector.kafka;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.util.stream.Stream;

@DisabledOnContainer(value = {}, type = {EngineType.FLINK, EngineType.SPARK})
public class CannalToKafakIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(CannalToKafakIT.class);

    private static GenericContainer<?> CanalServer;

    private static final String CANAL_DOCKER_IMAGE = "chinayin/canal:1.1.6";

    private static final String CANAL_HOST = "canal_e2e";

    private static final int CANAL_PORT = 11111;

    //----------------------------------------------------------------------------
    // kafka
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:latest";

    private static final int KAFKA_PORT = 9093;

    private static final String KAFKA_HOST = "kafkaCluster";

    private static KafkaContainer kafkaContainer;

    //----------------------------------------------------------------------------
    // mysql
    private static final String MYSQL_HOST = "mysql_e2e";

    private static final int MYSQL_PORT = 3306;

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
        new UniqueDatabase(MYSQL_CONTAINER, "inventory", "mysqluser", "mysqlpw");

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        MySqlContainer mySqlContainer = new MySqlContainer(version)
            .withConfigurationOverride("docker/server-gtids/my.cnf")
            .withSetupSQL("docker/setup.sql")
            .withNetwork(NETWORK)
            .withNetworkAliases(MYSQL_HOST)
            .withDatabaseName("inventory")
            .withUsername("st_user")
            .withPassword("seatunnel")
            .withLogConsumer(new Slf4jLogConsumer(LOG));
        mySqlContainer.setPortBindings(com.google.common.collect.Lists.newArrayList(
            String.format("%s:%s", MYSQL_PORT, MYSQL_PORT)));
        return mySqlContainer;
    }

    private static void createCanalContainer() {
        CanalServer = new GenericContainer<>(CANAL_DOCKER_IMAGE)
            .withCopyFileToContainer(MountableFile.forClasspathResource("canal/canal.properties"),"/app/server/conf/canal.properties")
            .withCopyFileToContainer(MountableFile.forClasspathResource("canal/instance.properties"), "/app/server/conf/example/instance.properties")
            .withNetwork(NETWORK)
            .withNetworkAliases(CANAL_HOST)
            .withCommand()
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(CANAL_DOCKER_IMAGE)));
        CanalServer.setPortBindings(com.google.common.collect.Lists.newArrayList(
            String.format("%s:%s", CANAL_PORT, CANAL_PORT)));
    }

    private static void createKafkaContainer(){
        kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
            .withNetwork(NETWORK)
            .withNetworkAliases(KAFKA_HOST)
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
        kafkaContainer.setPortBindings(com.google.common.collect.Lists.newArrayList(
            String.format("%s:%s", KAFKA_PORT, KAFKA_PORT)));
    }

    @BeforeAll
    @Override
    public void startUp() {

        LOG.info("The third stage: Starting Kafka containers...");
        createKafkaContainer();
        Startables.deepStart(Stream.of(kafkaContainer)).join();
        LOG.info("Containers are started");

        LOG.info("The first stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started");

        LOG.info("The first stage: Starting Canal containers...");
        createCanalContainer();
        Startables.deepStart(Stream.of(CanalServer)).join();
        LOG.info("Containers are started");
    }

    @TestTemplate
    public void testDorisSink(TestContainer container) throws IOException, InterruptedException {
        inventoryDatabase.createAndInitialize();
        Container.ExecResult execResult = container.executeJob("/kafkasource_canal_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @Override
    public void tearDown() throws Exception {
        //MYSQL_CONTAINER.close();
    }
}
