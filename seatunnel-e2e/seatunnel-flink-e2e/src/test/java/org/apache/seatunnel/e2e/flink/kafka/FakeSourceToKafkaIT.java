package org.apache.seatunnel.e2e.flink.kafka;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;

public class FakeSourceToKafkaIT extends FlinkContainer {
    private static final String KAFKA_DOCKER_IMAGE = "bitnami/kafka:2.4.1";
    private static final String ZOOKEEPER_DOCKER_IMAGE = "bitnami/zookeeper:latest";
    private static final Logger LOGGER = LoggerFactory.getLogger(FakeSourceToKafkaIT.class);
    private GenericContainer<?> kafkaServer;
    private GenericContainer<?> zookeeperServer;
    private Consumer<byte[], byte[]> consumer;

    @Before
    @SuppressWarnings("magicnumber")
    public void startKafkaContainer() throws InterruptedException {
        zookeeperServer = new GenericContainer<>(ZOOKEEPER_DOCKER_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases("zookeeper")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withEnv("ALLOW_ANONYMOUS_LOGIN", "yes");
        zookeeperServer.setPortBindings(Lists.newArrayList("2181:2181"));
        Startables.deepStart(Stream.of(zookeeperServer)).join();

        LOGGER.info("Zookeeper container started");
        // wait for zookeeper fully start
        Thread.sleep(5000L);

        kafkaServer = new GenericContainer<>(KAFKA_DOCKER_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases("kafka")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withEnv("ALLOW_PLAINTEXT_LISTENER", "yes")
                .withEnv("KAFKA_CFG_ZOOKEEPER_CONNECT", "zookeeper:2181");

        kafkaServer.setPortBindings(Lists.newArrayList("9092:9092"));

        Startables.deepStart(Stream.of(kafkaServer)).join();
        LOGGER.info("Kafka container started");
        // wait for Kafka fully start
        Thread.sleep(5000L);

        createKafkaConsumer();
    }

    /**
     * Test send csv formatted msg to kafka from fake source by flink batch mode.
     */
    @Test
    @SuppressWarnings("magicnumber")
    public void testFakeSourceToKafkaSinkWithCsvFormat() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/kafka/fakesource_to_kafka_with_csv_format.conf");
        Assert.assertEquals(0, execResult.getExitCode());

        consumer.subscribe(Arrays.asList("test_1"));
        final ConsumerRecords<byte[], byte[]> poll = consumer.poll(3000L);

        Assert.assertEquals(3, poll.count());
        poll.forEach(record -> {
            final String[] split = new String(record.value()).split(";");
            Assert.assertEquals(3, split.length);
        });

        consumer.unsubscribe();
    }

    /**
     * Test send json formatted msg to kafka from fake source by flink batch mode.
     */
    @Test
    @SuppressWarnings("magicnumber")
    public void testFakeSourceToKafkaSinkWithJsonFormat() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/kafka/fakesource_to_kafka_with_json_format.conf");
        Assert.assertEquals(0, execResult.getExitCode());

        consumer.subscribe(Arrays.asList("test_2"));
        final ConsumerRecords<byte[], byte[]> poll = consumer.poll(2000L);

        Assert.assertEquals(2, poll.count());
        consumer.unsubscribe();
    }

    private void createKafkaConsumer() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("group.id", "test");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.offset.commit", "false");

        consumer = new KafkaConsumer<>(props);
    }

    @After
    public void closeClickhouseContainer() {
        if (kafkaServer != null) {
            kafkaServer.stop();
        }
        if (consumer != null) {
            consumer.close();
        }
    }
}
