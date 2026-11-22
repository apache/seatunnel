/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.jms;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import lombok.extern.slf4j.Slf4j;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.await;

/**
 * Runs the Jms source against an ActiveMQ Artemis broker with the Artemis JMS client placed in
 * {@code plugins/connector-jms/}, as the documentation tells users to install a client.
 */
@Slf4j
public class JmsIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "apache/activemq-artemis:2.43.0";
    private static final String BROKER_HOST = "artemis-host";
    private static final String USER = "artemis";
    private static final String PASSWORD = "artemis";
    private static final String PLUGIN_DIR = "/tmp/seatunnel/plugins/connector-jms";

    /**
     * Turns off the broker's disk usage check. By default Artemis blocks producers once the disk
     * holding its data is 90% full, which a CI runner or a developer machine can reach.
     */
    private static final String BROKER_PROPERTIES = "maxDiskUsage=-1\nminDiskFree=-1\n";

    private static final long BROKER_CALL_TIMEOUT_SECONDS = 30;

    /** The runtime jars of artemis-jms-client 2.19.1, as listed in the connector documentation. */
    private static final List<String> ARTEMIS_CLIENT_JARS =
            Arrays.asList(
                    "artemis-jms-client-2.19.1.jar",
                    "artemis-core-client-2.19.1.jar",
                    "artemis-commons-2.19.1.jar",
                    "artemis-selector-2.19.1.jar",
                    "jakarta.jms-api-2.0.3.jar",
                    "jakarta.json-api-1.1.6.jar",
                    "johnzon-core-0.9.5.jar",
                    "jboss-logging-3.4.2.Final.jar",
                    "jgroups-3.6.13.Final.jar",
                    "commons-beanutils-1.9.4.jar",
                    "commons-collections-3.2.2.jar",
                    "commons-logging-1.2.jar",
                    "netty-buffer-4.1.73.Final.jar",
                    "netty-codec-4.1.73.Final.jar",
                    "netty-codec-http-4.1.73.Final.jar",
                    "netty-codec-socks-4.1.73.Final.jar",
                    "netty-common-4.1.73.Final.jar",
                    "netty-handler-4.1.73.Final.jar",
                    "netty-handler-proxy-4.1.73.Final.jar",
                    "netty-resolver-4.1.73.Final.jar",
                    "netty-transport-4.1.73.Final.jar",
                    "netty-transport-classes-epoll-4.1.73.Final.jar",
                    "netty-transport-classes-kqueue-4.1.73.Final.jar",
                    "netty-transport-native-epoll-4.1.73.Final-linux-x86_64.jar",
                    "netty-transport-native-kqueue-4.1.73.Final-osx-x86_64.jar",
                    "netty-transport-native-unix-common-4.1.73.Final.jar",
                    "netty-tcnative-classes-2.0.46.Final.jar");

    private GenericContainer<?> broker;
    private ActiveMQConnectionFactory factory;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult mkdir =
                        container.execInContainer("bash", "-c", "mkdir -p " + PLUGIN_DIR);
                Assertions.assertEquals(0, mkdir.getExitCode(), mkdir.getStderr());
                for (String jar : ARTEMIS_CLIENT_JARS) {
                    container.copyFileToContainer(
                            MountableFile.forHostPath(
                                    DependencyJar.staged("artemis-jms-client/" + jar).path()),
                            PLUGIN_DIR + "/" + jar);
                }
            };

    @BeforeAll
    @Override
    public void startUp() {
        broker =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(BROKER_HOST)
                        .withEnv("ARTEMIS_USER", USER)
                        .withEnv("ARTEMIS_PASSWORD", PASSWORD)
                        .withCopyToContainer(
                                Transferable.of(BROKER_PROPERTIES),
                                "/var/lib/artemis-instance/etc-override/broker.properties")
                        .withExposedPorts(61616)
                        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                        .waitingFor(
                                Wait.forLogMessage(".*AMQ221007.*", 1)
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        broker.start();
        factory =
                new ActiveMQConnectionFactory(
                        "tcp://" + broker.getHost() + ":" + broker.getMappedPort(61616));
        factory.setUser(USER);
        factory.setPassword(PASSWORD);
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (factory != null) {
            factory.close();
        }
        if (broker != null) {
            broker.close();
        }
    }

    /**
     * Zeta: a row is emitted, the message stays on the broker until a checkpoint completes, and is
     * then committed.
     */
    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Uses the Zeta job status, checkpoint and cancel APIs")
    public void testMessagesAreCommittedAfterACompletedCheckpoint(TestContainer container)
            throws Exception {
        String queue = "zeta-commit-" + UUID.randomUUID();
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> job =
                runAsync(container, "/jms_source_to_console.conf", jobId, queue);
        try {
            awaitConsumers(queue, 2, job);
            String value = "committed-" + UUID.randomUUID();
            publish(queue, value);
            awaitServerLog(container, value, job);
            long completed = container.getCompletedCheckpointCount(jobId);
            await().atMost(2, TimeUnit.MINUTES)
                    .untilAsserted(
                            () -> {
                                assertRunning(job);
                                Assertions.assertTrue(
                                        container.getCompletedCheckpointCount(jobId) > completed);
                            });
            await().atMost(1, TimeUnit.MINUTES)
                    .untilAsserted(() -> Assertions.assertEquals(0, messageCount(queue)));
        } finally {
            cancel(container, jobId, job);
        }
        Assertions.assertNull(receive(queue), "the committed message was redelivered");
    }

    /**
     * Zeta: without a completed checkpoint nothing is committed. The emitted message stays on the
     * broker and is redelivered after the job is cancelled.
     */
    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason = "Uses the Zeta job status and cancel APIs")
    public void testMessagesAreNotCommittedWithoutACompletedCheckpoint(TestContainer container)
            throws Exception {
        String queue = "zeta-uncommitted-" + UUID.randomUUID();
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> job =
                runAsync(container, "/jms_source_without_checkpoint.conf", jobId, queue);
        String value = "uncommitted-" + UUID.randomUUID();
        try {
            awaitConsumers(queue, 1, job);
            publish(queue, value);
            awaitServerLog(container, value, job);
            Assertions.assertEquals(1, messageCount(queue));
            Assertions.assertEquals(0, container.getCompletedCheckpointCount(jobId));
        } finally {
            cancel(container, jobId, job);
        }
        TextMessage redelivered = receive(queue);
        Assertions.assertNotNull(redelivered, "the uncommitted message was lost");
        Assertions.assertTrue(redelivered.getText().contains(value));
    }

    /**
     * Flink: a row is emitted and the message is committed once a Flink checkpoint completes. The
     * job keeps running until the container stops, as the Flink container has no cancel API.
     */
    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SEATUNNEL, EngineType.SPARK},
            disabledReason = "Covered by the Zeta tests above")
    public void testMessagesAreCommittedAfterAFlinkCheckpoint(TestContainer container)
            throws Exception {
        String queue = "flink-commit-" + UUID.randomUUID();
        CompletableFuture<Container.ExecResult> job =
                runAsync(container, "/jms_source_to_console.conf", null, queue);
        awaitConsumers(queue, 2, job);
        String value = "flink-" + UUID.randomUUID();
        publish(queue, value);
        awaitServerLog(container, value, job);
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            assertRunning(job);
                            Assertions.assertEquals(0, messageCount(queue));
                        });
    }

    private static CompletableFuture<Container.ExecResult> runAsync(
            TestContainer container, String config, String jobId, String queue) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        String variable = "queue=" + queue;
                        return jobId == null
                                ? container.executeJob(config, Arrays.asList(variable))
                                : container.executeJob(config, jobId, variable);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                });
    }

    private static void assertRunning(CompletableFuture<Container.ExecResult> job)
            throws Exception {
        if (job.isDone()) {
            Assertions.fail("The streaming job ended: " + job.get().getStderr());
        }
    }

    private static void awaitServerLog(
            TestContainer container, String value, CompletableFuture<Container.ExecResult> job) {
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            assertRunning(job);
                            Assertions.assertTrue(container.getServerLogs().contains(value));
                        });
    }

    private static void cancel(
            TestContainer container, String jobId, CompletableFuture<Container.ExecResult> job)
            throws Exception {
        if (!job.isDone()) {
            Container.ExecResult cancelled = container.cancelJob(jobId);
            Assertions.assertEquals(0, cancelled.getExitCode(), cancelled.getStderr());
        }
        job.get(2, TimeUnit.MINUTES);
    }

    /** Waits until the job's readers are connected, so that the test's messages reach them. */
    private void awaitConsumers(
            String queue, int consumers, CompletableFuture<Container.ExecResult> job) {
        await().atMost(3, TimeUnit.MINUTES)
                .pollInterval(Duration.ofSeconds(1))
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            assertRunning(job);
                            Assertions.assertEquals(
                                    consumers,
                                    ((Number) queueAttribute(queue, "consumerCount")).intValue());
                        });
    }

    private void publish(String queue, String value) throws Exception {
        withTimeout(
                () -> {
                    Connection connection = factory.createConnection();
                    try {
                        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        MessageProducer producer =
                                session.createProducer(session.createQueue(queue));
                        producer.send(session.createTextMessage("{\"value\": \"" + value + "\"}"));
                        return null;
                    } finally {
                        connection.close();
                    }
                });
    }

    private TextMessage receive(String queue) throws Exception {
        Connection connection = factory.createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(queue));
            return (TextMessage) consumer.receive(10_000);
        } finally {
            connection.close();
        }
    }

    /** Messages on the queue, including messages delivered to a consumer and not committed. */
    private long messageCount(String queue) throws Exception {
        return ((Number) queueAttribute(queue, "messageCount")).longValue();
    }

    private Object queueAttribute(String queue, String attribute) throws Exception {
        return withTimeout(() -> requestQueueAttribute(queue, attribute));
    }

    private Object requestQueueAttribute(String queue, String attribute) throws Exception {
        QueueConnection connection = factory.createQueueConnection();
        try {
            connection.start();
            QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue management = session.createQueue("activemq.management");
            QueueRequestor requestor = new QueueRequestor(session, management);
            Message request = session.createMessage();
            JMSManagementHelper.putAttribute(request, ResourceNames.QUEUE + queue, attribute);
            Message reply = requestor.request(request);
            Assertions.assertTrue(
                    JMSManagementHelper.hasOperationSucceeded(reply),
                    "management request failed for " + queue);
            return JMSManagementHelper.getResult(reply);
        } finally {
            connection.close();
        }
    }

    /**
     * Runs a broker call with a time limit, so that a broker which stops accepting messages fails
     * the test instead of blocking it.
     */
    private static <T> T withTimeout(Callable<T> call) throws Exception {
        CompletableFuture<T> future =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return call.call();
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        });
        try {
            return future.get(BROKER_CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new IllegalStateException(
                    "The broker did not answer within " + BROKER_CALL_TIMEOUT_SECONDS + " s", e);
        }
    }
}
