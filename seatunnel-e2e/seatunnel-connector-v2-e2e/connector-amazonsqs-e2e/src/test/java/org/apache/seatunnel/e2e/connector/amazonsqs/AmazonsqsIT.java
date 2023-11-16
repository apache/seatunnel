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

package org.apache.seatunnel.e2e.connector.amazonsqs;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class AmazonsqsIT extends TestSuiteBase implements TestResource {
    private static final String LOCALSTACK_DOCKER_IMAGE = "localstack/localstack:0.11.2";
    private static final String AMAZONSQS_JOB_CONFIG = "/amazonsqsIT_source_to_sink.conf";
    private static final String AMAZONSQS_CONTAINER_HOST = "sqs-host";
    private static final int AMAZONSQS_CONTAINER_PORT = 4566;
    private static final String SINK_QUEUE = "sink_queue";
    private static final String SOURCE_QUEUE = "source_queue";

    private static final String TEST_MESSAGE = "{\"name\":\"test_name\"}";

    protected SqsClient sqsClient;

    private LocalStackContainer localstack;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        // start a localstack docker container
        localstack =
                new LocalStackContainer()
                        .withServices(LocalStackContainer.Service.SQS)
                        .withEnv("AWS_DEFAULT_REGION", "us-east-1")
                        .withEnv("AWS_ACCESS_KEY_ID", "1234")
                        .withEnv("AWS_SECRET_ACCESS_KEY", "abcd")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(AMAZONSQS_CONTAINER_HOST)
                        .withExposedPorts(AMAZONSQS_CONTAINER_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(LOCALSTACK_DOCKER_IMAGE)));

        localstack.setPortBindings(
                Lists.newArrayList(
                        String.format(
                                "%s:%s", AMAZONSQS_CONTAINER_PORT, AMAZONSQS_CONTAINER_PORT)));
        Startables.deepStart(Stream.of(localstack)).join();

        log.info("localstack container started");
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(this::initializeSqsClient);
    }

    private void initializeSqsClient() {
        // create a sqs client
        sqsClient =
                SqsClient.builder()
                        .endpointOverride(localstack.getEndpoint())
                        .region(Region.US_EAST_1)
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create("1234", "abcd")))
                        .build();

        // create source and sink queue
        sqsClient.createQueue(r -> r.queueName(SOURCE_QUEUE));
        sqsClient.createQueue(r -> r.queueName(SINK_QUEUE));

        // insert message to source queue
        sqsClient.sendMessage(
                r ->
                        r.queueUrl(sqsClient.listQueues().queueUrls().get(0))
                                .messageBody(TEST_MESSAGE));
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (localstack != null) {
            localstack.close();
        }
    }

    @TestTemplate
    public void testAmazonSqs(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob(AMAZONSQS_JOB_CONFIG);
        Assertions.assertEquals(0, execResult.getExitCode());
        assertHasDataAndCompareResult();
    }

    private void assertHasDataAndCompareResult() {
        // check if there is message in sink queue, and compare the sink record with the source
        // record
        // the message is invisible after reception, so don't call it twice.
        List<Message> messages =
                sqsClient
                        .receiveMessage(r -> r.queueUrl(sqsClient.listQueues().queueUrls().get(1)))
                        .messages();
        Assertions.assertEquals(1, messages.size());
        Assertions.assertEquals(TEST_MESSAGE, messages.get(0).body());
    }
}
