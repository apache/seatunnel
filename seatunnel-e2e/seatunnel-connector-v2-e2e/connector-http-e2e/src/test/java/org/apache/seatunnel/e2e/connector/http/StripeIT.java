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

package org.apache.seatunnel.e2e.connector.http;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.util.stream.Stream;

class StripeIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "stripe/stripe-mock:v0.202.0";
    private static final int HTTP_PORT = 12111;

    private GenericContainer<?> stripeMock;

    @BeforeAll
    @Override
    public void startUp() {
        DockerImageName image = DockerImageName.parse(IMAGE);
        stripeMock =
                new GenericContainer<>(image)
                        .withNetwork(NETWORK)
                        .withNetworkAliases("stripe-mock")
                        .withExposedPorts(HTTP_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                image.asCanonicalNameString())))
                        .waitingFor(
                                Wait.forHttp("/v1/payment_intents")
                                        .forPort(HTTP_PORT)
                                        .withHeader("Authorization", "Bearer sk_test_123")
                                        .forStatusCode(200));
        Startables.deepStart(Stream.of(stripeMock)).join();
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (stripeMock != null) {
            stripeMock.close();
        }
    }

    @TestTemplate
    void testPaymentIntentsSource(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/stripe_payment_intents_to_assert.conf");

        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }
}
