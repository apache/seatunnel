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

package org.apache.seatunnel.e2e.connector.email;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.stream.Stream;

@Slf4j
public class EmailWithMultiIT extends TestSuiteBase implements TestResource {
    private static final String IMAGE = "greenmail/standalone";
    private static final String HOST = "email-e2e";
    private static final int PORT = 3025;

    private GenericContainer<?> smtpContainer;

    @BeforeAll
    @Override
    public void startUp() {
        this.smtpContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withExposedPorts(PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(LoggerFactory.getLogger("email-service")));
        Startables.deepStart(Stream.of(smtpContainer)).join();
        log.info("SMTP container started");
    }

    @Override
    public void tearDown() throws Exception {
        if (smtpContainer != null) {
            smtpContainer.stop();
        }
    }

    @TestTemplate
    public void testEmailSink(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult textWriteResult = container.executeJob("/fake_to_email.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK},
            disabledReason = "Currently FLINK do not support multi-table")
    public void testMultipleTableEmailSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult textWriteResult = container.executeJob("/fake_to_multiemailsink.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
    }

    @Disabled("Email authentication address and authentication information need to be configured")
    public void testOwnEmailSink(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult textReadResult = container.executeJob("/fake_to_email_test.conf");
        Assertions.assertEquals(0, textReadResult.getExitCode());
    }
}
