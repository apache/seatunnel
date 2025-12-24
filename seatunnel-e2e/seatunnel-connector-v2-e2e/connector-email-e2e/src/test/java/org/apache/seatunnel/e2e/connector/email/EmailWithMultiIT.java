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
import org.apache.seatunnel.e2e.common.container.TestContainer;

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

import javax.mail.Flags;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Store;

import java.io.IOException;
import java.util.Properties;
import java.util.stream.Stream;

@Slf4j
public class EmailWithMultiIT extends TestSuiteBase implements TestResource {
    private static final String IMAGE = "greenmail/standalone";
    private static final String HOST = "email-e2e";
    private static final int STMP_PORT = 3025;
    private static final int IMAP_PORT = 3143;

    private GenericContainer<?> smtpContainer;

    @BeforeAll
    @Override
    public void startUp() {
        this.smtpContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withExposedPorts(STMP_PORT, IMAP_PORT)
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
    public void testEmailSink(TestContainer container) throws Exception {
        Container.ExecResult textWriteResult = container.executeJob("/fake_to_email.conf");
        testEMailSuccess(1, "receiver-1@example.com", "receiver-2@example.com");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
    }

    @TestTemplate
    public void testMultipleTableEmailSink(TestContainer container) throws Exception {
        Container.ExecResult textWriteResult = container.executeJob("/fake_to_multiemailsink.conf");
        testEMailSuccess(2, "receiver-3@example.com", "receiver-4@example.com");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
    }

    private Session setupImap() {
        log.info("in setupImap");
        Properties props = new Properties();
        props.setProperty("mail.store.protocol", "imap");
        props.put("mail.imap.host", smtpContainer.getHost());
        props.put("mail.imap.port", smtpContainer.getMappedPort(IMAP_PORT));
        props.put("mail.imap.localaddress", smtpContainer.getHost());
        return Session.getInstance(props, null);
    }

    private void testEMailSuccess(int receivedNum, String... users) throws Exception {
        Session sessionIMAP = setupImap();
        for (String user : users) {
            Store store = sessionIMAP.getStore("imap");
            store.connect(
                    smtpContainer.getHost(), smtpContainer.getMappedPort(IMAP_PORT), user, "");
            if (store.isConnected()) {
                log.info("IMAP is connected");
                Folder folder = store.getFolder("INBOX");
                if (folder != null) {
                    // Open the folder in read/write mode
                    folder.open(Folder.READ_WRITE);

                    Message[] messages = folder.getMessages();
                    int unreadCount = 0;

                    for (Message message : messages) {
                        // Process only unread mail
                        if (!message.isSet(Flags.Flag.SEEN)) {
                            unreadCount++;
                            // Mark as read
                            message.setFlag(Flags.Flag.SEEN, true);
                        }
                    }

                    log.info("mail messages.length: {}", unreadCount);
                    Assertions.assertEquals(receivedNum, unreadCount);
                }
            } else {
                log.info("IMAP is not connected");
            }
        }
    }

    @Disabled("Email authentication address and authentication information need to be configured")
    public void testOwnEmailSink(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult textReadResult = container.executeJob("/fake_to_email_test.conf");
        Assertions.assertEquals(0, textReadResult.getExitCode());
    }
}
