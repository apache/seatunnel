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

package org.apache.seatunnel.connectors.seatunnel.jms;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnection;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ActiveMQ Classic and ActiveMQ Artemis brokers embedded in the test JVM, reached over TCP: Classic
 * through OpenWire, Artemis through its core protocol and through AMQP 1.0 with Qpid JMS.
 */
public final class JmsTestBrokers {

    private static BrokerService classic;
    private static EmbeddedActiveMQ artemis;
    private static int classicPort;
    private static int artemisCorePort;
    private static int artemisAmqpPort;

    private JmsTestBrokers() {}

    public static synchronized void start() throws Exception {
        if (classic != null) {
            return;
        }
        classicPort = freePort();
        artemisCorePort = freePort();
        artemisAmqpPort = freePort();

        classic = new BrokerService();
        classic.setBrokerName("classic");
        classic.setPersistent(false);
        classic.setUseJmx(false);
        classic.setUseShutdownHook(false);
        classic.addConnector("tcp://127.0.0.1:" + classicPort);
        classic.start();
        classic.waitUntilStarted();

        Configuration configuration = new ConfigurationImpl();
        configuration.setPersistenceEnabled(false);
        configuration.setSecurityEnabled(false);
        configuration.setJMXManagementEnabled(false);
        configuration.addAcceptorConfiguration(
                "core", "tcp://127.0.0.1:" + artemisCorePort + "?protocols=CORE");
        configuration.addAcceptorConfiguration(
                "amqp", "tcp://127.0.0.1:" + artemisAmqpPort + "?protocols=AMQP");
        artemis = new EmbeddedActiveMQ();
        artemis.setConfiguration(configuration);
        artemis.start();
    }

    public static synchronized void stop() throws Exception {
        if (artemis != null) {
            artemis.stop();
            artemis = null;
        }
        if (classic != null) {
            classic.stop();
            classic.waitUntilStopped();
            classic = null;
        }
    }

    /** The providers every source test runs against. */
    public static List<Provider> providers() {
        return Arrays.asList(
                classic(),
                new Provider(
                        "ActiveMQ Classic, queuePrefetch 0",
                        "org.apache.activemq.ActiveMQConnectionFactory",
                        properties(
                                "brokerURL",
                                "tcp://127.0.0.1:" + classicPort,
                                "prefetchPolicy.queuePrefetch",
                                "0"),
                        true,
                        JmsTestBrokers::dropClassic),
                artemisCore(),
                new Provider(
                        "ActiveMQ Artemis, AMQP 1.0 through Qpid JMS",
                        "org.apache.qpid.jms.JmsConnectionFactory",
                        properties("remoteURI", "amqp://127.0.0.1:" + artemisAmqpPort),
                        true,
                        JmsTestBrokers::dropArtemis));
    }

    /** ActiveMQ Classic with its default prefetch of 1000. */
    public static Provider classic() {
        return new Provider(
                "ActiveMQ Classic, default prefetch",
                "org.apache.activemq.ActiveMQConnectionFactory",
                properties("brokerURL", "tcp://127.0.0.1:" + classicPort),
                false,
                JmsTestBrokers::dropClassic);
    }

    public static Provider artemisCore() {
        return new Provider(
                "ActiveMQ Artemis, core protocol",
                "org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory",
                properties("brokerURL", "tcp://127.0.0.1:" + artemisCorePort),
                true,
                JmsTestBrokers::dropArtemis);
    }

    public static int artemisCorePort() {
        return artemisCorePort;
    }

    public static int classicPort() {
        return classicPort;
    }

    private static void dropClassic() throws Exception {
        for (org.apache.activemq.broker.Connection connection : classic.getBroker().getClients()) {
            ((TransportConnection) connection).serviceException(new IOException("injected drop"));
        }
    }

    private static void dropArtemis() {
        for (RemotingConnection connection :
                new ArrayList<>(
                        artemis.getActiveMQServer().getRemotingService().getConnections())) {
            connection.fail(new ActiveMQException("injected drop"));
        }
    }

    private static Map<String, String> properties(String... keysAndValues) {
        Map<String, String> properties = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            properties.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return properties;
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /** A JMS provider configured the way a user configures the connector. */
    public static final class Provider {
        private final String name;
        private final String factoryClass;
        private final Map<String, String> factoryProperties;
        private final boolean releasesPrefetchOnConsumerClose;
        private final Dropper dropper;

        private Provider(
                String name,
                String factoryClass,
                Map<String, String> factoryProperties,
                boolean releasesPrefetchOnConsumerClose,
                Dropper dropper) {
            this.name = name;
            this.factoryClass = factoryClass;
            this.factoryProperties = factoryProperties;
            this.releasesPrefetchOnConsumerClose = releasesPrefetchOnConsumerClose;
            this.dropper = dropper;
        }

        public String factoryClass() {
            return factoryClass;
        }

        public Map<String, String> factoryProperties() {
            return new HashMap<>(factoryProperties);
        }

        /**
         * False for ActiveMQ Classic with a non-zero prefetch: its closed consumer keeps the
         * messages it prefetched until the session is committed or rolled back.
         */
        public boolean releasesPrefetchOnConsumerClose() {
            return releasesPrefetchOnConsumerClose;
        }

        /** Fails every client connection on the broker side. */
        public void dropConnections() throws Exception {
            dropper.drop();
        }

        /** A connection factory for test producers and consumers, outside the connector. */
        public ConnectionFactory factory() throws Exception {
            ConnectionFactory factory =
                    (ConnectionFactory) Class.forName(factoryClass).getConstructor().newInstance();
            String url =
                    factoryProperties.containsKey("remoteURI")
                            ? factoryProperties.get("remoteURI")
                            : factoryProperties.get("brokerURL");
            factory.getClass()
                    .getMethod(
                            factoryProperties.containsKey("remoteURI")
                                    ? "setRemoteURI"
                                    : "setBrokerURL",
                            String.class)
                    .invoke(factory, url);
            return factory;
        }

        public void sendText(String queue, String... bodies) throws Exception {
            send(
                    queue,
                    session -> {
                        List<Message> messages = new ArrayList<>();
                        for (String body : bodies) {
                            messages.add(session.createTextMessage(body));
                        }
                        return messages;
                    });
        }

        public void send(String queue, MessageFactory messages) throws Exception {
            Connection connection = factory().createConnection();
            try {
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(session.createQueue(queue));
                for (Message message : messages.create(session)) {
                    producer.send(message);
                }
            } finally {
                connection.close();
            }
        }

        /**
         * Receives and removes every message left on the queue and returns the bodies. Waits up to
         * {@code firstWaitMillis} for the first message and a shorter time for the others, as a
         * provider may redeliver rolled back messages after a delay.
         */
        public List<String> drain(String queue, long firstWaitMillis) throws Exception {
            List<String> bodies = new ArrayList<>();
            Connection connection = factory().createConnection();
            try {
                connection.start();
                Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                MessageConsumer consumer = session.createConsumer(session.createQueue(queue));
                Message message = consumer.receive(firstWaitMillis);
                while (message != null) {
                    bodies.add(body(message));
                    message = consumer.receive(1000);
                }
                session.commit();
            } finally {
                connection.close();
            }
            return bodies;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public static String body(Message message) throws JMSException {
        if (message instanceof TextMessage) {
            return ((TextMessage) message).getText();
        }
        if (message instanceof BytesMessage) {
            BytesMessage bytes = (BytesMessage) message;
            byte[] data = new byte[(int) bytes.getBodyLength()];
            bytes.readBytes(data);
            return new String(data, StandardCharsets.UTF_8);
        }
        return message.getClass().getSimpleName();
    }

    /** Creates the messages to send in a session. */
    public interface MessageFactory {
        List<Message> create(Session session) throws JMSException;
    }

    private interface Dropper {
        void drop() throws Exception;
    }
}
