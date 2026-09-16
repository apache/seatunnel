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

package org.apache.seatunnel.connectors.seatunnel.activemq.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.activemq.source.ActivemqSourceEnumerator.Split;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.transport.InactivityIOException;
import org.apache.activemq.transport.TransportDisposedIOException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.QUEUE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.URI;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSourceOptions.MAX_IN_FLIGHT_MESSAGES;

/**
 * Synchronous queue reader with checkpoint-scoped individual acknowledgements. JMS calls and local
 * state are serialized; record emission also holds the engine checkpoint lock. Only logical
 * consumer slots are serialized: a new connection receives unacknowledged messages again from the
 * broker.
 */
public class ActivemqSourceReader implements SourceReader<SeaTunnelRow, Split> {
    private final ReadonlyConfig config;
    private final DeserializationSchema<SeaTunnelRow> deserializer;
    private final ConnectionFactory connectionFactory;
    private final Map<String, Split> splits = new LinkedHashMap<>();
    private final Deque<PendingMessage> pending = new ArrayDeque<>();
    // Sequence watermarks avoid copying all pending message handles for every checkpoint.
    private final NavigableMap<Long, Long> checkpoints = new TreeMap<>();
    private final AtomicReference<Exception> failure = new AtomicReference<>();
    private final int maxInFlight;
    private long sequence;
    private volatile boolean closed;
    private volatile Connection connection;
    private Session session;
    private MessageConsumer consumer;

    public ActivemqSourceReader(
            ReadonlyConfig config, DeserializationSchema<SeaTunnelRow> deserializer) {
        this(config, deserializer, createConnectionFactory(config));
    }

    ActivemqSourceReader(
            ReadonlyConfig config,
            DeserializationSchema<SeaTunnelRow> deserializer,
            ConnectionFactory connectionFactory) {
        this.config = config;
        this.deserializer = deserializer;
        this.connectionFactory = connectionFactory;
        this.maxInFlight = config.get(MAX_IN_FLIGHT_MESSAGES);
    }

    private static ActiveMQConnectionFactory createConnectionFactory(ReadonlyConfig config) {
        ActivemqSourceFactory.validate(config);
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory(config.get(URI) + "?connectionTimeout=10000");
        factory.getPrefetchPolicy().setQueuePrefetch(config.get(MAX_IN_FLIGHT_MESSAGES));
        factory.setOptimizeAcknowledge(false);
        // The checkpoint is already durable. One-way ACKs avoid an untimed broker request per
        // message; a lost ACK can only cause redelivery, not loss of checkpointed data.
        factory.setSendAcksAsync(true);
        factory.setWatchTopicAdvisories(false);
        factory.setTrustAllPackages(false);
        factory.setTrustedPackages(Collections.emptyList());
        factory.setCloseTimeout(5000);
        factory.setConnectResponseTimeout(10000);
        return factory;
    }

    /** Connections are opened lazily once this reader receives a consumer slot. */
    @Override
    public void open() {}

    private void openConsumer() throws JMSException {
        if (consumer != null) {
            return;
        }
        try {
            connection =
                    config.getOptional(USERNAME).isPresent()
                            ? connectionFactory.createConnection(
                                    config.get(USERNAME), config.get(PASSWORD))
                            : connectionFactory.createConnection();
            // close() may have observed null while createConnection was in progress.
            if (closed) {
                throw new JMSException("ActiveMQ source was closed during connection creation");
            }
            connection.setExceptionListener(error -> failure.compareAndSet(null, error));
            // CLIENT_ACKNOWLEDGE is cumulative and could acknowledge post-checkpoint records.
            session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
            consumer = session.createConsumer(session.createQueue(config.get(QUEUE_NAME)));
            connection.start();
        } catch (JMSException e) {
            failure.compareAndSet(null, e);
            try {
                close();
            } catch (IOException closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
    }

    /** Emit only from pollNext, never from an asynchronous JMS listener. */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (this) {
            checkFailure();
            if (closed) {
                return;
            }
            if (splits.isEmpty() || pending.size() >= maxInFlight) {
                wait(100);
                return;
            }
        }

        for (int i = 0; i < 32; i++) {
            Message message;
            synchronized (this) {
                checkFailure();
                if (closed || pending.size() >= maxInFlight) {
                    return;
                }
                openConsumer();
                message = i == 0 ? consumer.receive(200) : consumer.receiveNoWait();
            }
            if (message == null) {
                checkFailure();
                return;
            }
            synchronized (output.getCheckpointLock()) {
                synchronized (this) {
                    checkFailure();
                    if (closed) {
                        return;
                    }
                    try {
                        if (!(message instanceof TextMessage)) {
                            throw new IOException(
                                    "ActiveMQ source accepts only JMS TextMessage payloads");
                        }
                        String text = ((TextMessage) message).getText();
                        if (text == null) {
                            throw new IOException(
                                    "ActiveMQ source received a null TextMessage payload");
                        }
                        try {
                            byte[] payload = text.getBytes(StandardCharsets.UTF_8);
                            if (deserializer instanceof JsonDeserializationSchema) {
                                ((JsonDeserializationSchema) deserializer).collect(payload, output);
                            } else {
                                deserializer.deserialize(payload, output);
                            }
                        } catch (Exception error) {
                            // Shared format errors may embed the full queue payload. Neither retain
                            // their causes nor expose their messages through engine task logs.
                            throw new IOException(
                                    "ActiveMQ message deserialization or emission failed ("
                                            + error.getClass().getSimpleName()
                                            + "); check the configured schema and format");
                        }
                        pending.addLast(new PendingMessage(++sequence, message));
                    } catch (Exception e) {
                        failure.compareAndSet(null, e);
                        // No acknowledgement: closing the failed reader makes the message eligible
                        // for redelivery, including partial multi-row deserialization/emission.
                        throw e;
                    }
                }
            }
        }
    }

    @Override
    public synchronized void addSplits(List<Split> assigned) {
        assigned.forEach(split -> splits.putIfAbsent(split.splitId(), split));
        notifyAll();
    }

    @Override
    public void handleNoMoreSplits() {
        // An empty queue is not end-of-input for this unbounded source.
    }

    /** Snapshot emitted records only; received but not emitted records remain broker-owned. */
    @Override
    public synchronized List<Split> snapshotState(long checkpointId) throws Exception {
        checkFailure();
        checkpoints.put(checkpointId, sequence);
        return new ArrayList<>(splits.values());
    }

    /** Acknowledge only records included in this completed checkpoint, including earlier aborts. */
    @Override
    public synchronized void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (closed) {
            return;
        }
        checkFailure();
        Long watermark = checkpoints.get(checkpointId);
        if (watermark == null) {
            return;
        }
        try {
            while (!pending.isEmpty() && pending.peekFirst().sequence <= watermark) {
                pending.peekFirst().message.acknowledge();
                pending.removeFirst();
            }
        } catch (JMSException e) {
            failure.compareAndSet(null, e);
            throw new IOException(
                    "ActiveMQ acknowledgement failed for checkpoint " + checkpointId, e);
        }
        checkpoints.headMap(checkpointId, true).clear();
        notifyAll();
    }

    @Override
    public synchronized void notifyCheckpointAborted(long checkpointId) {
        checkpoints.remove(checkpointId);
    }

    private void checkFailure() throws IOException {
        Exception error = failure.get();
        if (error != null) {
            throw new IOException(
                    "ActiveMQ source reader failed; unacknowledged messages will be redelivered",
                    error);
        }
    }

    /** Close every JMS resource without acknowledging buffered or emitted records. */
    @Override
    public void close() throws IOException {
        closed = true;
        // Cancellation must not wait for the JMS lock: the broker may be withholding a response
        // while its transport remains alive. Closing the TCP/SSL socket unblocks pending I/O.
        Connection activeConnection = connection;
        IOException socketError = null;
        boolean interruptedTransport = false;
        if (activeConnection instanceof ActiveMQConnection) {
            Socket socket =
                    ((ActiveMQConnection) activeConnection).getTransport().narrow(Socket.class);
            if (socket != null) {
                try {
                    socket.close();
                    interruptedTransport = true;
                } catch (IOException e) {
                    socketError = e;
                }
            }
        }
        synchronized (this) {
            try {
                closeResources(interruptedTransport);
            } catch (IOException e) {
                if (socketError != null) {
                    e.addSuppressed(socketError);
                }
                throw e;
            }
        }
        if (socketError != null) {
            throw socketError;
        }
    }

    private void closeResources(boolean interruptedTransport) throws IOException {
        try {
            if (connection != null) {
                // Connection.close cascades to sessions/consumers and disposes local state even
                // if its final broker removal command fails after cancellation closed the socket.
                connection.close();
            }
        } catch (JMSException e) {
            if (!interruptedTransport
                    || !(connection instanceof ActiveMQConnection)
                    || !((ActiveMQConnection) connection).isClosed()
                    || !isTransportCloseError(e)) {
                throw new IOException("Could not close ActiveMQ connection", e);
            }
        } finally {
            consumer = null;
            session = null;
            connection = null;
            pending.clear();
            checkpoints.clear();
            notifyAll();
        }
    }

    private static boolean isTransportCloseError(JMSException error) {
        for (Throwable cause = error.getCause(); cause != null; cause = cause.getCause()) {
            if (cause instanceof SocketException
                    || cause instanceof TransportDisposedIOException
                    || cause instanceof InactivityIOException) {
                return true;
            }
        }
        return false;
    }

    private static final class PendingMessage {
        private final long sequence;
        private final Message message;

        private PendingMessage(long sequence, Message message) {
            this.sequence = sequence;
            this.message = message;
        }
    }
}
