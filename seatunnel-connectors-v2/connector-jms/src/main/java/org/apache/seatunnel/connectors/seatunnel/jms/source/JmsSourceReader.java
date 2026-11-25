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

package org.apache.seatunnel.connectors.seatunnel.jms.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jms.client.JmsConnectionFactories;
import org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import lombok.extern.slf4j.Slf4j;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.DESTINATION;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.MAX_IN_FLIGHT_MESSAGES;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSourceOptions.MESSAGE_SELECTOR;
import static org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorErrorCode.CLOSE_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorErrorCode.COMMIT_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorErrorCode.CONNECTION_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorErrorCode.DESERIALIZE_MESSAGE_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorErrorCode.UNSUPPORTED_MESSAGE;

/**
 * Reads one queue with a transacted session per checkpoint epoch.
 *
 * <p>JMS has no per-message acknowledgement: committing a session (or acknowledging a message)
 * covers every message the session received. Each epoch therefore gets its own transacted session.
 * {@link #snapshotState(long)} seals the open epoch for that checkpoint, the next {@link
 * #pollNext(Collector)} receives in a new session, and {@link #notifyCheckpointComplete(long)}
 * commits the sealed sessions up to the completed checkpoint. Sessions that are never committed are
 * rolled back when the connection closes, so the broker redelivers their messages: delivery is at
 * least once.
 *
 * <p>Locking: {@code pollNext} takes the checkpoint lock and then {@link #lock}; {@code
 * snapshotState} runs with the checkpoint lock held and takes {@link #lock}; {@code
 * notifyCheckpointComplete} and {@code close} take only {@link #lock}. {@link #lock} serializes
 * every session operation, as JMS sessions are single threaded. {@link #cancel()} takes no lock and
 * only closes the connection, which JMS allows from any thread.
 */
@Slf4j
public class JmsSourceReader implements SourceReader<SeaTunnelRow, JmsSourceSplit> {

    /** Upper bound of messages received in one call of {@link #pollNext(Collector)}. */
    private static final int MAX_MESSAGES_PER_POLL = 32;

    private final ReadonlyConfig config;
    private final DeserializationSchema<SeaTunnelRow> deserializer;
    private final String destination;
    private final String messageSelector;
    private final int maxInFlightMessages;

    private final Object lock = new Object();
    private final Map<String, JmsSourceSplit> splits = new LinkedHashMap<>();
    /** Sealed epochs by the checkpoint that sealed them; committed once it completes. */
    private final NavigableMap<Long, Epoch> sealed = new TreeMap<>();

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean closed;
    private volatile Connection connection;

    private ConnectionFactory connectionFactory;
    private Epoch open;
    /** Messages received in the open and the sealed epochs, not yet committed. */
    private int uncommitted;

    public JmsSourceReader(
            ReadonlyConfig config, DeserializationSchema<SeaTunnelRow> deserializer) {
        this(config, deserializer, null);
    }

    JmsSourceReader(
            ReadonlyConfig config,
            DeserializationSchema<SeaTunnelRow> deserializer,
            ConnectionFactory connectionFactory) {
        this.config = config;
        this.deserializer = deserializer;
        this.connectionFactory = connectionFactory;
        this.destination = config.get(DESTINATION);
        this.messageSelector = config.getOptional(MESSAGE_SELECTOR).orElse(null);
        this.maxInFlightMessages = config.get(MAX_IN_FLIGHT_MESSAGES);
    }

    /** The connection is opened by the first {@link #pollNext} after a split is assigned. */
    @Override
    public void open() {}

    /**
     * Receives and emits up to {@link #MAX_MESSAGES_PER_POLL} messages. Each message is received
     * and emitted while the checkpoint lock is held, so no message can be received before a barrier
     * and emitted after it. When nothing is received the method returns, and the engine's idle wait
     * happens outside the lock.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (lock) {
            checkFailure();
            if (closed || splits.isEmpty()) {
                return;
            }
            try {
                openConnection();
                closeSealedConsumers();
                if (open == null) {
                    open = openEpoch();
                }
            } catch (JMSException e) {
                throw fail(
                        new JmsConnectorException(
                                CONNECTION_FAILED,
                                "Could not open or rotate the consumer of JMS destination "
                                        + destination,
                                e));
            } catch (RuntimeException e) {
                throw fail(e);
            }
        }
        for (int i = 0; i < MAX_MESSAGES_PER_POLL; i++) {
            synchronized (output.getCheckpointLock()) {
                Message message;
                synchronized (lock) {
                    checkFailure();
                    if (closed || open == null || uncommitted >= maxInFlightMessages) {
                        return;
                    }
                    try {
                        message = open.consumer.receiveNoWait();
                    } catch (JMSException e) {
                        throw fail(
                                new JmsConnectorException(
                                        CONNECTION_FAILED,
                                        "Could not receive from JMS destination " + destination,
                                        e));
                    }
                    if (message == null) {
                        return;
                    }
                    open.received++;
                    uncommitted++;
                }
                emit(message, output);
            }
        }
    }

    /** Emits the rows of one message. Called with the checkpoint lock held. */
    private void emit(Message message, Collector<SeaTunnelRow> output) {
        try {
            byte[] payload = payload(message);
            try {
                if (deserializer instanceof JsonDeserializationSchema) {
                    ((JsonDeserializationSchema) deserializer).collect(payload, output);
                } else {
                    deserializer.deserialize(payload, output);
                }
            } catch (Exception e) {
                // Format errors may quote the payload, so neither their message nor the cause is
                // kept.
                throw new JmsConnectorException(
                        DESERIALIZE_MESSAGE_FAILED,
                        "Could not convert JMS message "
                                + messageId(message)
                                + " to rows ("
                                + e.getClass().getName()
                                + "); check the schema and format options");
            }
        } catch (JMSException e) {
            throw fail(
                    new JmsConnectorException(
                            CONNECTION_FAILED,
                            "Could not read the body of JMS message " + messageId(message),
                            e));
        } catch (RuntimeException e) {
            // The message is never committed: the failed reader's sessions are rolled back.
            throw fail(e);
        }
    }

    private static byte[] payload(Message message) throws JMSException {
        byte[] payload;
        if (message instanceof TextMessage) {
            String text = ((TextMessage) message).getText();
            payload = text == null ? null : text.getBytes(StandardCharsets.UTF_8);
        } else if (message instanceof BytesMessage) {
            BytesMessage bytesMessage = (BytesMessage) message;
            long length = bytesMessage.getBodyLength();
            if (length > Integer.MAX_VALUE - 8) {
                throw new JmsConnectorException(
                        UNSUPPORTED_MESSAGE,
                        "JMS message " + messageId(message) + " is too large to read");
            }
            payload = new byte[(int) length];
            if (length > 0 && bytesMessage.readBytes(payload) != length) {
                throw new JmsConnectorException(
                        UNSUPPORTED_MESSAGE,
                        "Could not read the whole body of JMS message " + messageId(message));
            }
        } else {
            // An ObjectMessage is never read: that would run Java deserialization on its body.
            throw new JmsConnectorException(
                    UNSUPPORTED_MESSAGE,
                    "JMS message "
                            + messageId(message)
                            + " is a "
                            + messageType(message)
                            + "; only TextMessage and BytesMessage are supported");
        }
        // An empty body yields no row. Never commit a message that would be dropped silently.
        if (payload == null || payload.length == 0) {
            throw new JmsConnectorException(
                    UNSUPPORTED_MESSAGE,
                    "JMS message " + messageId(message) + " has an empty body");
        }
        return payload;
    }

    private static String messageType(Message message) {
        if (message instanceof ObjectMessage) {
            return "ObjectMessage";
        }
        if (message instanceof MapMessage) {
            return "MapMessage";
        }
        if (message instanceof StreamMessage) {
            return "StreamMessage";
        }
        return "Message without a body";
    }

    private static String messageId(Message message) {
        try {
            return String.valueOf(message.getJMSMessageID());
        } catch (JMSException | RuntimeException e) {
            return "(unknown id)";
        }
    }

    /** Opens the connection once. Called with {@link #lock} held. */
    private void openConnection() throws JMSException {
        if (connection != null) {
            return;
        }
        if (connectionFactory == null) {
            connectionFactory = JmsConnectionFactories.create(config);
        }
        Connection created = JmsConnectionFactories.createConnection(connectionFactory, config);
        connection = created;
        // cancel() or close() may have run while the connection was being created. It sets
        // closed before it reads connection, and this method sets connection before it reads
        // closed, so at least one side closes the new connection.
        if (closed) {
            closeQuietly(created);
            throw new JMSException("The JMS source was closed while connecting");
        }
        // receiveNoWait() can return null after a connection loss instead of throwing (ActiveMQ
        // Classic did right after a broker side drop), so the listener also detects the loss, and
        // a commit is never attempted after it.
        created.setExceptionListener(e -> failure.compareAndSet(null, e));
        created.start();
    }

    /** Opens the next epoch's transacted session and consumer. Called with {@link #lock} held. */
    private Epoch openEpoch() throws JMSException {
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        try {
            Queue queue = session.createQueue(destination);
            MessageConsumer consumer =
                    messageSelector == null
                            ? session.createConsumer(queue)
                            : session.createConsumer(queue, messageSelector);
            return new Epoch(session, consumer);
        } catch (JMSException | RuntimeException e) {
            try {
                session.close();
            } catch (JMSException closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
    }

    /**
     * Closes the consumers of sealed epochs before the next epoch receives, so that providers which
     * release prefetched messages on consumer close hand them to the next consumer. The sessions
     * stay open and uncommitted. Called with {@link #lock} held.
     */
    private void closeSealedConsumers() throws JMSException {
        for (Epoch epoch : sealed.values()) {
            if (epoch.consumer != null) {
                MessageConsumer consumer = epoch.consumer;
                epoch.consumer = null;
                consumer.close();
            }
        }
    }

    @Override
    public void addSplits(List<JmsSourceSplit> assigned) {
        synchronized (lock) {
            assigned.forEach(split -> splits.putIfAbsent(split.splitId(), split));
        }
    }

    /** A queue has no end, so no more splits does not end this unbounded reader. */
    @Override
    public void handleNoMoreSplits() {}

    /**
     * Seals the open epoch for this checkpoint. Makes no JMS call. Runs with the checkpoint lock
     * held, so no message is being received or emitted.
     */
    @Override
    public List<JmsSourceSplit> snapshotState(long checkpointId) throws Exception {
        synchronized (lock) {
            checkFailure();
            if (open != null && open.received > 0) {
                sealed.put(checkpointId, open);
                open = null;
            }
            return new ArrayList<>(splits.values());
        }
    }

    /**
     * Commits and closes every sealed epoch up to this checkpoint, including epochs of earlier
     * aborted checkpoints. A repeated notification finds nothing to commit. A failed commit fails
     * the reader; its messages are redelivered unless the broker applied the commit, so they are
     * never lost.
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        synchronized (lock) {
            if (closed) {
                return;
            }
            checkFailure();
            Iterator<Epoch> completed = sealed.headMap(checkpointId, true).values().iterator();
            while (completed.hasNext()) {
                Epoch epoch = completed.next();
                try {
                    epoch.session.commit();
                } catch (JMSException | RuntimeException e) {
                    throw fail(
                            new JmsConnectorException(
                                    COMMIT_FAILED,
                                    "Could not commit the JMS messages of checkpoint "
                                            + checkpointId
                                            + "; uncommitted messages are redelivered",
                                    e));
                }
                completed.remove();
                uncommitted -= epoch.received;
                try {
                    epoch.session.close();
                } catch (JMSException e) {
                    // The transaction is already committed; a lost connection is reported
                    // through the exception listener.
                    log.warn(
                            "Could not close a committed JMS session of checkpoint {}",
                            checkpointId,
                            e);
                }
            }
        }
    }

    /** Aborted checkpoints commit nothing; a later completed checkpoint commits their epochs. */
    @Override
    public void notifyCheckpointAborted(long checkpointId) {}

    /**
     * Closes the connection without taking any lock. Closing a connection is the one JMS operation
     * allowed concurrently with others; it rolls back every uncommitted session.
     */
    @Override
    public void cancel() {
        closed = true;
        Connection current = connection;
        if (current != null) {
            closeQuietly(current);
        }
    }

    /**
     * Closes the connection; uncommitted sessions are rolled back and their messages redelivered.
     */
    @Override
    public void close() throws IOException {
        closed = true;
        Connection current = connection;
        JMSException error = null;
        if (current != null) {
            try {
                current.close();
            } catch (JMSException e) {
                error = e;
            }
        }
        synchronized (lock) {
            connection = null;
            open = null;
            sealed.clear();
            uncommitted = 0;
        }
        if (error != null && failure.get() == null) {
            throw new JmsConnectorException(
                    CLOSE_FAILED, "Could not close the JMS connection", error);
        }
    }

    private void checkFailure() {
        Throwable error = failure.get();
        if (error != null) {
            throw new JmsConnectorException(
                    CONNECTION_FAILED,
                    "The JMS source reader failed; uncommitted messages are redelivered",
                    error);
        }
    }

    private RuntimeException fail(RuntimeException error) {
        failure.compareAndSet(null, error);
        return error;
    }

    private static void closeQuietly(Connection connection) {
        try {
            connection.close();
        } catch (JMSException | RuntimeException e) {
            log.warn("Could not close the JMS connection", e);
        }
    }

    /** One checkpoint epoch: a transacted session and the messages it received. */
    private static final class Epoch {
        private final Session session;
        private MessageConsumer consumer;
        private int received;

        private Epoch(Session session, MessageConsumer consumer) {
            this.session = session;
            this.consumer = consumer;
        }
    }
}
