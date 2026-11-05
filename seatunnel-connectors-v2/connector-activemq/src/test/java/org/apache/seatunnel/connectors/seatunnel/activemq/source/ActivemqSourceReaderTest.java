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
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.activemq.source.ActivemqSourceEnumerator.Split;
import org.apache.seatunnel.connectors.seatunnel.activemq.source.ActivemqSourceEnumerator.State;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.transport.Transport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ActivemqSourceReaderTest {
    private ConnectionFactory factory;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private Collector<SeaTunnelRow> collector;
    private DeserializationSchema<SeaTunnelRow> deserializer;
    private ActivemqSourceReader reader;
    private java.util.Queue<Message> messages;

    @BeforeEach
    void setUp() throws Exception {
        factory = mock(ConnectionFactory.class);
        connection = mock(Connection.class);
        session = mock(Session.class);
        consumer = mock(MessageConsumer.class);
        collector = mock(Collector.class);
        deserializer = mock(DeserializationSchema.class);
        messages = new ArrayDeque<>();
        when(factory.createConnection()).thenReturn(connection);
        when(connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE))
                .thenReturn(session);
        when(session.createQueue("events")).thenReturn(mock(Queue.class));
        when(session.createConsumer(any())).thenReturn(consumer);
        when(consumer.receive(anyLong())).thenAnswer(call -> messages.poll());
        when(consumer.receiveNoWait()).thenAnswer(call -> messages.poll());
        when(collector.getCheckpointLock()).thenReturn(new Object());
        reader = new ActivemqSourceReader(config(), deserializer, factory);
        reader.open();
        reader.addSplits(Collections.singletonList(new Split(0)));
    }

    @AfterEach
    void tearDown() throws Exception {
        reader.close();
    }

    @Test
    void acknowledgesOnlyTheCompletedSnapshot() throws Exception {
        TextMessage first = enqueue("first");
        reader.pollNext(collector);
        reader.snapshotState(1);
        TextMessage later = enqueue("later");
        reader.pollNext(collector);
        verify(first, never()).acknowledge();
        reader.notifyCheckpointComplete(1);
        verify(first).acknowledge();
        verify(later, never()).acknowledge();
        reader.snapshotState(2);
        reader.notifyCheckpointComplete(2);
        verify(later).acknowledge();
        verify(connection).createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
    }

    @Test
    void abortedSnapshotRemainsCoveredByLaterCompletion() throws Exception {
        TextMessage first = enqueue("first");
        reader.pollNext(collector);
        reader.snapshotState(1);
        reader.notifyCheckpointAborted(1);
        reader.notifyCheckpointComplete(1);
        verify(first, never()).acknowledge();
        reader.snapshotState(2);
        reader.notifyCheckpointComplete(2);
        verify(first).acknowledge();
    }

    @Test
    void completionIsIdempotentAndHandlesSubsumedCheckpoints() throws Exception {
        TextMessage first = enqueue("first");
        reader.pollNext(collector);
        reader.snapshotState(1);
        reader.snapshotState(2);
        reader.notifyCheckpointComplete(99);
        verify(first, never()).acknowledge();
        reader.notifyCheckpointComplete(2);
        reader.notifyCheckpointComplete(1);
        reader.notifyCheckpointComplete(2);
        verify(first, times(1)).acknowledge();
    }

    @Test
    void backpressureStopsReceivingUntilCheckpointCompletes() throws Exception {
        TextMessage first = enqueue("first");
        enqueue("second");
        TextMessage third = enqueue("third");
        reader.pollNext(collector);
        reader.pollNext(collector);
        assertEquals(1, messages.size());
        reader.snapshotState(1);
        reader.notifyCheckpointComplete(1);
        verify(first).acknowledge();
        reader.pollNext(collector);
        assertEquals(0, messages.size());
        verify(third, never()).acknowledge();
    }

    @Test
    void acknowledgementFailureStopsFurtherProcessing() throws Exception {
        TextMessage first = enqueue("first");
        TextMessage second = enqueue("second");
        reader.pollNext(collector);
        reader.snapshotState(1);
        doThrow(new JMSException("disconnected")).when(second).acknowledge();
        assertThrows(IOException.class, () -> reader.notifyCheckpointComplete(1));
        verify(first).acknowledge();
        assertThrows(IOException.class, () -> reader.pollNext(collector));
        assertThrows(IOException.class, () -> reader.snapshotState(2));
    }

    @Test
    void malformedPayloadIsNotAcknowledged() throws Exception {
        TextMessage message = enqueue("invalid");
        doThrow(new IOException("invalid JSON"))
                .when(deserializer)
                .deserialize(any(byte[].class), eq(collector));
        assertThrows(IOException.class, () -> reader.pollNext(collector));
        verify(message, never()).acknowledge();
        assertThrows(IOException.class, () -> reader.snapshotState(1));
    }

    @Test
    void failedMultiRowEmissionIsNotAcknowledged() throws Exception {
        TextMessage message = enqueue("rows");
        doAnswer(
                        call -> {
                            Collector<SeaTunnelRow> output = call.getArgument(1);
                            output.collect(new SeaTunnelRow(new Object[] {"first"}));
                            throw new IOException("second row failed");
                        })
                .when(deserializer)
                .deserialize(any(byte[].class), eq(collector));
        assertThrows(IOException.class, () -> reader.pollNext(collector));
        verify(collector).collect(any(SeaTunnelRow.class));
        verify(message, never()).acknowledge();
    }

    @Test
    void rejectsObjectMessagesWithoutDeserializingThem() throws Exception {
        ObjectMessage message = mock(ObjectMessage.class);
        messages.add(message);
        assertThrows(IOException.class, () -> reader.pollNext(collector));
        verify(message, never()).getObject();
        verify(message, never()).acknowledge();
    }

    @Test
    void rejectsNullTextPayload() throws Exception {
        TextMessage message = enqueue(null);
        assertThrows(IOException.class, () -> reader.pollNext(collector));
        verify(message, never()).acknowledge();
    }

    @Test
    void rejectsEmptyTextPayloadWithoutAcknowledging() throws Exception {
        TextMessage message = enqueue("");
        assertThrows(IOException.class, () -> reader.pollNext(collector));
        verify(deserializer, never()).deserialize(any(byte[].class), eq(collector));
        verify(message, never()).acknowledge();
        assertThrows(IOException.class, () -> reader.snapshotState(1));
    }

    @Test
    void closeDoesNotAcknowledgeAndClosesAllResources() throws Exception {
        TextMessage message = enqueue("pending");
        reader.pollNext(collector);
        reader.snapshotState(1);
        reader.close();
        reader.notifyCheckpointComplete(1);
        reader.close();
        verify(message, never()).acknowledge();
        // JMS Connection.close owns cascading cleanup without separate broker removal requests.
        verify(consumer, never()).close();
        verify(session, never()).close();
        verify(connection, times(1)).close();
    }

    @Test
    void connectionCleanupFailureIsReported() throws Exception {
        reader.pollNext(collector);
        doThrow(new JMSException("connection close failed")).when(connection).close();
        assertThrows(IOException.class, reader::close);
        verify(connection).close();
    }

    @Test
    void failedStartupClosesPartialResources() throws Exception {
        doThrow(new JMSException("start failed")).when(connection).start();
        assertThrows(JMSException.class, () -> reader.pollNext(collector));
        verify(connection).close();
    }

    @Test
    void asynchronousConnectionFailureIsPropagated() throws Exception {
        reader.pollNext(collector);
        ArgumentCaptor<ExceptionListener> listener =
                ArgumentCaptor.forClass(ExceptionListener.class);
        verify(connection).setExceptionListener(listener.capture());
        listener.getValue().onException(new JMSException("broker stopped"));
        assertThrows(IOException.class, () -> reader.pollNext(collector));
    }

    @Test
    void duplicateRestoredSlotsDoNotCreateExtraConsumers() throws Exception {
        reader.addSplits(Arrays.asList(new Split(0), new Split(1)));
        reader.pollNext(collector);
        assertEquals(2, reader.snapshotState(1).size());
        verify(session, times(1)).createConsumer(any());
    }

    @Test
    void unassignedReaderDoesNotConnect() throws Exception {
        reader.close();
        reader = new ActivemqSourceReader(config(), deserializer, factory);
        reader.open();
        reader.handleNoMoreSplits();
        reader.pollNext(collector);
        assertEquals(0, reader.snapshotState(1).size());
        verify(factory, never()).createConnection();
    }

    @Test
    void enumeratorRestoresOnlyPendingSlotsAndReassignsReturnedSlots() throws Exception {
        SourceSplitEnumerator.Context<Split> context = mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(2);
        when(context.registeredReaders()).thenReturn(Collections.singleton(0));
        ActivemqSourceEnumerator enumerator = new ActivemqSourceEnumerator(context);
        enumerator.run();
        assertEquals(1, enumerator.currentUnassignedSplitSize());
        State state = enumerator.snapshotState(1);
        SourceSplitEnumerator.Context<Split> restoredContext =
                mock(SourceSplitEnumerator.Context.class);
        when(restoredContext.currentParallelism()).thenReturn(2);
        when(restoredContext.registeredReaders()).thenReturn(new HashSet<>(Arrays.asList(0, 1)));
        ActivemqSourceEnumerator restored = new ActivemqSourceEnumerator(restoredContext, state);
        restored.run();
        verify(restoredContext, never()).assignSplit(eq(0), any(List.class));
        verify(restoredContext).assignSplit(eq(1), any(List.class));
        assertEquals(0, restored.currentUnassignedSplitSize());
        restored.addSplitsBack(Collections.singletonList(new Split(0)), 0);
        verify(restoredContext).assignSplit(eq(0), any(List.class));
        // State snapshots must not alias the live pending collection.
        ActivemqSourceEnumerator anotherRestore =
                new ActivemqSourceEnumerator(restoredContext, state);
        assertEquals(1, anotherRestore.currentUnassignedSplitSize());
    }

    @Test
    void failedAssignmentDoesNotLosePendingSlots() {
        SourceSplitEnumerator.Context<Split> context = mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(1);
        when(context.registeredReaders()).thenReturn(Collections.singleton(0));
        doThrow(new IllegalStateException("reader unavailable"))
                .when(context)
                .assignSplit(eq(0), any(List.class));
        ActivemqSourceEnumerator enumerator = new ActivemqSourceEnumerator(context);
        assertThrows(IllegalStateException.class, enumerator::run);
        assertEquals(1, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void restoreScaleUpAddsOnlyNewSlots() throws Exception {
        SourceSplitEnumerator.Context<Split> context = mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(1);
        when(context.registeredReaders()).thenReturn(Collections.singleton(0));
        ActivemqSourceEnumerator enumerator = new ActivemqSourceEnumerator(context);
        enumerator.run();
        State saved = enumerator.snapshotState(1);
        SourceSplitEnumerator.Context<Split> restoredContext =
                mock(SourceSplitEnumerator.Context.class);
        when(restoredContext.currentParallelism()).thenReturn(4);
        when(restoredContext.registeredReaders())
                .thenReturn(new HashSet<>(Arrays.asList(0, 1, 2, 3)));
        ActivemqSourceEnumerator restored = new ActivemqSourceEnumerator(restoredContext, saved);
        restored.addSplitsBack(Collections.singletonList(new Split(0)), 0);
        restored.run();
        restored.run();
        for (int i = 0; i < 4; i++) {
            verify(restoredContext, times(1)).assignSplit(eq(i), any(List.class));
        }
        assertEquals(0, restored.currentUnassignedSplitSize());
    }

    @Test
    void credentialsArePassedSeparatelyToConnectionFactory() throws Exception {
        reader.close();
        Map<String, Object> options = new HashMap<>();
        options.put("queue_name", "events");
        options.put("username", "reader");
        options.put("password", "test-password");
        when(factory.createConnection("reader", "test-password")).thenReturn(connection);
        reader = new ActivemqSourceReader(ReadonlyConfig.fromMap(options), deserializer, factory);
        reader.addSplits(Collections.singletonList(new Split(0)));
        reader.pollNext(collector);
        verify(factory).createConnection("reader", "test-password");
        verify(factory, never()).createConnection();
    }

    @Test
    void cancellationInterruptsTransportBeforeWaitingForReaderLock() throws Exception {
        ActiveMQConnection activeConnection = mock(ActiveMQConnection.class);
        Transport transport = mock(Transport.class);
        Socket socket = mock(Socket.class);
        when(factory.createConnection()).thenReturn(activeConnection);
        when(activeConnection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE))
                .thenReturn(session);
        when(activeConnection.getTransport()).thenReturn(transport);
        when(transport.narrow(Socket.class)).thenReturn(socket);
        CountDownLatch receiving = new CountDownLatch(1);
        CountDownLatch socketClosed = new CountDownLatch(1);
        when(consumer.receive(anyLong()))
                .thenAnswer(
                        call -> {
                            receiving.countDown();
                            assertTrue(socketClosed.await(10, TimeUnit.SECONDS));
                            return null;
                        });
        doAnswer(
                        call -> {
                            socketClosed.countDown();
                            return null;
                        })
                .when(socket)
                .close();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> polling =
                    executor.submit(
                            () -> {
                                reader.pollNext(collector);
                                return null;
                            });
            assertTrue(receiving.await(5, TimeUnit.SECONDS));
            Future<?> closing =
                    executor.submit(
                            () -> {
                                reader.close();
                                return null;
                            });
            closing.get(5, TimeUnit.SECONDS);
            polling.get(5, TimeUnit.SECONDS);
            verify(socket).close();
            verify(activeConnection).close();
        } finally {
            socketClosed.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void cancellationDuringConnectionCreationClosesNewConnectionWithoutOpeningSession()
            throws Exception {
        when(factory.createConnection())
                .thenAnswer(
                        call -> {
                            // Reproduce close observing null before the newly created connection is
                            // published.
                            reader.close();
                            return connection;
                        });
        assertThrows(JMSException.class, () -> reader.pollNext(collector));
        verify(connection, never()).createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        verify(connection).close();
    }

    @Test
    void realJsonParserFailureDoesNotExposePayloadOrAcknowledgeMessage() throws Exception {
        reader.close();
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        reader =
                new ActivemqSourceReader(
                        config(), new JsonDeserializationSchema(false, false, rowType), factory);
        reader.addSplits(Collections.singletonList(new Split(0)));
        String marker = "SYNTHETIC_SECRET_7f83";
        TextMessage message = enqueue("{\"value\":\"" + marker + "\",BROKEN}");
        IOException initial = assertThrows(IOException.class, () -> reader.pollNext(collector));
        IOException subsequent = assertThrows(IOException.class, () -> reader.snapshotState(1));
        for (IOException error : Arrays.asList(initial, subsequent)) {
            StringWriter trace = new StringWriter();
            error.printStackTrace(new PrintWriter(trace));
            assertFalse(trace.toString().contains(marker));
            assertTrue(trace.toString().contains("deserialization or emission failed"));
        }
        verify(message, never()).acknowledge();
    }

    @Test
    void jsonArrayAcknowledgesOnlyAfterEveryRowIsEmittedAndCheckpointCompletes() throws Exception {
        reader.close();
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE});
        reader =
                new ActivemqSourceReader(
                        config(), new JsonDeserializationSchema(false, false, rowType), factory);
        reader.addSplits(Collections.singletonList(new Split(0)));
        TextMessage valid = enqueue("[{\"value\":1},{\"value\":2}]");
        reader.pollNext(collector);
        verify(collector, times(2)).collect(any(SeaTunnelRow.class));
        verify(valid, never()).acknowledge();
        reader.snapshotState(1);
        reader.notifyCheckpointComplete(1);
        verify(valid).acknowledge();
        TextMessage invalid = enqueue("[{\"value\":3},{\"value\":\"not-a-number\"}]");
        assertThrows(IOException.class, () -> reader.pollNext(collector));
        verify(collector, times(3)).collect(any(SeaTunnelRow.class));
        verify(invalid, never()).acknowledge();
        assertThrows(IOException.class, () -> reader.snapshotState(2));
    }

    @Test
    void interruptedTransportErrorIsIgnoredOnlyAfterConnectionHasClosed() throws Exception {
        ActiveMQConnection active = useActiveConnection();
        reader.pollNext(collector);
        JMSException error = new JMSException("socket closed");
        error.initCause(new java.net.SocketException("socket closed"));
        doThrow(error).when(active).close();
        when(active.isClosed()).thenReturn(true);
        reader.close();
        verify(active).close();
    }

    @Test
    void unrelatedCleanupErrorAfterSocketInterruptionIsStillReported() throws Exception {
        ActiveMQConnection active = useActiveConnection();
        reader.pollNext(collector);
        doThrow(new JMSException("scheduler shutdown failed")).when(active).close();
        when(active.isClosed()).thenReturn(true);
        assertThrows(IOException.class, reader::close);
    }

    @Test
    void transportErrorBeforeConnectionCleanupCompletesIsStillReported() throws Exception {
        ActiveMQConnection active = useActiveConnection();
        reader.pollNext(collector);
        JMSException error = new JMSException("socket closed");
        error.initCause(new java.net.SocketException("socket closed"));
        doThrow(error).when(active).close();
        when(active.isClosed()).thenReturn(false);
        assertThrows(IOException.class, reader::close);
    }

    private ActiveMQConnection useActiveConnection() throws Exception {
        ActiveMQConnection active = mock(ActiveMQConnection.class);
        Transport transport = mock(Transport.class);
        when(active.getTransport()).thenReturn(transport);
        when(transport.narrow(Socket.class)).thenReturn(mock(Socket.class));
        when(factory.createConnection()).thenReturn(active);
        when(active.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE))
                .thenReturn(session);
        return active;
    }

    private TextMessage enqueue(String text) throws JMSException {
        TextMessage message = mock(TextMessage.class);
        when(message.getText()).thenReturn(text);
        messages.add(message);
        return message;
    }

    private ReadonlyConfig config() {
        Map<String, Object> config = new HashMap<>();
        config.put("queue_name", "events");
        config.put("max_in_flight_messages", 2);
        return ReadonlyConfig.fromMap(config);
    }
}
