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

package org.apache.seatunnel.connectors.seatunnel.syslog.sink;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SyslogSinkWriterTest {
    @Test
    void synchronousWriteAndEveryCheckpointHookFlush() throws Exception {
        AtomicInteger flushes = new AtomicInteger();
        ByteArrayOutputStream output =
                new ByteArrayOutputStream() {
                    @Override
                    public void flush() {
                        flushes.incrementAndGet();
                    }
                };
        SSLSocket tls = tls();
        SyslogSinkWriter writer = writer(new Socket(), tls, output, 5000);
        try {
            writer.write(SyslogMessageEncoderTest.row());
            assertTrue(output.size() > 0);
            writer.prepareCommit();
            writer.prepareCommit(1);
            assertTrue(writer.snapshotState(1).isEmpty());
            writer.close();
            writer.close();
            assertEquals(5, flushes.get());
            assertTrue(tls.isClosed());
            assertThrows(IOException.class, () -> writer.write(SyslogMessageEncoderTest.row()));
        } finally {
            writer.close();
        }
    }

    @Test
    void partialWritePermanentlyFailsWithoutRetry() throws Exception {
        AtomicInteger bytes = new AtomicInteger();
        OutputStream output =
                new OutputStream() {
                    @Override
                    public void write(int b) throws IOException {
                        if (bytes.incrementAndGet() == 5) {
                            throw new IOException("partial write");
                        }
                    }
                };
        Socket raw = new Socket();
        SyslogSinkWriter writer = writer(raw, tls(), output, 5000);
        try {
            IOException failure =
                    assertThrows(
                            IOException.class, () -> writer.write(SyslogMessageEncoderTest.row()));
            assertTrue(failure.getMessage().contains("uncertain"));
            assertTrue(raw.isClosed());
            assertThrows(IOException.class, () -> writer.write(SyslogMessageEncoderTest.row()));
            assertThrows(UncheckedIOException.class, writer::prepareCommit);
            assertThrows(IOException.class, () -> writer.prepareCommit(1));
            assertThrows(IOException.class, () -> writer.snapshotState(1));
            assertThrows(IOException.class, writer::close);
            assertEquals(5, bytes.get());
        } finally {
            closeAfterFailure(writer);
        }
    }

    @Test
    void eachCheckpointHookAndClosePropagatesFlushFailure() {
        for (int hook = 0; hook < 4; hook++) {
            SyslogSinkWriter writer =
                    writer(
                            new Socket(),
                            tls(),
                            new OutputStream() {
                                @Override
                                public void write(int b) {}

                                @Override
                                public void flush() throws IOException {
                                    throw new IOException("flush failure");
                                }
                            },
                            5000);
            try {
                switch (hook) {
                    case 0:
                        assertThrows(UncheckedIOException.class, writer::prepareCommit);
                        break;
                    case 1:
                        assertThrows(IOException.class, () -> writer.prepareCommit(1));
                        break;
                    case 2:
                        assertThrows(IOException.class, () -> writer.snapshotState(1));
                        break;
                    default:
                        assertThrows(IOException.class, writer::close);
                }
                assertThrows(IOException.class, writer::close);
            } finally {
                closeAfterFailure(writer);
            }
        }
    }

    @Test
    void deadlineClosesUnderlyingSocketAndReleasesBlockedWrite() throws Exception {
        blockedWrite(false);
    }

    @Test
    void concurrentCloseReleasesBlockedWriteWithoutWaitingForDeadline() throws Exception {
        blockedWrite(true);
    }

    private void blockedWrite(boolean closeConcurrently) throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        Socket raw =
                new Socket() {
                    @Override
                    public void close() {
                        closed.countDown();
                    }
                };
        OutputStream output =
                new OutputStream() {
                    @Override
                    public void write(int b) throws IOException {
                        entered.countDown();
                        try {
                            if (!closed.await(15, TimeUnit.SECONDS)) {
                                throw new AssertionError("TCP close did not release blocked write");
                            }
                            throw new IOException("TCP closed while writing");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                    }
                };
        SyslogSinkWriter writer = writer(raw, tls(), output, closeConcurrently ? 30000 : 500);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<IOException> result =
                    worker.submit(
                            () ->
                                    assertThrows(
                                            IOException.class,
                                            () -> writer.write(SyslogMessageEncoderTest.row())));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            if (closeConcurrently) {
                assertThrows(IOException.class, writer::close);
            }
            IOException failure = result.get(5, TimeUnit.SECONDS);
            if (!closeConcurrently) {
                assertTrue(failure instanceof SocketTimeoutException);
            }
            assertTrue(closed.await(1, TimeUnit.SECONDS));
            assertThrows(IOException.class, () -> writer.prepareCommit(2));
        } finally {
            closed.countDown();
            worker.shutdownNow();
            closeAfterFailure(writer);
            assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void interruptedCallerFailsAndClosesTransport() {
        Socket raw = new Socket();
        SyslogSinkWriter writer = writer(raw, tls(), new ByteArrayOutputStream(), 5000);
        try {
            Thread.currentThread().interrupt();
            assertThrows(IOException.class, () -> writer.write(SyslogMessageEncoderTest.row()));
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(raw.isClosed());
        } finally {
            Thread.interrupted();
            assertThrows(IOException.class, writer::close);
        }
    }

    private static SyslogSinkWriter writer(
            Socket raw, SSLSocket tls, OutputStream output, int timeout) {
        return new SyslogSinkWriter(
                new SyslogMessageEncoder(SyslogMessageEncoderTest.TYPE, 8192),
                new SyslogTlsClient(raw, tls, output, timeout));
    }

    private static void closeAfterFailure(SyslogSinkWriter writer) {
        try {
            writer.close();
        } catch (IOException expected) {
            // Failure assertions are in the test body; cleanup must also run if one fails.
        }
    }

    private static SSLSocket tls() {
        try {
            return (SSLSocket) SSLSocketFactory.getDefault().createSocket();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
