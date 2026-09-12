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

package org.apache.seatunnel.engine.server.task.error;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.engine.server.task.context.SinkWriterContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

class ErrorHandlingSinkWriterTest {

    @Test
    void timerFlushPreservesMainSinkActionAndFlushesErrorHandler() throws Exception {
        AtomicInteger mainFlushes = new AtomicInteger();
        AtomicInteger errorFlushes = new AtomicInteger();
        SinkWriterContext context = new SinkWriterContext(1, 0, null, null);
        context.registerFlushAction(mainFlushes::incrementAndGet);
        ErrorHandler<String> errorHandler =
                new ErrorHandler<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(),
                        new CountingErrorSinkWriter(errorFlushes));
        ErrorHandlingSinkWriter<String, String, String> writer =
                new ErrorHandlingSinkWriter<>(
                        new NoopSinkWriter(), errorHandler, (error, row, ctx) -> true, "test");

        writer.registerFlushAction(context);
        context.getFlushAction().run();

        Assertions.assertEquals(1, mainFlushes.get());
        Assertions.assertEquals(1, errorFlushes.get());
    }

    @Test
    void reportsWrittenRoutedAndDroppedOutcomes() throws Exception {
        Assertions.assertEquals(
                ErrorHandlingSinkWriter.WriteOutcome.WRITTEN,
                writer(ErrorHandlerMode.LOG, null, false).writeWithOutcome("row"));
        Assertions.assertEquals(
                ErrorHandlingSinkWriter.WriteOutcome.ROUTED_TO_ERROR_SINK,
                writer(
                                ErrorHandlerMode.ROUTE,
                                new CountingErrorSinkWriter(new AtomicInteger()),
                                true)
                        .writeWithOutcome("row"));
        Assertions.assertEquals(
                ErrorHandlingSinkWriter.WriteOutcome.DROPPED,
                writer(ErrorHandlerMode.LOG, null, true).writeWithOutcome("row"));
        Assertions.assertEquals(
                ErrorHandlingSinkWriter.WriteOutcome.DROPPED,
                writer(ErrorHandlerMode.ROUTE, new DroppingErrorSinkWriter(), true)
                        .writeWithOutcome("row"));
    }

    @Test
    void timerFlushPropagatesErrorSinkFailure() {
        AtomicInteger mainFlushes = new AtomicInteger();
        SinkWriterContext context = new SinkWriterContext(1, 0, null, null);
        context.registerFlushAction(mainFlushes::incrementAndGet);
        ErrorHandler<String> errorHandler =
                new ErrorHandler<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(),
                        new FailingErrorSinkWriter());
        ErrorHandlingSinkWriter<String, String, String> writer =
                new ErrorHandlingSinkWriter<>(
                        new NoopSinkWriter(false), errorHandler, (error, row, ctx) -> true, "test");
        writer.registerFlushAction(context);

        IOException exception =
                Assertions.assertThrows(IOException.class, () -> context.getFlushAction().run());

        Assertions.assertEquals("flush failed", exception.getMessage());
        Assertions.assertEquals(1, mainFlushes.get());
    }

    private static ErrorHandlingSinkWriter<String, String, String> writer(
            ErrorHandlerMode mode, ErrorSinkRowWriter<String> errorSink, boolean fail) {
        return new ErrorHandlingSinkWriter<>(
                new NoopSinkWriter(fail),
                new ErrorHandler<>(StageErrorConfig.builder().mode(mode).build(), errorSink),
                (error, row, ctx) -> true,
                "test");
    }

    private static final class NoopSinkWriter implements SinkWriter<String, String, String> {
        private final boolean fail;

        private NoopSinkWriter() {
            this(false);
        }

        private NoopSinkWriter(boolean fail) {
            this.fail = fail;
        }

        @Override
        public void write(String element) throws IOException {
            if (fail) {
                throw new IOException("row failed");
            }
        }

        @Override
        public Optional<String> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }

    private static final class CountingErrorSinkWriter implements ErrorSinkRowWriter<String> {
        private final AtomicInteger flushes;

        private CountingErrorSinkWriter(AtomicInteger flushes) {
            this.flushes = flushes;
        }

        @Override
        public void write(RowErrorContext ctx, String row, Throwable t) {}

        @Override
        public void flush() {
            flushes.incrementAndGet();
        }

        @Override
        public void close() {}
    }

    private static final class DroppingErrorSinkWriter implements ErrorSinkRowWriter<String> {
        @Override
        public void write(RowErrorContext ctx, String row, Throwable t) {}

        @Override
        public boolean writeAndCheckAccepted(RowErrorContext ctx, String row, Throwable t) {
            return false;
        }

        @Override
        public void close() {}
    }

    private static final class FailingErrorSinkWriter implements ErrorSinkRowWriter<String> {
        @Override
        public void write(RowErrorContext ctx, String row, Throwable t) {}

        @Override
        public void flush() throws IOException {
            throw new IOException("flush failed");
        }

        @Override
        public void close() {}
    }
}
