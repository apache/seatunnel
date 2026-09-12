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

package org.apache.seatunnel.edge.agent.connector.file.multiline;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class MultilineAssemblerTest {

    private static final String TIMESTAMP_REGEX = "^\\d{4}-\\d{2}-\\d{2}";
    private static final String FILE = "/var/log/app.log";

    private static MultilineAssembler.LineElement line(String text, long lineNum, long ts) {
        return new MultilineAssembler.LineElement(text, FILE, lineNum, lineNum * 100L, ts);
    }

    /** AFTER mode: matching line starts a new event, flushing the previous buffered lines. */
    @Test
    void afterModeFlushesOnBoundary() {
        MultilineAssembler asm =
                new MultilineAssembler(
                        TIMESTAMP_REGEX, MultilineAssembler.MatchMode.AFTER, false, 500);

        List<MultilineAssembler.LineElement> result;

        result = asm.addLine(line("2024-01-01 ERROR NullPointer", 1, 1000L));
        Assertions.assertNull(result);
        Assertions.assertTrue(asm.hasPending());

        result = asm.addLine(line("\tat com.foo.Bar.method(Bar.java:42)", 2, 1001L));
        Assertions.assertNull(result);

        result = asm.addLine(line("\tat com.foo.Baz.run(Baz.java:10)", 3, 1002L));
        Assertions.assertNull(result);

        // Next timestamp line triggers flush of previous event (lines 1-3)
        result = asm.addLine(line("2024-01-01 INFO Started", 4, 1003L));
        Assertions.assertNotNull(result);
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals("2024-01-01 ERROR NullPointer", result.get(0).getText());
        Assertions.assertEquals("\tat com.foo.Baz.run(Baz.java:10)", result.get(2).getText());

        // Line 4 is now buffered
        Assertions.assertTrue(asm.hasPending());
    }

    /** AFTER + negate: non-matching lines are boundaries. */
    @Test
    void afterNegateMode() {
        MultilineAssembler asm =
                new MultilineAssembler("^\\s", MultilineAssembler.MatchMode.AFTER, true, 500);

        List<MultilineAssembler.LineElement> result;

        result = asm.addLine(line("first event line 1", 1, 1000L));
        Assertions.assertNull(result);

        result = asm.addLine(line("  continuation", 2, 1001L));
        Assertions.assertNull(result);

        // Non-matching (no leading whitespace) triggers flush
        result = asm.addLine(line("second event", 3, 1002L));
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("first event line 1", result.get(0).getText());
    }

    /** BEFORE mode: matching line is the last line of the current event. */
    @Test
    void beforeModeFlushesOnMatch() {
        MultilineAssembler asm =
                new MultilineAssembler("^END$", MultilineAssembler.MatchMode.BEFORE, false, 500);

        List<MultilineAssembler.LineElement> result;

        result = asm.addLine(line("line A", 1, 1000L));
        Assertions.assertNull(result);

        result = asm.addLine(line("line B", 2, 1001L));
        Assertions.assertNull(result);

        // "END" matches => buffer (including this line) is flushed
        result = asm.addLine(line("END", 3, 1002L));
        Assertions.assertNotNull(result);
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals("END", result.get(2).getText());
        Assertions.assertFalse(asm.hasPending());
    }

    /** BEFORE + negate: non-matching line is the event terminator. */
    @Test
    void beforeNegateMode() {
        MultilineAssembler asm =
                new MultilineAssembler("^\\s", MultilineAssembler.MatchMode.BEFORE, true, 500);

        List<MultilineAssembler.LineElement> result;

        result = asm.addLine(line("  indented 1", 1, 1000L));
        Assertions.assertNull(result);

        result = asm.addLine(line("  indented 2", 2, 1001L));
        Assertions.assertNull(result);

        // Non-matching (no leading whitespace) => negate makes it "match" => flush
        result = asm.addLine(line("not indented", 3, 1002L));
        Assertions.assertNotNull(result);
        Assertions.assertEquals(3, result.size());
    }

    /** maxLines forces a flush when the buffer reaches the limit. */
    @Test
    void maxLinesForceFlush() {
        MultilineAssembler asm =
                new MultilineAssembler(
                        TIMESTAMP_REGEX, MultilineAssembler.MatchMode.AFTER, false, 3);

        List<MultilineAssembler.LineElement> result;

        result = asm.addLine(line("2024-01-01 first", 1, 1000L));
        Assertions.assertNull(result);

        result = asm.addLine(line("\tat line2", 2, 1001L));
        Assertions.assertNull(result);

        result = asm.addLine(line("\tat line3", 3, 1002L));
        Assertions.assertNull(result);

        // Buffer has 3 lines (at maxLines). Next add triggers maxLines flush.
        result = asm.addLine(line("\tat line4", 4, 1003L));
        Assertions.assertNotNull(result);
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals("2024-01-01 first", result.get(0).getText());

        // line4 is now in buffer
        Assertions.assertTrue(asm.hasPending());
    }

    /** flush() returns remaining buffer and clears it. */
    @Test
    void flushReturnsAndClears() {
        MultilineAssembler asm =
                new MultilineAssembler(
                        TIMESTAMP_REGEX, MultilineAssembler.MatchMode.AFTER, false, 500);

        asm.addLine(line("2024-01-01 event", 1, 1000L));
        asm.addLine(line("\tat stack", 2, 1001L));
        Assertions.assertTrue(asm.hasPending());

        List<MultilineAssembler.LineElement> flushed = asm.flush();
        Assertions.assertEquals(2, flushed.size());
        Assertions.assertFalse(asm.hasPending());

        // Second flush returns empty
        List<MultilineAssembler.LineElement> empty = asm.flush();
        Assertions.assertTrue(empty.isEmpty());
    }

    /** hasPending() returns false when buffer is empty. */
    @Test
    void hasPendingFalseWhenEmpty() {
        MultilineAssembler asm =
                new MultilineAssembler(
                        TIMESTAMP_REGEX, MultilineAssembler.MatchMode.AFTER, false, 500);
        Assertions.assertFalse(asm.hasPending());
    }

    /** Single-line events (each line matches boundary in AFTER mode). */
    @Test
    void singleLineEventsInAfterMode() {
        MultilineAssembler asm =
                new MultilineAssembler(
                        TIMESTAMP_REGEX, MultilineAssembler.MatchMode.AFTER, false, 500);

        List<MultilineAssembler.LineElement> result;

        result = asm.addLine(line("2024-01-01 first", 1, 1000L));
        Assertions.assertNull(result);

        result = asm.addLine(line("2024-01-02 second", 2, 1001L));
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("2024-01-01 first", result.get(0).getText());
    }

    /** bufferFirstTimestamp tracks the timestamp of the first line in the buffer. */
    @Test
    void bufferFirstTimestampTracking() {
        MultilineAssembler asm =
                new MultilineAssembler(
                        TIMESTAMP_REGEX, MultilineAssembler.MatchMode.AFTER, false, 500);

        Assertions.assertEquals(0L, asm.getBufferFirstTimestamp());

        asm.addLine(line("2024-01-01 first", 1, 5000L));
        Assertions.assertEquals(5000L, asm.getBufferFirstTimestamp());

        asm.addLine(line("\tat stack", 2, 6000L));
        Assertions.assertEquals(5000L, asm.getBufferFirstTimestamp());

        // Boundary triggers flush, line 3 becomes new buffer start
        asm.addLine(line("2024-01-02 second", 3, 7000L));
        Assertions.assertEquals(7000L, asm.getBufferFirstTimestamp());

        // flush clears timestamp
        asm.flush();
        Assertions.assertEquals(0L, asm.getBufferFirstTimestamp());
    }

    /** Realistic Java exception stack trace with Caused-by chain is collected as a single event. */
    @Test
    void javaExceptionStackTraceCollectedAsOneEvent() {
        MultilineAssembler asm =
                new MultilineAssembler(
                        TIMESTAMP_REGEX, MultilineAssembler.MatchMode.AFTER, false, 500);

        String[] stackTrace = {
            "2024-01-15 10:23:45.678 ERROR [st-zeta-worker-1] o.a.s.c.s.source.JdbcSourceReader - Read table failed",
            "org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException: Unable to read table [public.users]",
            "\tat org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceReader.pollNext(JdbcSourceReader.java:112)",
            "\tat org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask.run(SourceSplitEnumeratorTask.java:89)",
            "\tat org.apache.seatunnel.engine.server.TaskExecutionService$TaskRunner.call(TaskExecutionService.java:203)",
            "\tat java.util.concurrent.FutureTask.run(FutureTask.java:266)",
            "Caused by: java.sql.SQLException: Connection refused to host: 10.0.1.5:5432",
            "\tat org.postgresql.core.v3.ConnectionFactoryImpl.openConnectionImpl(ConnectionFactoryImpl.java:315)",
            "\tat org.postgresql.Driver.connect(Driver.java:283)",
            "\t... 4 more",
        };

        List<MultilineAssembler.LineElement> result;
        for (int i = 0; i < stackTrace.length; i++) {
            result = asm.addLine(line(stackTrace[i], i + 1, 1000L + i));
            Assertions.assertNull(result, "No flush expected within the stack trace at line " + i);
        }

        Assertions.assertTrue(asm.hasPending());
        Assertions.assertEquals(10, asm.flush().size());
    }

    /** Two consecutive Java exceptions separated by a normal log line. */
    @Test
    void consecutiveJavaExceptionsAreSplitCorrectly() {
        MultilineAssembler asm =
                new MultilineAssembler(
                        TIMESTAMP_REGEX, MultilineAssembler.MatchMode.AFTER, false, 500);

        List<MultilineAssembler.LineElement> result;

        // First exception (3 lines)
        result =
                asm.addLine(
                        line(
                                "2024-01-15 10:23:45.678 ERROR [main] c.e.App - NullPointerException",
                                1,
                                1000L));
        Assertions.assertNull(result);

        result = asm.addLine(line("java.lang.NullPointerException: null", 2, 1001L));
        Assertions.assertNull(result);

        result = asm.addLine(line("\tat com.example.App.process(App.java:99)", 3, 1002L));
        Assertions.assertNull(result);

        // Second exception starts — this flushes the first (3 lines)
        result =
                asm.addLine(
                        line(
                                "2024-01-15 10:23:46.001 ERROR [pool-1] c.e.Worker - IllegalStateException",
                                4,
                                1003L));
        Assertions.assertNotNull(result);
        Assertions.assertEquals(3, result.size());
        Assertions.assertTrue(result.get(0).getText().contains("NullPointerException"));
        Assertions.assertEquals(
                "\tat com.example.App.process(App.java:99)", result.get(2).getText());

        result =
                asm.addLine(
                        line(
                                "java.lang.IllegalStateException: Connection pool exhausted",
                                5,
                                1004L));
        Assertions.assertNull(result);

        result =
                asm.addLine(
                        line(
                                "\tat com.example.pool.ConnectionPool.acquire(ConnectionPool.java:45)",
                                6,
                                1005L));
        Assertions.assertNull(result);

        result = asm.addLine(line("\tat com.example.Worker.run(Worker.java:22)", 7, 1006L));
        Assertions.assertNull(result);

        result =
                asm.addLine(
                        line(
                                "Caused by: java.net.SocketTimeoutException: connect timed out",
                                8,
                                1007L));
        Assertions.assertNull(result);

        result = asm.addLine(line("\t... 2 more", 9, 1008L));
        Assertions.assertNull(result);

        // Normal info log starts — flushes second exception (6 lines: lines 4-9)
        result =
                asm.addLine(
                        line(
                                "2024-01-15 10:23:47.500 INFO  [main] c.e.App - Recovery complete",
                                10,
                                1009L));
        Assertions.assertNotNull(result);
        Assertions.assertEquals(6, result.size());
        Assertions.assertTrue(result.get(0).getText().contains("IllegalStateException"));
        Assertions.assertEquals("\t... 2 more", result.get(5).getText());

        // Last line is buffered
        Assertions.assertTrue(asm.hasPending());
        List<MultilineAssembler.LineElement> last = asm.flush();
        Assertions.assertEquals(1, last.size());
        Assertions.assertTrue(last.get(0).getText().contains("Recovery complete"));
    }

    /** bufferFirstTimestamp resets after maxLines flush. */
    @Test
    void bufferFirstTimestampResetsOnMaxLinesFlush() {
        MultilineAssembler asm =
                new MultilineAssembler(
                        TIMESTAMP_REGEX, MultilineAssembler.MatchMode.AFTER, false, 2);

        asm.addLine(line("2024-01-01 first", 1, 1000L));
        Assertions.assertEquals(1000L, asm.getBufferFirstTimestamp());

        asm.addLine(line("\tat line2", 2, 2000L));
        Assertions.assertEquals(1000L, asm.getBufferFirstTimestamp());

        // Buffer is at maxLines=2, next add triggers flush
        asm.addLine(line("\tat line3", 3, 3000L));
        // After flush, line3 is the new buffer start
        Assertions.assertEquals(3000L, asm.getBufferFirstTimestamp());
    }
}
