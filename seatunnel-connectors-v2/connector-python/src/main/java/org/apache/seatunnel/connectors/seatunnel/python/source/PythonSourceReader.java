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

package org.apache.seatunnel.connectors.seatunnel.python.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Reader that drains one Python process stdout stream and converts each line into a SeaTunnelRow.
 *
 * <p>The reader forwards stderr to worker logs and keeps the latest stderr lines so non-zero exits
 * report actionable context instead of a generic process failure.
 */
public class PythonSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private static final Logger LOG = LoggerFactory.getLogger(PythonSourceReader.class);
    private static final int STDERR_HISTORY_LIMIT = 50;
    private static final int STDOUT_QUEUE_CAPACITY = 256;
    private static final int MAX_ROWS_PER_POLL = 128;
    private static final long INITIAL_CONFIG_WRITE_TIMEOUT_SECONDS = 5L;
    private static final long PROCESS_DESTROY_TIMEOUT_SECONDS = 5L;
    private static final long PROCESS_EXIT_CHECK_TIMEOUT_MILLIS = 200L;
    private static final long STDOUT_POLL_TIMEOUT_MILLIS = 200L;
    private static final long QUEUE_OFFER_TIMEOUT_MILLIS = 200L;

    private final PythonSourceConfig sourceConfig;
    private final CatalogTable catalogTable;
    /** Bridges bounded process completion back to the engine source lifecycle. */
    private final SingleSplitReaderContext readerContext;

    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private final Deque<String> recentStderrLines;
    private final BlockingQueue<String> stdoutLines;
    /** Serializes process publication, polling terminal state, and engine-driven close. */
    private final Object lifecycleLock = new Object();

    private Process process;
    private Thread stdinWriterThread;
    private Thread stderrPumpThread;
    private Thread stdoutPumpThread;
    private volatile Throwable stdinWriterFailure;
    private volatile Throwable stderrPumpFailure;
    private volatile Throwable stdoutPumpFailure;
    /** Set after the complete initial JSON payload has been written and stdin has been closed. */
    private volatile boolean initialConfigWritten;
    /** Set after the stdout pump reaches EOF or is intentionally stopped during cleanup. */
    private volatile boolean stdoutCompleted;
    /** Stops all pump loops when the engine closes or cancels this reader. */
    private volatile boolean closeRequested;
    /** Distinguishes intentional stderr pipe shutdown from a real pump failure. */
    private volatile boolean stderrShutdownRequested;
    /** Prevents duplicate bounded-source completion signals across later poll calls. */
    private volatile boolean processExitVerified;
    /**
     * Immutable deadline for draining inherited stdout after the direct Python process exits.
     *
     * <p>Child output must not renew this deadline, otherwise a periodic child process can keep a
     * bounded source alive forever after the configured script has already exited.
     */
    private volatile long stdoutCloseDeadlineNanos;
    /** Number of poll calls that must finish before close can release process resources. */
    private int activePolls;
    /** Ensures concurrent close callers share one deterministic cleanup operation. */
    private boolean closeInProgress;
    /** Records that process streams and pump threads have completed cleanup. */
    private boolean closeComplete;

    public PythonSourceReader(
            PythonSourceConfig sourceConfig,
            CatalogTable catalogTable,
            SingleSplitReaderContext readerContext) {
        this.sourceConfig = sourceConfig;
        this.catalogTable = catalogTable;
        this.readerContext = readerContext;
        this.deserializationSchema = createDeserializationSchema(sourceConfig, catalogTable);
        this.recentStderrLines = new ArrayDeque<>(STDERR_HISTORY_LIMIT);
        this.stdoutLines = new ArrayBlockingQueue<>(STDOUT_QUEUE_CAPACITY);
    }

    @Override
    public void open() throws Exception {
        synchronized (lifecycleLock) {
            if (closeRequested) {
                throw new IOException("Python source reader has already been closed");
            }
        }
        Path scriptPath = validateScriptPath().toAbsolutePath().normalize();
        Path resolvedExecutable =
                PythonSourceExecutionPolicy.resolveExecutable(sourceConfig.getPythonExecutable());
        LOG.warn(
                "Python source runs unsandboxed external code. Resolved executable='{}', scriptOrigin='python.script.path={}', guarded by system properties '{}','{}'.",
                resolvedExecutable,
                scriptPath,
                PythonSourceExecutionPolicy.PYTHON_SOURCE_ENABLED_PROPERTY,
                PythonSourceExecutionPolicy.PYTHON_ALLOWED_EXECUTABLES_PROPERTY);
        ProcessBuilder processBuilder =
                new ProcessBuilder(resolvedExecutable.toString(), scriptPath.toString());
        configureWorkingDirectory(processBuilder, scriptPath);

        synchronized (lifecycleLock) {
            if (closeRequested) {
                throw new IOException("Python source reader has already been closed");
            }
            try {
                this.process = processBuilder.start();
            } catch (IOException e) {
                throw new IOException(
                        "Failed to start python source process with executable ["
                                + resolvedExecutable
                                + "] and script ["
                                + scriptPath
                                + "]",
                        e);
            }

            try {
                startStderrPump();
                startStdoutPump();
                startStdinWriter(sourceConfig.getPythonScriptConfig());
                waitForInitialConfigWrite();
            } catch (Exception e) {
                try {
                    close();
                } catch (IOException closeException) {
                    e.addSuppressed(closeException);
                }
                throw e;
            }
        }
    }

    /**
     * Polls a bounded queue populated by a dedicated stdout pump so task cancellation can still
     * reach the process even when the Python script keeps stdout open for a long time.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (!beginPoll()) {
            return;
        }

        try {
            checkPumpFailures();

            String firstLine = stdoutLines.poll(STDOUT_POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (closeRequested) {
                return;
            }
            if (firstLine == null) {
                finishIfProcessCompleted();
                checkPumpFailures();
                return;
            }
            synchronized (output.getCheckpointLock()) {
                if (closeRequested) {
                    return;
                }
                emitRow(firstLine, output);

                for (int emittedRows = 1; emittedRows < MAX_ROWS_PER_POLL; emittedRows++) {
                    if (closeRequested) {
                        return;
                    }
                    String nextLine = stdoutLines.poll();
                    if (nextLine == null) {
                        break;
                    }
                    emitRow(nextLine, output);
                }
            }

            if (!closeRequested) {
                finishIfProcessCompleted();
            }
        } finally {
            endPoll();
        }
    }

    @Override
    public void close() throws IOException {
        boolean interrupted = false;
        synchronized (lifecycleLock) {
            closeRequested = true;
            while (closeInProgress && !closeComplete) {
                interrupted |= waitForLifecycleChange();
            }
            if (closeComplete) {
                restoreInterrupt(interrupted);
                return;
            }
            closeInProgress = true;
            while (activePolls > 0) {
                interrupted |= waitForLifecycleChange();
            }
        }

        IOException closeException = null;
        try {
            boolean inheritedStdoutClose = stdoutCloseDeadlineNanos != 0L;
            long closeJoinTimeoutMillis =
                    inheritedStdoutClose
                            ? PROCESS_EXIT_CHECK_TIMEOUT_MILLIS
                            : TimeUnit.SECONDS.toMillis(PROCESS_DESTROY_TIMEOUT_SECONDS);
            if (process != null) {
                destroyProcess(process, closeJoinTimeoutMillis);
                closeProcessStreams(process, closeJoinTimeoutMillis);
            }

            // Child processes can inherit stdin on Windows as well, so the bounded close path
            // must not wait the full default timeout for the writer thread after cancellation.
            closeException =
                    joinThread(
                            stdinWriterThread,
                            "stdin writer",
                            closeException,
                            closeJoinTimeoutMillis,
                            !inheritedStdoutClose);
            closeException =
                    joinThread(
                            stdoutPumpThread,
                            "stdout pump",
                            closeException,
                            closeJoinTimeoutMillis,
                            !inheritedStdoutClose);
            closeException =
                    joinThread(
                            stderrPumpThread,
                            "stderr pump",
                            closeException,
                            closeJoinTimeoutMillis,
                            !inheritedStdoutClose);
        } finally {
            synchronized (lifecycleLock) {
                closeComplete = true;
                closeInProgress = false;
                lifecycleLock.notifyAll();
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
            if (closeException == null) {
                closeException = new IOException("Interrupted while closing python source reader");
            }
        }
        if (closeException != null) {
            throw closeException;
        }
    }

    private Path validateScriptPath() {
        Path scriptPath = Paths.get(sourceConfig.getPythonScriptPath());
        if (!Files.isRegularFile(scriptPath)) {
            throw new IllegalArgumentException(
                    "python.script.path does not point to a readable file: " + scriptPath);
        }
        return scriptPath;
    }

    private void configureWorkingDirectory(ProcessBuilder processBuilder, Path scriptPath) {
        String workingDirectory = sourceConfig.getPythonWorkingDirectory();
        Path processDirectory;
        if (workingDirectory != null && !workingDirectory.trim().isEmpty()) {
            processDirectory = Paths.get(workingDirectory);
            if (!Files.isDirectory(processDirectory)) {
                throw new IllegalArgumentException(
                        "python.working.directory is not a directory: " + processDirectory);
            }
        } else {
            processDirectory = scriptPath.toAbsolutePath().getParent();
        }

        if (processDirectory != null) {
            processBuilder.directory(processDirectory.toFile());
        }
    }

    /**
     * Starts a bounded background write so a script that ignores stdin cannot block open forever.
     */
    private void startStdinWriter(Map<String, Object> scriptConfig) {
        stdinWriterThread =
                new Thread(
                        () -> {
                            try {
                                writeInitialScriptConfig(scriptConfig);
                            } catch (Exception e) {
                                stdinWriterFailure = e;
                            } finally {
                                initialConfigWritten = true;
                            }
                        },
                        "python-source-stdin-writer");
        stdinWriterThread.setDaemon(true);
        stdinWriterThread.start();
    }

    /** Fails process initialization when the child does not consume the initial JSON contract. */
    private void waitForInitialConfigWrite() throws IOException {
        try {
            stdinWriterThread.join(TimeUnit.SECONDS.toMillis(INITIAL_CONFIG_WRITE_TIMEOUT_SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while writing python.script.config", e);
        }

        if (closeRequested) {
            throw new IOException("Python source was closed while writing python.script.config");
        }
        if (!initialConfigWritten) {
            throw new IOException(
                    "Timed out after "
                            + INITIAL_CONFIG_WRITE_TIMEOUT_SECONDS
                            + " seconds while writing python.script.config; ensure the Python script reads its first stdin line");
        }
        if (stdinWriterFailure != null) {
            if (!process.isAlive()) {
                // A closed stdin pipe surfaces here as a generic write IOException (e.g. "Broken
                // pipe"), but the actual contract violation is that the script exited without
                // reading python.script.config; report that instead of the write symptom.
                throw new IOException(
                        "Python script exited before reading its first stdin line;"
                                + " python.script.config could not be delivered"
                                + formatRecentStderr(),
                        stdinWriterFailure);
            }
            throw new IOException("Failed to write python.script.config", stdinWriterFailure);
        }
    }

    /** The first stdin line is the stable contract between Java and the Python script. */
    private void writeInitialScriptConfig(Map<String, Object> scriptConfig) throws IOException {
        try (BufferedWriter writer =
                new BufferedWriter(
                        new OutputStreamWriter(
                                process.getOutputStream(), StandardCharsets.UTF_8))) {
            writer.write(JsonUtils.toJsonString(scriptConfig));
            writer.newLine();
            writer.flush();
        }
    }

    private void startStderrPump() {
        stderrPumpThread =
                new Thread(
                        () -> {
                            try (BufferedReader stderrReader =
                                    new BufferedReader(
                                            new InputStreamReader(
                                                    process.getErrorStream(),
                                                    StandardCharsets.UTF_8))) {
                                String line;
                                int loggedLines = 0;
                                while ((line = stderrReader.readLine()) != null) {
                                    appendStderrLine(line);
                                    if (loggedLines < STDERR_HISTORY_LIMIT) {
                                        LOG.warn("Python source stderr: {}", line);
                                    } else if (loggedLines == STDERR_HISTORY_LIMIT) {
                                        LOG.warn(
                                                "Suppressing further Python source stderr lines for this process");
                                    }
                                    loggedLines++;
                                }
                            } catch (IOException e) {
                                if (!closeRequested && !stderrShutdownRequested) {
                                    stderrPumpFailure = e;
                                }
                            }
                        },
                        "python-source-stderr-pump");
        stderrPumpThread.setDaemon(true);
        stderrPumpThread.start();
    }

    private void startStdoutPump() {
        stdoutPumpThread =
                new Thread(
                        () -> {
                            try (BufferedReader reader =
                                    new BufferedReader(
                                            new InputStreamReader(
                                                    process.getInputStream(),
                                                    StandardCharsets.UTF_8))) {
                                String line;
                                while (!closeRequested && (line = reader.readLine()) != null) {
                                    offerStdoutLine(line);
                                }
                            } catch (ClosedByInterruptException e) {
                                Thread.currentThread().interrupt();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } catch (IOException e) {
                                if (!closeRequested) {
                                    stdoutPumpFailure = e;
                                }
                            } finally {
                                stdoutCompleted = true;
                            }
                        },
                        "python-source-stdout-pump");
        stdoutPumpThread.setDaemon(true);
        stdoutPumpThread.start();
    }

    private void offerStdoutLine(String line) throws InterruptedException {
        while (!closeRequested) {
            if (stdoutLines.offer(line, QUEUE_OFFER_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                return;
            }
        }
    }

    private void emitRow(String line, Collector<SeaTunnelRow> output) throws IOException {
        SeaTunnelRow row;
        try {
            row = deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IOException(
                    "Failed to deserialize python source stdout line [" + line + "]", e);
        }

        if (row != null) {
            output.collect(row);
        }
    }

    private void finishIfProcessCompleted() throws Exception {
        if (closeRequested || processExitVerified) {
            return;
        }

        if (process.isAlive()) {
            return;
        }

        if (!verifyProcessExit()) {
            return;
        }
        if (closeRequested) {
            return;
        }
        synchronized (lifecycleLock) {
            if (closeRequested) {
                return;
            }
            processExitVerified = true;
            noMoreSplits = true;
            readerContext.signalNoMoreElement();
        }
    }

    /** Returns false when engine cancellation wins the race with normal process completion. */
    private boolean verifyProcessExit() throws Exception {
        int exitCode = process.waitFor();
        if (closeRequested) {
            return false;
        }
        if (!finishPumpsAfterProcessExit()) {
            return false;
        }
        if (closeRequested) {
            return false;
        }
        checkPumpFailures();

        if (exitCode != 0) {
            throw new IllegalStateException(
                    "Python source process exited with code " + exitCode + formatRecentStderr());
        }
        return true;
    }

    /**
     * Waits for stdout EOF after the direct process exits. Buffered rows are returned to pollNext,
     * while an inherited pipe that never reaches EOF fails explicitly instead of hanging forever.
     *
     * @return true when both output pumps are finished and no stdout rows remain buffered
     */
    private boolean finishPumpsAfterProcessExit() throws IOException {
        if (!awaitStdoutPumpAfterProcessExit()) {
            return false;
        }

        IOException joinException = joinThread(stdoutPumpThread, "stdout pump", null);
        if (!stdoutLines.isEmpty()) {
            return false;
        }
        waitForPumpDrain(stderrPumpThread);
        if (isAlive(stderrPumpThread)) {
            stderrShutdownRequested = true;
            closeQuietly(process.getErrorStream());
        }

        joinException = joinThread(stderrPumpThread, "stderr pump", joinException);
        if (joinException != null) {
            throw joinException;
        }
        return true;
    }

    /**
     * Gives stdout one short drain interval per poll after the direct process exits.
     *
     * @return true when stdout reached EOF with no buffered rows, or false when polling must
     *     continue or engine cancellation won the race
     */
    private boolean awaitStdoutPumpAfterProcessExit() throws IOException {
        if (closeRequested) {
            return false;
        }
        if (stdoutCloseDeadlineNanos == 0L) {
            stdoutCloseDeadlineNanos =
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(PROCESS_DESTROY_TIMEOUT_SECONDS);
        }
        if (!isAlive(stdoutPumpThread)) {
            return !closeRequested && stdoutLines.isEmpty();
        }
        if (!stdoutLines.isEmpty()) {
            if (System.nanoTime() >= stdoutCloseDeadlineNanos) {
                throw inheritedStdoutTimeout();
            }
            return false;
        }

        long remainingNanos = stdoutCloseDeadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            throw inheritedStdoutTimeout();
        }
        long waitMillis =
                Math.min(
                        PROCESS_EXIT_CHECK_TIMEOUT_MILLIS,
                        Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
        try {
            stdoutPumpThread.join(waitMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(
                    "Interrupted while draining Python source stdout after process exit", e);
        }

        if (closeRequested) {
            return false;
        }
        if (!stdoutLines.isEmpty()) {
            if (System.nanoTime() >= stdoutCloseDeadlineNanos) {
                throw inheritedStdoutTimeout();
            }
            return false;
        }
        if (isAlive(stdoutPumpThread)) {
            if (System.nanoTime() >= stdoutCloseDeadlineNanos) {
                throw inheritedStdoutTimeout();
            }
            return false;
        }
        return true;
    }

    /** Creates the explicit protocol failure used when a child keeps the stdout pipe open. */
    private static IOException inheritedStdoutTimeout() {
        return new IOException(
                "Timed out waiting for Python source stdout to close after the process exited; ensure child processes do not inherit stdout");
    }

    /** Gives a pump a short grace period to consume bytes already written by the direct child. */
    private void waitForPumpDrain(Thread thread) throws IOException {
        if (thread == null || !thread.isAlive()) {
            return;
        }
        try {
            thread.join(PROCESS_EXIT_CHECK_TIMEOUT_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while draining python source process output", e);
        }
    }

    private void checkPumpFailures() {
        if (stdoutPumpFailure != null) {
            throw new IllegalStateException(
                    "Failed to consume python source stdout", stdoutPumpFailure);
        }
        if (stderrPumpFailure == null) {
            return;
        }
        throw new IllegalStateException(
                "Failed to consume python source stderr", stderrPumpFailure);
    }

    /** Registers one poll so close cannot return while rows or completion are still emitted. */
    private boolean beginPoll() {
        synchronized (lifecycleLock) {
            if (noMoreSplits || closeRequested) {
                return false;
            }
            activePolls++;
            return true;
        }
    }

    /** Releases the close barrier after every row and terminal signal has completed. */
    private void endPoll() {
        synchronized (lifecycleLock) {
            activePolls--;
            lifecycleLock.notifyAll();
        }
    }

    /** Waits without abandoning cleanup when the close thread itself is interrupted. */
    private boolean waitForLifecycleChange() {
        try {
            lifecycleLock.wait();
            return false;
        } catch (InterruptedException e) {
            return true;
        }
    }

    /** Restores an interrupt consumed while waiting for another close caller. */
    private void restoreInterrupt(boolean interrupted) throws IOException {
        if (!interrupted) {
            return;
        }
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while closing python source reader");
    }

    private IOException joinThread(Thread thread, String threadName, IOException closeException) {
        return joinThread(
                thread,
                threadName,
                closeException,
                TimeUnit.SECONDS.toMillis(PROCESS_DESTROY_TIMEOUT_SECONDS));
    }

    private IOException joinThread(
            Thread thread, String threadName, IOException closeException, long timeoutMillis) {
        return joinThread(thread, threadName, closeException, timeoutMillis, true);
    }

    private IOException joinThread(
            Thread thread,
            String threadName,
            IOException closeException,
            long timeoutMillis,
            boolean failOnTimeout) {
        if (thread == null) {
            return closeException;
        }

        if (closeRequested) {
            thread.interrupt();
        }
        try {
            thread.join(timeoutMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (closeException == null) {
                return new IOException("Interrupted while closing python source " + threadName, e);
            }
        }
        if (thread.isAlive()) {
            if (failOnTimeout && closeException == null) {
                return new IOException("Timed out while closing python source " + threadName);
            }
            // Not surfaced as a close failure (failOnTimeout=false, or an earlier join already
            // produced closeException), but the thread outliving its bounded join is otherwise
            // invisible: log it so a leaked thread or inherited descendant is observable.
            LOG.warn(
                    "Python source {} thread did not finish within {}ms while closing; it may keep"
                            + " running along with any descendant process it holds open",
                    threadName,
                    timeoutMillis);
        }
        return closeException;
    }

    private synchronized void appendStderrLine(String line) {
        if (recentStderrLines.size() == STDERR_HISTORY_LIMIT) {
            recentStderrLines.removeFirst();
        }
        recentStderrLines.addLast(line);
    }

    private synchronized String formatRecentStderr() {
        if (recentStderrLines.isEmpty()) {
            return "";
        }
        return ". Recent stderr: " + String.join(" | ", recentStderrLines);
    }

    private static void destroyProcess(Process runningProcess, long timeoutMillis) {
        if (!runningProcess.isAlive()) {
            return;
        }

        boolean interrupted = false;
        runningProcess.destroy();
        try {
            if (!runningProcess.waitFor(timeoutMillis, TimeUnit.MILLISECONDS)) {
                runningProcess.destroyForcibly();
            }
        } catch (InterruptedException e) {
            interrupted = true;
            runningProcess.destroyForcibly();
        }
        if (runningProcess.isAlive()) {
            try {
                if (!runningProcess.waitFor(timeoutMillis, TimeUnit.MILLISECONDS)) {
                    LOG.warn("Python source process did not terminate after forced shutdown");
                }
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Closes all Java pipe endpoints so blocked reader and writer threads can terminate.
     *
     * <p>Bounded on purpose. When a child process inherited stdout, the pipe stays open on the
     * child's side and closing the read end blocks behind the pump thread's pending read until that
     * child exits. On Windows that wait is not interruptible by the close itself, so performing it
     * inline would let cancellation exceed the reader's bounded shutdown window. The close is
     * handed to a daemon thread instead: the process has already been destroyed by this point, so
     * the remaining handles are released either by that thread or by JVM exit.
     */
    private static void closeProcessStreams(Process runningProcess, long timeoutMillis) {
        Thread streamCloser =
                new Thread(
                        () -> {
                            closeQuietly(runningProcess.getOutputStream());
                            closeQuietly(runningProcess.getInputStream());
                            closeQuietly(runningProcess.getErrorStream());
                        },
                        "python-source-stream-closer");
        streamCloser.setDaemon(true);
        streamCloser.start();
        try {
            streamCloser.join(timeoutMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (streamCloser.isAlive()) {
            LOG.warn(
                    "Python source stream closer did not finish within {}ms; process stream"
                            + " handles may remain open until it completes or the JVM exits",
                    timeoutMillis);
        }
    }

    /** Stream cleanup is best effort because process termination is the authoritative boundary. */
    private static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            LOG.debug("Failed to close Python source process stream", e);
        }
    }

    private static boolean isAlive(Thread thread) {
        return thread != null && thread.isAlive();
    }

    private static DeserializationSchema<SeaTunnelRow> createDeserializationSchema(
            PythonSourceConfig sourceConfig, CatalogTable catalogTable) {
        return TextDeserializationSchema.builder()
                .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                .delimiter(sourceConfig.getFieldDelimiter())
                .setCatalogTable(catalogTable)
                .build();
    }
}
