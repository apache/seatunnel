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
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
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
    private static final long PROCESS_DESTROY_TIMEOUT_SECONDS = 5L;

    private final PythonSourceConfig sourceConfig;
    private final CatalogTable catalogTable;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private final Deque<String> recentStderrLines;

    private Process process;
    private BufferedReader stdoutReader;
    private Thread stderrPumpThread;
    private volatile Throwable stderrPumpFailure;

    public PythonSourceReader(PythonSourceConfig sourceConfig, CatalogTable catalogTable) {
        this.sourceConfig = sourceConfig;
        this.catalogTable = catalogTable;
        this.deserializationSchema = createDeserializationSchema(sourceConfig, catalogTable);
        this.recentStderrLines = new ArrayDeque<>(STDERR_HISTORY_LIMIT);
    }

    @Override
    public void open() throws Exception {
        Path scriptPath = validateScriptPath();
        ProcessBuilder processBuilder =
                new ProcessBuilder(
                        sourceConfig.getPythonExecutable(), scriptPath.toAbsolutePath().toString());
        configureWorkingDirectory(processBuilder, scriptPath);

        try {
            this.process = processBuilder.start();
        } catch (IOException e) {
            throw new IOException(
                    "Failed to start python source process with executable ["
                            + sourceConfig.getPythonExecutable()
                            + "] and script ["
                            + scriptPath
                            + "]",
                    e);
        }

        this.stdoutReader =
                new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        startStderrPump();
        writeInitialScriptConfig(sourceConfig.getPythonScriptConfig());
    }

    /**
     * Drain the full bounded stdout stream in a single poll and emit rows in script output order.
     */
    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        String line;
        while ((line = stdoutReader.readLine()) != null) {
            checkStderrPumpFailure();

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

        waitForProcessExit();
    }

    @Override
    public void close() throws IOException {
        IOException closeException = null;
        if (stdoutReader != null) {
            try {
                stdoutReader.close();
            } catch (IOException e) {
                closeException = e;
            }
        }

        if (process != null) {
            destroyProcess(process);
        }

        if (stderrPumpThread != null) {
            try {
                stderrPumpThread.join(TimeUnit.SECONDS.toMillis(PROCESS_DESTROY_TIMEOUT_SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (closeException == null) {
                    closeException = new IOException("Interrupted while closing stderr pump", e);
                }
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
                        "python.working_directory is not a directory: " + processDirectory);
            }
        } else {
            processDirectory = scriptPath.toAbsolutePath().getParent();
        }

        if (processDirectory != null) {
            processBuilder.directory(processDirectory.toFile());
        }
    }

    /**
     * The first stdin line is the stable contract between Java and the Python script in Phase 1.
     */
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
                                while ((line = stderrReader.readLine()) != null) {
                                    appendStderrLine(line);
                                    LOG.warn("Python source stderr: {}", line);
                                }
                            } catch (IOException e) {
                                stderrPumpFailure = e;
                            }
                        },
                        "python-source-stderr-pump");
        stderrPumpThread.setDaemon(true);
        stderrPumpThread.start();
    }

    private void waitForProcessExit() throws Exception {
        int exitCode = process.waitFor();
        if (stderrPumpThread != null) {
            stderrPumpThread.join(TimeUnit.SECONDS.toMillis(PROCESS_DESTROY_TIMEOUT_SECONDS));
        }
        checkStderrPumpFailure();

        if (exitCode != 0) {
            throw new IllegalStateException(
                    "Python source process exited with code " + exitCode + formatRecentStderr());
        }
    }

    private void checkStderrPumpFailure() {
        if (stderrPumpFailure == null) {
            return;
        }
        throw new IllegalStateException(
                "Failed to consume python source stderr", stderrPumpFailure);
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

    private static void destroyProcess(Process runningProcess) {
        if (!runningProcess.isAlive()) {
            return;
        }

        runningProcess.destroy();
        try {
            if (!runningProcess.waitFor(PROCESS_DESTROY_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                runningProcess.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            runningProcess.destroyForcibly();
        }
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
