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

package org.apache.seatunnel.engine.e2e.partialfailure;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Writer of {@link PartialFailureGateSink}.
 *
 * <p>With {@code fail_after_release=true} the writer parks on the first row it receives, before
 * touching the element, and throws the injected failure only after the test released the gate. The
 * failure therefore reaches the engine after the test observed the job's pipelines {@code RUNNING}
 * and the healthy sibling writing rows, which is the precondition of the regression this test
 * guards: a runtime task failure must cancel the still-running sibling pipelines.
 *
 * <p>Without {@code fail_after_release} the writer appends every row to a file that is unique per
 * writer instance and flushes it immediately, so the test can poll the file as a real progress
 * signal of the healthy sibling pipeline instead of relying on a status field.
 */
@Slf4j
public class PartialFailureGateSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private static final String OUT_OF_MEMORY_MESSAGE =
            "Injected runtime failure: the healthy sibling pipeline was observed to be progressing";

    /** Key that binds this writer to the gate armed by the test. */
    private final String holdKey;

    /** Output file of this writer instance; {@code null} for the failing writer. */
    private final Path outputFile;

    /** Whether this writer injects the failure instead of writing the rows. */
    private final boolean failAfterRelease;

    /** Index of the subtask that owns this writer, used to keep the log lines distinguishable. */
    private final int subtaskIndex;

    private BufferedWriter fileWriter;

    private long writtenRows;

    public PartialFailureGateSinkWriter(
            SinkWriter.Context context,
            String holdKey,
            String outputPath,
            boolean failAfterRelease) {
        this.holdKey = holdKey;
        this.failAfterRelease = failAfterRelease;
        this.subtaskIndex = context.getIndexOfSubtask();
        this.outputFile =
                outputPath == null
                        ? null
                        : Paths.get(
                                outputPath,
                                String.format(
                                        "partial-failure-sink-%d-%s.txt",
                                        subtaskIndex,
                                        UUID.randomUUID().toString().replace("-", "")));
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (failAfterRelease) {
            parkUntilReleasedThenFail();
            return;
        }
        appendRow(element);
    }

    /**
     * Parks the task thread inside {@code write()} until the test releases the gate and only then
     * injects the failure. Compared to a delay inside the writer this changes the injection from a
     * race the test hopes to win into a condition the test observed itself.
     */
    private void parkUntilReleasedThenFail() {
        // WARN on purpose: the module's test logging keeps the root logger at WARN.
        log.warn(
                "Sink writer of subtask {} reached its first row and parks on hold key {}",
                subtaskIndex,
                holdKey);
        PartialFailureGate.awaitRelease(holdKey);
        log.warn(
                "Sink writer of subtask {} released from hold key {}, injecting the runtime failure",
                subtaskIndex,
                holdKey);
        throw new OutOfMemoryError(OUT_OF_MEMORY_MESSAGE);
    }

    private void appendRow(SeaTunnelRow element) throws IOException {
        if (fileWriter == null) {
            Files.createDirectories(outputFile.getParent());
            fileWriter = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8);
        }
        fileWriter.write(
                Arrays.stream(element.getFields())
                        .map(String::valueOf)
                        .collect(Collectors.joining("\t")));
        fileWriter.newLine();
        // Flushed on every row on purpose: the test polls this file as the sibling progress signal.
        fileWriter.flush();
        writtenRows++;
    }

    @Override
    public void close() throws IOException {
        if (fileWriter != null) {
            fileWriter.close();
            fileWriter = null;
            log.warn(
                    "Sink writer {} closed after writing {} rows",
                    outputFile.getFileName(),
                    writtenRows);
        }
    }
}
