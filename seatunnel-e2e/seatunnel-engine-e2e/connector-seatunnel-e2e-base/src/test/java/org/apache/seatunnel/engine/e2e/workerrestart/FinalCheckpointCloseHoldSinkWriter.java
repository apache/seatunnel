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

package org.apache.seatunnel.engine.e2e.workerrestart;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Writer of {@link FinalCheckpointCloseHoldSink}.
 *
 * <p>Rows are appended to a file that is unique per writer instance, flushed at every {@code
 * prepareCommit()} (which the final barrier triggers before the task acknowledges it, so all rows
 * are durable before the master can complete the final checkpoint) and closed at the start of
 * {@link #close()}. Only then does {@code close()} park on the hold gate, so the file already holds
 * the complete output of this writer while the task is frozen and the test can count it later.
 */
@Slf4j
public class FinalCheckpointCloseHoldSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final String holdKey;

    /** Output file of this writer instance; opened lazily on the first row. */
    private final Path outputFile;

    private BufferedWriter fileWriter;

    private long writtenRows;

    public FinalCheckpointCloseHoldSinkWriter(
            SinkWriter.Context context, String holdKey, String outputPath) {
        this.holdKey = holdKey;
        this.outputFile =
                Paths.get(
                        outputPath,
                        String.format(
                                "hold-sink-%d-%s.txt",
                                context.getIndexOfSubtask(),
                                UUID.randomUUID().toString().replace("-", "")));
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (fileWriter == null) {
            Files.createDirectories(outputFile.getParent());
            fileWriter = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8);
        }
        fileWriter.write(
                Arrays.stream(element.getFields())
                        .map(String::valueOf)
                        .collect(Collectors.joining("\t")));
        fileWriter.newLine();
        writtenRows++;
    }

    /**
     * Flushes the rows received so far. The final barrier calls this before the task acknowledges
     * it, so every row is on disk before the master can complete the final checkpoint.
     */
    @Override
    public Optional<Void> prepareCommit() {
        if (fileWriter != null) {
            try {
                fileWriter.flush();
            } catch (IOException e) {
                throw new UncheckedIOException("Flush of " + outputFile + " failed", e);
            }
        }
        return Optional.empty();
    }

    /**
     * Completes the durable output first and only then parks on the hold gate, so the task stays
     * RUNNING for the master exactly in the window after the final checkpoint completed.
     */
    @Override
    public void close() throws IOException {
        if (fileWriter != null) {
            fileWriter.close();
            fileWriter = null;
        }
        // WARN on purpose: the module's test logging keeps the root logger at WARN.
        log.warn(
                "Writer {} wrote {} rows, holding close() on key {}",
                outputFile.getFileName(),
                writtenRows,
                holdKey);
        FinalCheckpointCloseHoldGate.awaitRelease(holdKey);
        log.warn(
                "Writer {} released from close() hold on key {}",
                outputFile.getFileName(),
                holdKey);
    }
}
