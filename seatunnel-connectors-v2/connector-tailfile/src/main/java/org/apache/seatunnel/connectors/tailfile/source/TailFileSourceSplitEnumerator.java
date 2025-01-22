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

package org.apache.seatunnel.connectors.tailfile.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.tailfile.source.tailfile.ChangedFiles;
import org.apache.seatunnel.connectors.tailfile.source.tailfile.FileMatcher;
import org.apache.seatunnel.connectors.tailfile.source.tailfile.FileNode;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class TailFileSourceSplitEnumerator
        implements SourceSplitEnumerator<TailFileSourceSplit, TailFileSourceState> {

    // todo 删除
    private final Object stateLock = new Object();
    private final Context<TailFileSourceSplit> context;
    private final TailFileSourceConfig config;

    private final Map<Integer, List<TailFileSourceSplit>> pendingSplits;
    private final List<Integer> waitingReaders;
    private FileMatcher fileMatcher;

    public TailFileSourceSplitEnumerator(
            Context<TailFileSourceSplit> context, TailFileSourceConfig config) {
        this(context, config, null);
    }

    public TailFileSourceSplitEnumerator(
            Context<TailFileSourceSplit> context,
            TailFileSourceConfig config,
            TailFileSourceState state) {
        this.context = context;
        this.config = config;
        this.pendingSplits = new HashMap<>();
        if (state == null) {
            this.waitingReaders = new ArrayList<>();
        } else {
            this.waitingReaders =
                    Collections.synchronizedList(
                            IntStream.range(0, context.currentParallelism())
                                    .boxed()
                                    .collect(Collectors.toList()));
        }
    }

    @Override
    public void open() {
        log.info("Open split enumerator.");
    }

    @Override
    public void run() throws Exception {
        if (!waitingReaders.isEmpty()) {
            log.debug("Waiting for readers {} to be ready.", waitingReaders);
            Thread.sleep(1000L);
            return;
        }

        if (fileMatcher == null) {
            this.fileMatcher = createFileMatcher(pendingSplits);
            log.info("Readers are ready. Start to file matching...");
        }

        Set<Integer> readers = context.registeredReaders();
        synchronized (stateLock) {
            ChangedFiles changedFiles = fileMatcher.getChangedFiles();
            for (FileNode fileNode : changedFiles.getAddedFiles()) {
                log.info("Found new file: {}", fileNode);
                addPendingSplit(TailFileSourceSplit.of(fileNode));
            }
            for (FileNode fileNode : changedFiles.getRemovedFiles()) {
                log.info("Found removed file: {}", fileNode);
            }
            assignSplit(readers);

            // todo
            stateLock.wait(config.getScanInterval());
        }

        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            readers.forEach(context::signalNoMoreSplits);
            log.info("No more splits to assign. Sending NoMoreSplitsEvent to reader {}.", readers);
        }
    }

    @Override
    public void close() {
        log.info("Close split enumerator.");
    }

    @Override
    public void addSplitsBack(List<TailFileSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            synchronized (stateLock) {
                for (TailFileSourceSplit split : splits) {
                    addPendingSplit(split);
                }
            }
        }
        waitingReaders.remove((Object) subtaskId);
        log.info("Add back splits {} to Enumerator.", splits.size());
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        synchronized (stateLock) {
            stateLock.notify();
        }
    }

    @Override
    public void registerReader(int subtaskId) {
        log.info("Register reader {} to Enumerator.", subtaskId);
    }

    @Override
    public TailFileSourceState snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return TailFileSourceState.EMPTY;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    private FileMatcher createFileMatcher(Map<Integer, List<TailFileSourceSplit>> state) {
        if (state.isEmpty()) {
            return new FileMatcher(
                    config.isCachePatternMatching(), config.getDir(), config.getPath());
        }

        List<FileNode> fileNodes =
                state.values().stream()
                        .flatMap(Collection::stream)
                        .map(
                                split ->
                                        new FileNode(
                                                split.getFilepath(),
                                                split.getInode(),
                                                split.getLastModified()))
                        .collect(Collectors.toList());
        return new FileMatcher(
                config.isCachePatternMatching(), config.getDir(), config.getPath(), fileNodes);
    }

    private void assignSplit(Collection<Integer> readers) {
        log.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<TailFileSourceSplit> assignmentForReader = pendingSplits.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                try {
                    log.debug("Assign splits {} to reader {}", assignmentForReader, reader);
                    context.assignSplit(reader, assignmentForReader);
                } catch (Exception e) {
                    log.error("Failed to assign split to reader.", e);
                    pendingSplits.put(reader, assignmentForReader);
                }
            }
        }
    }

    private void addPendingSplit(TailFileSourceSplit split) {
        int ownerReader = getSplitOwner(split.splitId(), context.currentParallelism());
        log.debug("Assigning {} to {} reader.", split, ownerReader);

        pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
    }

    private static int getSplitOwner(String splitId, int numReaders) {
        return (splitId.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    private void checkThrowInterruptedException() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            log.info("Enumerator thread is interrupted.");
            throw new InterruptedException("Enumerator thread is interrupted.");
        }
    }
}
