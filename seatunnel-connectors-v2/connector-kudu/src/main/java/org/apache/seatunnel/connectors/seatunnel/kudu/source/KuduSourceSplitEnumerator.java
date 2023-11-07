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

package org.apache.seatunnel.connectors.seatunnel.kudu.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient.KuduInputFormat;
import org.apache.seatunnel.connectors.seatunnel.kudu.state.KuduSourceState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KuduSourceSplitEnumerator
        implements SourceSplitEnumerator<KuduSourceSplit, KuduSourceState> {

    private static final Logger log = LoggerFactory.getLogger(KuduSourceSplitEnumerator.class);
    private final SourceSplitEnumerator.Context<KuduSourceSplit> enumeratorContext;
    private KuduSourceState checkpointState;
    private KuduSourceConfig kuduSourceConfig;
    private final Map<Integer, List<KuduSourceSplit>> pendingSplits;

    private final KuduInputFormat kuduInputFormat;

    private final Object stateLock = new Object();
    private volatile boolean shouldEnumerate;

    public KuduSourceSplitEnumerator(
            Context<KuduSourceSplit> enumeratorContext,
            KuduSourceConfig kuduSourceConfig,
            KuduInputFormat kuduInputFormat) {
        this(enumeratorContext, kuduSourceConfig, kuduInputFormat, null);
    }

    public KuduSourceSplitEnumerator(
            SourceSplitEnumerator.Context<KuduSourceSplit> enumeratorContext,
            KuduSourceConfig kuduSourceConfig,
            KuduInputFormat kuduInputFormat,
            KuduSourceState checkpointState) {
        this.enumeratorContext = enumeratorContext;
        this.kuduSourceConfig = kuduSourceConfig;
        this.pendingSplits = new HashMap<>();
        this.kuduInputFormat = kuduInputFormat;
        this.shouldEnumerate = checkpointState == null;
        if (checkpointState != null) {
            this.shouldEnumerate = checkpointState.isShouldEnumerate();
            this.pendingSplits.putAll(checkpointState.getPendingSplits());
        }
    }

    @Override
    public void open() {
        kuduInputFormat.openInputFormat();
    }

    @Override
    public void run() throws IOException {
        Set<Integer> readers = enumeratorContext.registeredReaders();
        if (shouldEnumerate) {
            Set<KuduSourceSplit> newSplits = discoverySplits();

            synchronized (stateLock) {
                addPendingSplit(newSplits);
                shouldEnumerate = false;
            }

            assignSplit(readers);
        }

        log.debug(
                "No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(enumeratorContext::signalNoMoreSplits);
    }

    private Set<KuduSourceSplit> discoverySplits() throws IOException {
        return kuduInputFormat.createInputSplits();
    }

    @Override
    public void close() throws IOException {
        kuduInputFormat.closeInputFormat();
    }

    @Override
    public void addSplitsBack(List<KuduSourceSplit> splits, int subtaskId) {
        log.debug("Add back splits {} to KuduSourceSplitEnumerator.", splits);
        synchronized (stateLock) {
            if (!splits.isEmpty()) {
                addPendingSplit(splits);
                assignSplit(Collections.singletonList(subtaskId));
            }
        }
    }

    private void assignSplit(Collection<Integer> readers) {
        log.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<KuduSourceSplit> assignmentForReader = pendingSplits.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info("Assign splits {} to reader {}", assignmentForReader, reader);
                try {
                    enumeratorContext.assignSplit(reader, assignmentForReader);
                } catch (Exception e) {
                    log.error(
                            "Failed to assign splits {} to reader {}",
                            assignmentForReader,
                            reader,
                            e);
                    pendingSplits.put(reader, assignmentForReader);
                }
            }
        }
    }

    private void addPendingSplit(Collection<KuduSourceSplit> splits) {
        int readerCount = enumeratorContext.currentParallelism();
        for (KuduSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning {} to {} reader.", split, ownerReader);
            pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private int getSplitOwner(String splitId, int numReaders) {
        return (splitId.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new KuduConnectorException(
                CommonErrorCode.UNSUPPORTED_OPERATION,
                String.format("Unsupported handleSplitRequest: %d", subtaskId));
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to KuduSourceSplitEnumerator.", subtaskId);
        synchronized (stateLock) {
            if (!pendingSplits.isEmpty()) {
                assignSplit(Collections.singletonList(subtaskId));
            }
        }
    }

    @Override
    public KuduSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new KuduSourceState(shouldEnumerate, new HashMap<>(pendingSplits));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
