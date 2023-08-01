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

package org.apache.seatunnel.connectors.seatunnel.mongodb.source.enumerator;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplit;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplitStrategy;

import com.mongodb.MongoNamespace;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** MongoSplitEnumerator generates {@link MongoSplit} according to partition strategies. */
@Slf4j
public class MongodbSplitEnumerator
        implements SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> {

    private final ArrayList<MongoSplit> pendingSplits = Lists.newArrayList();

    private final Context<MongoSplit> context;

    private final MongodbClientProvider clientProvider;

    private final MongoSplitStrategy strategy;

    public MongodbSplitEnumerator(
            Context<MongoSplit> context,
            MongodbClientProvider clientProvider,
            MongoSplitStrategy strategy) {
        this(context, clientProvider, strategy, Collections.emptyList());
    }

    public MongodbSplitEnumerator(
            Context<MongoSplit> context,
            MongodbClientProvider clientProvider,
            MongoSplitStrategy strategy,
            List<MongoSplit> splits) {
        this.context = context;
        this.clientProvider = clientProvider;
        this.strategy = strategy;
        this.pendingSplits.addAll(splits);
    }

    @Override
    public void open() {}

    @Override
    public synchronized void run() {
        log.info("Starting MongoSplitEnumerator.");
        Set<Integer> readers = context.registeredReaders();
        pendingSplits.addAll(strategy.split());
        MongoNamespace namespace = clientProvider.getDefaultCollection().getNamespace();
        log.info(
                "Added {} pending splits for namespace {}.",
                pendingSplits.size(),
                namespace.getFullName());
        assignSplits(readers);
    }

    @Override
    public void close() {
        if (clientProvider != null) {
            clientProvider.close();
        }
    }

    @Override
    public void addSplitsBack(List<MongoSplit> splits, int subtaskId) {
        if (splits != null) {
            log.info("Received {} split(s) back from subtask {}.", splits.size(), subtaskId);
            pendingSplits.addAll(splits);
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new MongodbConnectorException(
                CommonErrorCode.UNSUPPORTED_OPERATION,
                String.format("Unsupported handleSplitRequest: %d", subtaskId));
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to MongodbSplitEnumerator.", subtaskId);
        if (!pendingSplits.isEmpty()) {
            assignSplits(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public ArrayList<MongoSplit> snapshotState(long checkpointId) {
        return pendingSplits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Do nothing
    }

    private synchronized void assignSplits(Collection<Integer> readers) {
        log.debug("Assign pendingSplits to readers {}", readers);
        int numReaders = readers.size();

        Map<Integer, List<MongoSplit>> splitsBySubtaskId =
                pendingSplits.stream()
                        .collect(
                                Collectors.groupingBy(
                                        split -> getSplitOwner(split.splitId(), numReaders)));

        readers.forEach(subtaskId -> assignSplitsToSubtask(subtaskId, splitsBySubtaskId));

        pendingSplits.clear();
        readers.forEach(context::signalNoMoreSplits);
    }

    private void assignSplitsToSubtask(
            Integer subtaskId, Map<Integer, List<MongoSplit>> splitsBySubtaskId) {
        log.info("Received split request from taskId {}.", subtaskId);

        List<MongoSplit> assignedSplits =
                splitsBySubtaskId.getOrDefault(subtaskId, Collections.emptyList());

        context.assignSplit(subtaskId, assignedSplits);
        log.info(
                "Assigned {} splits to subtask {}, remaining splits: {}.",
                assignedSplits.size(),
                subtaskId,
                pendingSplits.size() - assignedSplits.size());
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }
}
