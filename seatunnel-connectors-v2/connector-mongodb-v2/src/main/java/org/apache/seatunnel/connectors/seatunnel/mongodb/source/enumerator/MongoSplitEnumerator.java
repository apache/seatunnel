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

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplit;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplitStrategy;

import com.google.common.collect.Lists;
import com.mongodb.MongoNamespace;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** MongoSplitEnumerator generates {@link MongoSplit} according to partition strategies. */
@Slf4j
public class MongoSplitEnumerator
        implements SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> {

    private final ArrayList<MongoSplit> pendingSplits = Lists.newArrayList();

    private final Context<MongoSplit> context;

    private final MongoClientProvider clientProvider;

    private final MongoSplitStrategy strategy;

    public MongoSplitEnumerator(
            Context<MongoSplit> context,
            MongoClientProvider clientProvider,
            MongoSplitStrategy strategy) {
        this(context, clientProvider, strategy, Collections.emptyList());
    }

    public MongoSplitEnumerator(
            Context<MongoSplit> context,
            MongoClientProvider clientProvider,
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
    public void run() throws Exception {
        log.info("Starting MongoSplitEnumerator.");
        pendingSplits.addAll(strategy.split());
        MongoNamespace namespace = clientProvider.getDefaultCollection().getNamespace();
        log.info(
                "Added {} pending splits for namespace {}.",
                pendingSplits.size(),
                namespace.getFullName());
        assignSplit(context.registeredReaders());
    }

    @Override
    public void close() throws IOException {}

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
        // only add splits if the reader requests
    }

    @Override
    public ArrayList<MongoSplit> snapshotState(long checkpointId) throws Exception {
        return pendingSplits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Do nothing
    }

    private void assignSplit(Collection<Integer> readers) {
        log.debug("Assign pendingSplits to readers {}", readers);
        for (int subtaskId : readers) {
            log.info("Received split request from taskId {}.", subtaskId);
            if (pendingSplits.size() > 0) {
                MongoSplit nextSplit = pendingSplits.remove(0);
                context.assignSplit(subtaskId, nextSplit);
                log.info(
                        "Assigned split {} to subtask {}, remaining splits: {}.",
                        nextSplit.splitId(),
                        subtaskId,
                        pendingSplits.size());
            } else {
                log.info("No more splits can be assign, signal subtask {}.", subtaskId);
                context.signalNoMoreSplits(subtaskId);
            }
        }
    }
}
