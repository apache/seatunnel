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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.split.JdbcNumericBetweenParametersProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JdbcSourceSplitEnumerator
        implements SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceSplitEnumerator.class);
    private final SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext;

    private final Map<Integer, List<JdbcSourceSplit>> pendingSplits;

    private final Object stateLock = new Object();
    private volatile boolean shouldEnumerate;

    private JdbcSourceConfig jdbcSourceConfig;
    private final PartitionParameter partitionParameter;

    public JdbcSourceSplitEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext,
            JdbcSourceConfig jdbcSourceConfig,
            PartitionParameter partitionParameter) {
        this(enumeratorContext, jdbcSourceConfig, partitionParameter, null);
    }

    public JdbcSourceSplitEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext,
            JdbcSourceConfig jdbcSourceConfig,
            PartitionParameter partitionParameter,
            JdbcSourceState sourceState) {
        this.enumeratorContext = enumeratorContext;
        this.jdbcSourceConfig = jdbcSourceConfig;
        this.partitionParameter = partitionParameter;
        this.pendingSplits = new HashMap<>();
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplits.putAll(sourceState.getPendingSplits());
        }
    }

    @Override
    public void open() {
        // No connection needs to be opened
    }

    @Override
    public void run() throws Exception {
        Set<Integer> readers = enumeratorContext.registeredReaders();
        if (shouldEnumerate) {
            Set<JdbcSourceSplit> newSplits = discoverySplits();

            synchronized (stateLock) {
                addPendingSplit(newSplits);
                shouldEnumerate = false;
            }

            assignSplit(readers);
        }

        LOG.debug(
                "No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(enumeratorContext::signalNoMoreSplits);
    }

    private Set<JdbcSourceSplit> discoverySplits() {
        Set<JdbcSourceSplit> allSplit = new HashSet<>();
        LOG.info("Starting to calculate splits.");
        if (null != partitionParameter) {
            int partitionNumber =
                    partitionParameter.getPartitionNumber() != null
                            ? partitionParameter.getPartitionNumber()
                            : enumeratorContext.currentParallelism();
            if (partitionParameter.getDataType().equals(BasicType.STRING_TYPE)) {
                for (int i = 0; i < partitionNumber; i++) {
                    allSplit.add(new JdbcSourceSplit(new Object[] {i}, i));
                }
            } else {
                JdbcNumericBetweenParametersProvider jdbcNumericBetweenParametersProvider =
                        new JdbcNumericBetweenParametersProvider(
                                        partitionParameter.getMinValue(),
                                        partitionParameter.getMaxValue())
                                .ofBatchNum(partitionNumber);
                Serializable[][] parameterValues =
                        jdbcNumericBetweenParametersProvider.getParameterValues();
                for (int i = 0; i < parameterValues.length; i++) {
                    allSplit.add(new JdbcSourceSplit(parameterValues[i], i));
                }
            }
        } else {
            allSplit.add(new JdbcSourceSplit(null, 0));
        }
        return allSplit;
    }

    @Override
    public void close() throws IOException {
        // nothing
    }

    @Override
    public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {
        LOG.debug("Add back splits {} to JdbcSourceSplitEnumerator.", splits);
        if (!splits.isEmpty()) {
            addPendingSplit(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    private void addPendingSplit(Collection<JdbcSourceSplit> splits) {
        int readerCount = enumeratorContext.currentParallelism();
        for (JdbcSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            LOG.info("Assigning {} to {} reader.", split, ownerReader);
            pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    private void assignSplit(Collection<Integer> readers) {
        LOG.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<JdbcSourceSplit> assignmentForReader = pendingSplits.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                LOG.info("Assign splits {} to reader {}", assignmentForReader, reader);
                try {
                    enumeratorContext.assignSplit(reader, assignmentForReader);
                } catch (Exception e) {
                    LOG.error(
                            "Failed to assign splits {} to reader {}",
                            assignmentForReader,
                            reader,
                            e);
                    pendingSplits.put(reader, assignmentForReader);
                }
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new JdbcConnectorException(
                CommonErrorCode.UNSUPPORTED_OPERATION,
                String.format("Unsupported handleSplitRequest: %d", subtaskId));
    }

    @Override
    public void registerReader(int subtaskId) {
        LOG.debug("Register reader {} to JdbcSourceSplitEnumerator.", subtaskId);
        if (!pendingSplits.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public JdbcSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new JdbcSourceState(shouldEnumerate, pendingSplits);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
