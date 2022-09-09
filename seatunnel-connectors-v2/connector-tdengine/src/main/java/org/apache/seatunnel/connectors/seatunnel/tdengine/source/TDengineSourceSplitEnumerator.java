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

package org.apache.seatunnel.connectors.seatunnel.tdengine.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.tdengine.state.TDengineSourceState;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.SneakyThrows;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class TDengineSourceSplitEnumerator implements SourceSplitEnumerator<TDengineSourceSplit, TDengineSourceState> {

    private final SourceSplitEnumerator.Context<TDengineSourceSplit> context;
    private final TDengineSourceConfig config;
    private Set<TDengineSourceSplit> pendingSplit = new HashSet<>();
    private Set<TDengineSourceSplit> assignedSplit = new HashSet<>();
    private Connection conn;

    public TDengineSourceSplitEnumerator(TDengineSourceConfig config, SourceSplitEnumerator.Context<TDengineSourceSplit> context) {
        this.config = config;
        this.context = context;
    }

    public TDengineSourceSplitEnumerator(TDengineSourceConfig config, TDengineSourceState sourceState, SourceSplitEnumerator.Context<TDengineSourceSplit> context) {
        this(config, context);
        this.assignedSplit = sourceState.getAssignedSplit();
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    @SneakyThrows
    @Override
    public void open() {
        String jdbcUrl = StringUtils.join(config.getUrl(), config.getDatabase(), "?user=", config.getUsername(), "&password=", config.getPassword());
        conn = DriverManager.getConnection(jdbcUrl);
    }

    @Override
    public void run() throws SQLException {
        pendingSplit = getAllSplits();
        assignSplit(context.registeredReaders());
    }

    /*
     * 1. get timestampField
     * 2. get all sub tables of configured super table
     * 3. each split has one sub table
     */
    private Set<TDengineSourceSplit> getAllSplits() throws SQLException {
        final String timestampFieldName;
        try (Statement statement = conn.createStatement()) {
            final ResultSet fieldNameResultSet = statement.executeQuery("desc " + config.getDatabase() + "." + config.getStable());
            fieldNameResultSet.next();
            timestampFieldName = fieldNameResultSet.getString(1);
        }

        final Set<TDengineSourceSplit> splits = Sets.newHashSet();
        try (Statement statement = conn.createStatement()) {
            String metaSQL = "select table_name from information_schema.ins_tables where db_name = '" + config.getDatabase() + "' and stable_name='" + config.getStable() + "';";
            ResultSet subTableNameResultSet = statement.executeQuery(metaSQL);
            while (subTableNameResultSet.next()) {
                final String subTableName = subTableNameResultSet.getString(1);
                final TDengineSourceSplit splitBySubTable = createSplitBySubTable(subTableName, timestampFieldName);
                splits.add(splitBySubTable);
            }
        }
        return splits;
    }

    private TDengineSourceSplit createSplitBySubTable(String subTableName, String timestampFieldName) {
        final List<String> fieldsAndTags = Lists.newArrayListWithExpectedSize(config.getFields().size() + config.getTags().size());
        fieldsAndTags.addAll(config.getFields());
        fieldsAndTags.addAll(config.getTags());
        String fieldsAndTagsStr = StringUtils.EMPTY;
        if (CollectionUtils.isNotEmpty(fieldsAndTags)) {
            fieldsAndTagsStr = StringUtils.join(fieldsAndTags, ",");
        }
        String subTableSQL = "select " + fieldsAndTagsStr + " from " + config.getDatabase() + "." + subTableName;
        String start = config.getLowerBound();
        String end = config.getUpperBound();
        if (start != null || end != null) {
            String startCondition = null;
            String endCondition = null;
            //Left closed right away
            if (start != null) {
                startCondition = timestampFieldName + " >= '" + start + "'";
            }
            if (end != null) {
                endCondition = timestampFieldName + " < '" + end + "'";
            }
            String query = StringUtils.join(new String[]{startCondition, endCondition}, " and ");
            subTableSQL = subTableSQL + " where " + query;
        }

        return new TDengineSourceSplit(subTableName, subTableSQL);
    }

    @Override
    public void addSplitsBack(List<TDengineSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplit.addAll(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    private void assignSplit(Collection<Integer> taskIDList) {
        assignedSplit = pendingSplit.stream()
                .map(split -> {
                    int splitOwner = getSplitOwner(split.splitId(), context.currentParallelism());
                    if (taskIDList.contains(splitOwner)) {
                        context.assignSplit(splitOwner, split);
                        return split;
                    } else {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        pendingSplit.clear();
    }

    @Override
    public TDengineSourceState snapshotState(long checkpointId) {
        return new TDengineSourceState(assignedSplit);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        //nothing to do
    }

    @Override
    public void close() throws IOException {
        try {
            if (!Objects.isNull(conn)) {
                conn.close();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        //nothing to do
    }
}
