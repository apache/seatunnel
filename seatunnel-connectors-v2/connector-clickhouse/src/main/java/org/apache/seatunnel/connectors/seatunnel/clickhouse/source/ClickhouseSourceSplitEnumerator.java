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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseCatalogConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSourceState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ClickhouseSourceSplitEnumerator
        implements SourceSplitEnumerator<ClickhouseSourceSplit, ClickhouseSourceState> {

    private final Context<ClickhouseSourceSplit> context;
    private final Set<Integer> readers;
    private volatile Map<TablePath, Integer> tablePathAssigned = new HashMap<>();

    private Map<TablePath, ClickhouseCatalogConfig> tableClickhouseCatalogConfigMap;

    // TODO support read distributed engine use multi split
    ClickhouseSourceSplitEnumerator(
            Context<ClickhouseSourceSplit> enumeratorContext,
            Map<TablePath, ClickhouseCatalogConfig> tableClickhouseCatalogConfigMap) {
        this.context = enumeratorContext;
        this.readers = new HashSet<>();
        this.tableClickhouseCatalogConfigMap = tableClickhouseCatalogConfigMap;
        for (TablePath tablePath : tableClickhouseCatalogConfigMap.keySet()) {
            tablePathAssigned.put(tablePath, -1);
        }
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {}

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<ClickhouseSourceSplit> splits, int subtaskId) {
        if (splits.isEmpty()) {
            return;
        }
        for (TablePath tablePath : tableClickhouseCatalogConfigMap.keySet()) {
            if (tablePathAssigned.get(tablePath) == subtaskId) {
                Optional<Integer> otherReader =
                        readers.stream().filter(r -> r != subtaskId).findAny();
                if (otherReader.isPresent()) {
                    context.assignSplit(otherReader.get(), splits);
                } else {
                    tablePathAssigned.put(tablePath, -1);
                }
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return tablePathAssigned.values().stream()
                                .filter(value -> value < 0)
                                .collect(Collectors.toList())
                                .size()
                        > 0
                ? 0
                : 1;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {
        readers.add(subtaskId);
        List<ClickhouseSourceSplit> clickhouseSourceSplits = new ArrayList<>();
        for (TablePath tablePath : tablePathAssigned.keySet()) {
            if (tablePathAssigned.get(tablePath) < 0) {
                tablePathAssigned.put(tablePath, subtaskId);
                ClickhouseSourceSplit clickhouseSourceSplit = new ClickhouseSourceSplit();
                clickhouseSourceSplit.setTablePath(tablePath);
                clickhouseSourceSplits.add(clickhouseSourceSplit);
            }
        }
        context.assignSplit(subtaskId, clickhouseSourceSplits);
    }

    @Override
    public ClickhouseSourceState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
