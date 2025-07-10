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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source.split;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseTable;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.ClickhouseSourceTable;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class SqlStrategySplitter implements Splitter, AutoCloseable, Serializable {
    private static final long serialVersionUID = -6512116577805882794L;

    public List<ClickhouseSourceSplit> generateSplits(
            ClickhouseSourceTable clickhouseSourceTable, List<Shard> clusterShardList) {
        log.info(
                "start sql strategy splitter generate splits. table: {}",
                clickhouseSourceTable.getTablePath());

        if (clickhouseSourceTable.isComplexSql()) {
            log.info("Complex SQL detected, creating a single split for the query.");
            return createSingleSplit(clickhouseSourceTable, clusterShardList);
        }

        List<ClickhouseSourceSplit> splits = new ArrayList<>();
        ClickhouseTable clickhouseTable = clickhouseSourceTable.getClickhouseTable();

        String querySql = rewriteQueryForLocalTable(clickhouseSourceTable, clickhouseTable);

        // parallelism reading based on input sql, creating splits for each shard
        clusterShardList.forEach(
                shard ->
                        splits.add(
                                new ClickhouseSourceSplit(
                                        TablePath.of(
                                                clickhouseTable.getLocalDatabase(),
                                                clickhouseTable.getLocalTableName()),
                                        TablePath.of(
                                                clickhouseTable.getDatabase(),
                                                clickhouseTable.getTableName()),
                                        new ArrayList<>(),
                                        shard,
                                        querySql,
                                        0,
                                        createSplitId(
                                                clickhouseSourceTable.getTablePath(),
                                                shard,
                                                splits.size()))));

        log.info("generate splits size: {}", splits.size());
        return splits;
    }

    @Override
    public String createSplitId(TablePath tablePath, Shard shard, int index) {
        return String.format("%s-%s-%s", tablePath, shard.hashCode(), index);
    }

    private String rewriteQueryForLocalTable(
            ClickhouseSourceTable clickhouseSourceTable, ClickhouseTable clickhouseTable) {
        if (clickhouseTable.getDistributedEngine() != null) {
            String localTableId = clickhouseTable.getLocalTableIdentifier();

            String querySql = clickhouseSourceTable.getOriginQuery();
            return querySql.replace(
                    ClickhouseUtil.extractTablePathFromSql(querySql).getFullName(), localTableId);
        }

        return clickhouseSourceTable.getOriginQuery();
    }

    private List<ClickhouseSourceSplit> createSingleSplit(
            ClickhouseSourceTable clickhouseSourceTable, List<Shard> clusterShardList) {
        return Collections.singletonList(
                new ClickhouseSourceSplit(
                        clickhouseSourceTable.getTablePath(),
                        clickhouseSourceTable.getTablePath(),
                        new ArrayList<>(),
                        clusterShardList.get(0),
                        clickhouseSourceTable.getOriginQuery(),
                        0,
                        createSplitId(
                                clickhouseSourceTable.getTablePath(), clusterShardList.get(0), 0)));
    }

    @Override
    public void close() {}
}
