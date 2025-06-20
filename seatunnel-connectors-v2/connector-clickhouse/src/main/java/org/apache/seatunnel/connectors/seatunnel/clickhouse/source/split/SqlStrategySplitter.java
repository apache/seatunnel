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

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class SqlStrategySplitter implements Splitter, AutoCloseable, Serializable {
    private static final long serialVersionUID = -6512116577805882794L;

    public List<ClickhouseSourceSplit> generateSplits(ClickhouseSourceTable clickhouseSourceTable) {
        log.info(
                "start sql strategy splitter generate splits. table: {}",
                clickhouseSourceTable.getTablePath());

        List<ClickhouseSourceSplit> splits = new ArrayList<>();
        ClickhouseTable clickhouseTable = clickhouseSourceTable.getClickhouseTable();

        String querySql = rewriteQueryForLocalTable(clickhouseSourceTable, clickhouseTable);

        // parallelism reading based on input sql, creating splits for each shard
        clickhouseSourceTable
                .getClusterShardList()
                .forEach(
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

            return clickhouseSourceTable
                    .getOriginQuery()
                    .replace(clickhouseTable.getTableIdentifier(), localTableId);
        }

        return clickhouseSourceTable.getOriginQuery();
    }

    @Override
    public void close() {}
}
