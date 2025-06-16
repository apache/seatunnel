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
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseTable;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.ClickhousePart;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.ClickhouseSourceTable;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class TablePartSplitter implements AutoCloseable, Serializable {

    private static final long serialVersionUID = 1284356772463422708L;

    public List<ClickhouseSourceSplit> generateSplits(ClickhouseSourceTable clickhouseSourceTable) {
        log.info("start generate splits. table: {}", clickhouseSourceTable.getTablePath());

        ClickhouseTable clickhouseTable = clickhouseSourceTable.getClickhouseTable();
        Map<Shard, List<ClickhousePart>> shardToParts = new HashMap<>();

        clickhouseSourceTable
                .getClusterShardList()
                .forEach(
                        shard -> {
                            try (ClickhouseProxy proxy = new ClickhouseProxy(shard.getNode())) {
                                List<ClickhousePart> partList =
                                        proxy.getPartList(
                                                clickhouseTable.getLocalDatabase(),
                                                clickhouseTable.getLocalTableName(),
                                                shard,
                                                clickhouseSourceTable.getPartitionList());

                                shardToParts.put(shard, partList);
                            }
                        });

        // generate splits
        return partMapToSplits(clickhouseSourceTable, shardToParts);
    }

    public List<ClickhouseSourceSplit> partMapToSplits(
            ClickhouseSourceTable clickhouseSourceTable,
            Map<Shard, List<ClickhousePart>> shardToParts) {

        int partSplitSize = partCountLimitForOneSplit(clickhouseSourceTable);
        List<ClickhouseSourceSplit> splits = new ArrayList<>();
        ClickhouseTable clickhouseTable = clickhouseSourceTable.getClickhouseTable();

        // generate splits
        for (Map.Entry<Shard, List<ClickhousePart>> shardPartsEntry : shardToParts.entrySet()) {
            log.debug("generate splits with shard part: {}", shardPartsEntry);
            HashSet<ClickhousePart> partSet = new HashSet<>(shardPartsEntry.getValue());
            shardPartsEntry.getValue().clear();
            shardPartsEntry.getValue().addAll(partSet);

            int fromIndex = 0;
            while (fromIndex < shardPartsEntry.getValue().size()) {
                Set<ClickhousePart> partSplit =
                        new HashSet<>(
                                shardPartsEntry
                                        .getValue()
                                        .subList(
                                                fromIndex,
                                                Math.min(
                                                        fromIndex + partSplitSize,
                                                        shardPartsEntry.getValue().size())));

                log.debug("partSplit size: {}", partSplit.size());
                fromIndex += partSplitSize;

                String splitId =
                        String.valueOf(
                                createSplitId(
                                        clickhouseSourceTable.getTablePath(),
                                        shardPartsEntry.getKey(),
                                        partSplit));
                ClickhouseSourceSplit clickhouseSourceSplit =
                        new ClickhouseSourceSplit(
                                TablePath.of(
                                        clickhouseTable.getLocalDatabase(),
                                        clickhouseTable.getLocalTableName()),
                                TablePath.of(
                                        clickhouseTable.getDatabase(),
                                        clickhouseTable.getTableName()),
                                partSplit,
                                shardPartsEntry.getKey(),
                                splitId);
                log.debug("generate one split: {}", clickhouseSourceSplit);
                splits.add(clickhouseSourceSplit);
            }
        }
        log.info("generate splits size: {}", splits.size());
        return splits;
    }

    public int partCountLimitForOneSplit(ClickhouseSourceTable clickhouseSourceTable) {
        int partSize = ClickhouseSourceOptions.CLICKHOUSE_PART_SIZE_DEFAULT;
        if (clickhouseSourceTable.getPartSize() != null) {
            partSize = clickhouseSourceTable.getPartSize();
        }

        if (partSize < ClickhouseSourceOptions.CLICKHOUSE_PART_SIZE_MIN) {
            log.warn(
                    "part size {} is less than {}, set to default value {}",
                    partSize,
                    ClickhouseSourceOptions.CLICKHOUSE_PART_SIZE_MIN,
                    ClickhouseSourceOptions.CLICKHOUSE_PART_SIZE_DEFAULT);
            partSize = ClickhouseSourceOptions.CLICKHOUSE_PART_SIZE_MIN;
        }
        log.debug("part size is set to {}", partSize);

        return partSize;
    }

    public int createSplitId(TablePath tablePath, Shard shard, Set<ClickhousePart> parts) {

        int result = tablePath.hashCode();
        result = 31 * result * shard.hashCode();
        result = 31 * result * parts.hashCode();

        return result;
    }

    @Override
    public void close() {}
}
