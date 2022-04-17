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

package org.apache.seatunnel.flink.clickhouse.sink.client;

import org.apache.seatunnel.flink.clickhouse.pojo.DistributedEngine;
import org.apache.seatunnel.flink.clickhouse.pojo.Shard;
import org.apache.seatunnel.flink.clickhouse.pojo.ShardMetadata;

import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

public class ShardRouter implements Serializable {

    private static final long serialVersionUID = -1L;

    private String shardTable;
    private final String table;
    private int shardWeightCount;
    private final TreeMap<Integer, Shard> shards;
    private final String shardKey;
    private final String shardKeyType;
    private final boolean splitMode;

    private final XXHash64 hashInstance = XXHashFactory.fastestInstance().hash64();
    private final ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();

    public ShardRouter(ClickhouseClient clickhouseClient, ShardMetadata shardMetadata) {
        this.shards = new TreeMap<>();
        this.shardKey = shardMetadata.getShardKey();
        this.shardKeyType = shardMetadata.getShardKeyType();
        this.splitMode = shardMetadata.getSplitMode();
        this.table = shardMetadata.getTable();
        if (StringUtils.isNotEmpty(shardKey) && StringUtils.isEmpty(shardKeyType)) {
            throw new IllegalArgumentException("Shard key " + shardKey + " not found in table " + table);
        }

        try (
            ClickHouseConnection connection = clickhouseClient.getClickhouseConnection()) {
            if (splitMode) {
                DistributedEngine localTable = clickhouseClient.getClickhouseDistributedTable(connection, shardMetadata.getDatabase(), table);
                this.shardTable = localTable.getTable();
                List<Shard> shardList = clickhouseClient.getClusterShardList(connection, localTable.getClusterName(), localTable.getDatabase(), shardMetadata.getDefaultShard().getPort());
                int weight = 0;
                for (Shard shard : shardList) {
                    shards.put(weight, shard);
                    weight += shard.getShardWeight();
                }
                shardWeightCount = weight;
            } else {
                shards.put(0, shardMetadata.getDefaultShard());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public String getShardTable() {
        return splitMode ? shardTable : table;
    }

    public Shard getShard(Row row) {
        if (!splitMode) {
            return shards.firstEntry().getValue();
        }
        if (StringUtils.isEmpty(shardKey) || row.getField(shardKey) == null) {
            return shards.lowerEntry(threadLocalRandom.nextInt(shardWeightCount + 1)).getValue();
        }
        int offset = (int) (hashInstance.hash(ByteBuffer.wrap(row.getField(shardKey).toString().getBytes(StandardCharsets.UTF_8)), 0) & Long.MAX_VALUE % shardWeightCount);
        return shards.lowerEntry(offset + 1).getValue();
    }

    public TreeMap<Integer, Shard> getShards() {
        return shards;
    }
}
