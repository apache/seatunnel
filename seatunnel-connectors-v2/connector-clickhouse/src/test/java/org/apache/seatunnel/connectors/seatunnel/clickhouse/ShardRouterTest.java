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

package org.apache.seatunnel.connectors.seatunnel.clickhouse;

import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.ShardMetadata;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.ShardRouter;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.DistributedEngine;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

public class ShardRouterTest {

    @Test
    public void testWithShardRouterGetShardRight() {
        String clusterName = "default";
        String database = "test_db";
        String localTable = "test_table_local";
        String localTableEngine = "ReplicatedMergeTree";
        String localTableDDL =
                "create table test_db.test_table_local (token String) ENGINE = ReplicatedMergeTree()";
        String username = "test";
        String password = "123456";

        // Assuming there are 28 clickhouse nodes with 2 replica
        List<Shard> shardList = new ArrayList<>();
        Set<Integer> expected = new TreeSet<>();
        for (int i = 1; i <= 14; i++) {
            expected.add(i);
            Shard shard =
                    new Shard(
                            i,
                            1,
                            1,
                            "shard" + i,
                            "shard" + i,
                            9000,
                            database,
                            username,
                            password,
                            Collections.emptyMap());
            shardList.add(shard);
        }

        DistributedEngine distributedEngine =
                new DistributedEngine(
                        clusterName, database, localTable, localTableEngine, localTableDDL);
        ClickhouseProxy proxy = Mockito.mock(ClickhouseProxy.class);
        Mockito.when(proxy.getClickhouseConnection(Mockito.any(Shard.class))).thenReturn(null);
        Mockito.when(
                        proxy.getClickhouseDistributedTable(
                                Mockito.eq(null), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(distributedEngine);
        Mockito.when(
                        proxy.getClusterShardList(
                                Mockito.eq(null),
                                Mockito.eq("default"),
                                Mockito.eq("test_db"),
                                Mockito.eq(9000),
                                Mockito.eq(null),
                                Mockito.eq(null),
                                Mockito.eq(Collections.emptyMap())))
                .thenReturn(shardList);

        String shardKey = "token";
        String shardKeyType = "String";
        ShardMetadata shardMetadata =
                new ShardMetadata(
                        shardKey,
                        shardKeyType,
                        shardKey,
                        database,
                        localTable,
                        localTableEngine,
                        true,
                        shardList.get(0));

        Set<Integer> actual = new TreeSet<>();
        ShardRouter shardRouter = new ShardRouter(proxy, shardMetadata);
        for (int i = 0; i < 10000000; i++) {
            byte[] randomBytes = new byte[16];
            ThreadLocalRandom.current().nextBytes(randomBytes);
            Shard shard = shardRouter.getShard(Arrays.toString(randomBytes));
            int shardNum = shard.getShardNum();
            actual.add(shardNum);
        }

        Assertions.assertEquals(expected, actual);
    }
}
