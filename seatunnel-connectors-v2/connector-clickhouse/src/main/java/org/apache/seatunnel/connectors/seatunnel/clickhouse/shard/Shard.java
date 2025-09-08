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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.shard;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class Shard implements Serializable {
    private static final long serialVersionUID = -1L;

    private final int shardNum;
    private final int replicaNum;

    private final ClickHouseNode node;

    // cache the hash code
    private int hashCode = -1;

    public Shard(
            int shardNum,
            int shardWeight,
            int replicaNum,
            String hostname,
            String hostAddress,
            int port,
            String database,
            String username,
            String password,
            Map<String, String> options) {
        this.shardNum = shardNum;
        this.replicaNum = replicaNum;
        this.node =
                ClickHouseNode.builder()
                        .host(hostname)
                        .port(ClickHouseProtocol.HTTP, port)
                        .database(database)
                        .weight(shardWeight)
                        .credentials(ClickHouseCredentials.fromUserAndPassword(username, password))
                        .options(options)
                        .build();
    }

    public Shard(int shardNum, int replicaNum, ClickHouseNode node) {
        this.shardNum = shardNum;
        this.replicaNum = replicaNum;
        this.node = node;
    }

    public int getShardNum() {
        return shardNum;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public ClickHouseNode getNode() {
        return node;
    }

    public String getJdbcUrl() {
        return "jdbc:clickhouse://"
                + node.getAddress().getHostName()
                + ":"
                + node.getAddress().getPort()
                + "/"
                + node.getDatabase().get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Shard shard = (Shard) o;
        return shardNum == shard.shardNum
                && replicaNum == shard.replicaNum
                && hashCode == shard.hashCode
                && Objects.equals(node, shard.node);
    }

    @Override
    public int hashCode() {
        if (hashCode == -1) {
            hashCode = Objects.hash(shardNum, replicaNum, node, hashCode);
        }
        return hashCode;
    }
}
