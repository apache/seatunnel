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

package org.apache.seatunnel.flink.clickhouse.pojo;

import java.io.Serializable;

public class Shard implements Serializable {
    private static final long serialVersionUID = -1L;

    private final int shardNum;
    private final int shardWeight;
    private final int replicaNum;
    private final String hostname;
    private final String hostAddress;
    private final String port;
    private final String database;

    // cache the hash code
    private int hashCode = -1;

    public Shard(int shardNum,
                 int shardWeight,
                 int replicaNum,
                 String hostname,
                 String hostAddress,
                 String port,
                 String database) {
        this.shardNum = shardNum;
        this.shardWeight = shardWeight;
        this.replicaNum = replicaNum;
        this.hostname = hostname;
        this.hostAddress = hostAddress;
        this.port = port;
        this.database = database;
    }

    public int getShardNum() {
        return shardNum;
    }

    public int getShardWeight() {
        return shardWeight;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public String getHostname() {
        return hostname;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public String getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getJdbcUrl() {
        return "jdbc:clickhouse://" + hostAddress + ":" + port + "/" + database;
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

        if (shardNum != shard.shardNum) {
            return false;
        }
        if (shardWeight != shard.shardWeight) {
            return false;
        }
        if (replicaNum != shard.replicaNum) {
            return false;
        }
        if (!hostname.equals(shard.hostname)) {
            return false;
        }
        if (!hostAddress.equals(shard.hostAddress)) {
            return false;
        }
        if (!port.equals(shard.port)) {
            return false;
        }
        return database.equals(shard.database);
    }

    @Override
    public int hashCode() {
        if (hashCode != -1) {
            return hashCode;
        }
        int result = shardNum;
        result = 31 * result + shardWeight;
        result = 31 * result + replicaNum;
        result = 31 * result + hostname.hashCode();
        result = 31 * result + hostAddress.hashCode();
        result = 31 * result + port.hashCode();
        result = 31 * result + database.hashCode();
        hashCode = result;
        return hashCode;
    }
}
