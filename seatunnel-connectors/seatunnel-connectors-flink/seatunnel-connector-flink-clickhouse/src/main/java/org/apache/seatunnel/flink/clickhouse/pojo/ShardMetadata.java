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

public class ShardMetadata implements Serializable {

    private static final long serialVersionUID = -1L;

    private String shardKey;
    private String shardKeyType;
    private String database;
    private String table;
    private boolean splitMode;
    private Shard defaultShard;

    public ShardMetadata(String shardKey,
                         String shardKeyType,
                         String database,
                         String table,
                         boolean splitMode,
                         Shard defaultShard) {
        this.shardKey = shardKey;
        this.shardKeyType = shardKeyType;
        this.database = database;
        this.table = table;
        this.splitMode = splitMode;
        this.defaultShard = defaultShard;
    }

    public String getShardKey() {
        return shardKey;
    }

    public void setShardKey(String shardKey) {
        this.shardKey = shardKey;
    }

    public String getShardKeyType() {
        return shardKeyType;
    }

    public void setShardKeyType(String shardKeyType) {
        this.shardKeyType = shardKeyType;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public boolean getSplitMode() {
        return splitMode;
    }

    public void setSplitMode(boolean splitMode) {
        this.splitMode = splitMode;
    }

    public Shard getDefaultShard() {
        return defaultShard;
    }

    public void setDefaultShard(Shard defaultShard) {
        this.defaultShard = defaultShard;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ShardMetadata that = (ShardMetadata) o;

        if (splitMode != that.splitMode) {
            return false;
        }
        if (!shardKey.equals(that.shardKey)) {
            return false;
        }
        if (!shardKeyType.equals(that.shardKeyType)) {
            return false;
        }
        if (!database.equals(that.database)) {
            return false;
        }
        if (!table.equals(that.table)) {
            return false;
        }
        return defaultShard.equals(that.defaultShard);
    }

    @Override
    @SuppressWarnings("magicnumber")
    public int hashCode() {
        int result = shardKey.hashCode();
        result = 31 * result + shardKeyType.hashCode();
        result = 31 * result + database.hashCode();
        result = 31 * result + table.hashCode();
        result = 31 * result + (splitMode ? 1 : 0);
        result = 31 * result + defaultShard.hashCode();
        return result;
    }
}
