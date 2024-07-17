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

package org.apache.seatunnel.connectors.seatunnel.timeplus.shard;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class ShardMetadata implements Serializable {

    private static final long serialVersionUID = -1L;

    private String shardKey;
    private String shardKeyType;
    private String sortingKey;
    private String database;
    private String table;
    private String tableEngine;
    private boolean splitMode;
    private Shard defaultShard;
    private String username;
    private String password;

    public ShardMetadata(
            String shardKey,
            String shardKeyType,
            String sortingKey,
            String database,
            String table,
            String tableEngine,
            boolean splitMode,
            Shard defaultShard) {
        this(
                shardKey,
                shardKeyType,
                sortingKey,
                database,
                table,
                tableEngine,
                splitMode,
                defaultShard,
                null,
                null);
    }

    public ShardMetadata(
            String shardKey,
            String shardKeyType,
            String database,
            String table,
            String tableEngine,
            boolean splitMode,
            Shard defaultShard,
            String username,
            String password) {
        this(
                shardKey,
                shardKeyType,
                null,
                database,
                table,
                tableEngine,
                splitMode,
                defaultShard,
                username,
                password);
    }
}
