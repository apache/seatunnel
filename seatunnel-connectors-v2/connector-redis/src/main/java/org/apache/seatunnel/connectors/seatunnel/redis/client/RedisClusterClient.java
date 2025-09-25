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

package org.apache.seatunnel.connectors.seatunnel.redis.client;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.connectors.seatunnel.redis.config.JedisWrapper;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisDataType;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;

import org.apache.commons.collections4.CollectionUtils;

import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisClusterClient extends RedisClient {
    private final List<Map.Entry<String, ConnectionPool>> nodes;
    private final JedisWrapper jedisWrapper;

    public RedisClusterClient(RedisParameters redisParameters, Jedis jedis, int redisVersion) {
        super(redisParameters, jedis, redisVersion);

        this.jedisWrapper = (JedisWrapper) jedis;
        this.nodes = new ArrayList<>(jedisWrapper.getClusterNodes().entrySet());
    }

    @Override
    public List<String> batchGetString(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<String> result = new ArrayList<>(keys.size());
        for (String key : keys) {
            result.add(jedis.get(key));
        }
        return result;
    }

    @Override
    public List<List<String>> batchGetList(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<List<String>> result = new ArrayList<>(keys.size());
        for (String key : keys) {
            result.add(jedis.lrange(key, 0, -1));
        }
        return result;
    }

    @Override
    public List<Set<String>> batchGetSet(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<Set<String>> result = new ArrayList<>(keys.size());
        for (String key : keys) {
            result.add(jedis.smembers(key));
        }
        return result;
    }

    @Override
    public List<Map<String, String>> batchGetHash(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<Map<String, String>> result = new ArrayList<>(keys.size());
        for (String key : keys) {
            Map<String, String> map = jedis.hgetAll(key);
            map.put(redisParameters.getKeyFieldName(), key);
            result.add(map);
        }
        return result;
    }

    @Override
    public List<List<String>> batchGetZset(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<List<String>> result = new ArrayList<>(keys.size());
        for (String key : keys) {
            result.add(jedis.zrange(key, 0, -1));
        }
        return result;
    }

    @Override
    public void batchWriteString(
            List<RowKind> rowKinds, List<String> keys, List<String> values, long expireSeconds) {
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            if (rowKinds.get(i) == RowKind.DELETE || rowKinds.get(i) == RowKind.UPDATE_BEFORE) {
                RedisDataType.STRING.del(jedis, keys.get(i), values.get(i));
            } else {
                RedisDataType.STRING.set(jedis, keys.get(i), values.get(i), expireSeconds);
            }
        }
    }

    @Override
    public void batchWriteList(
            List<RowKind> rowKinds, List<String> keys, List<String> values, long expireSeconds) {
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            if (rowKinds.get(i) == RowKind.DELETE || rowKinds.get(i) == RowKind.UPDATE_BEFORE) {
                RedisDataType.LIST.del(jedis, keys.get(i), values.get(i));
            } else {
                RedisDataType.LIST.set(jedis, keys.get(i), values.get(i), expireSeconds);
            }
        }
    }

    @Override
    public void batchWriteSet(
            List<RowKind> rowKinds, List<String> keys, List<String> values, long expireSeconds) {
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            if (rowKinds.get(i) == RowKind.DELETE || rowKinds.get(i) == RowKind.UPDATE_BEFORE) {
                RedisDataType.SET.del(jedis, keys.get(i), values.get(i));
            } else {
                RedisDataType.SET.set(jedis, keys.get(i), values.get(i), expireSeconds);
            }
        }
    }

    @Override
    public void batchWriteHash(
            List<RowKind> rowKinds, List<String> keys, List<String> values, long expireSeconds) {
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            if (rowKinds.get(i) == RowKind.DELETE || rowKinds.get(i) == RowKind.UPDATE_BEFORE) {
                RedisDataType.HASH.del(jedis, keys.get(i), values.get(i));
            } else {
                RedisDataType.HASH.set(jedis, keys.get(i), values.get(i), expireSeconds);
            }
        }
    }

    @Override
    public void batchWriteZset(
            List<RowKind> rowKinds, List<String> keys, List<String> values, long expireSeconds) {
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            if (rowKinds.get(i) == RowKind.DELETE || rowKinds.get(i) == RowKind.UPDATE_BEFORE) {
                RedisDataType.ZSET.del(jedis, keys.get(i), values.get(i));
            } else {
                RedisDataType.ZSET.set(jedis, keys.get(i), values.get(i), expireSeconds);
            }
        }
    }

    /** In cluster mode, traverse and scan each node key */
    @Override
    public ScanResult<String> scanKeyResult(
            final String cursor, final ScanParams params, final RedisDataType type) {
        // Create a composite cursor to traverse the cluster nodes
        // the format is "Node Index:Node cursor"
        int nodeIndex = 0;
        String nodeCursor = cursor;
        boolean isFirstScan = !cursor.contains(":");

        if (!ScanParams.SCAN_POINTER_START.equals(cursor) && cursor.contains(":")) {
            String[] parts = cursor.split(":", 2);
            nodeIndex = Integer.parseInt(parts[0]);
            nodeCursor = parts[1];
        }

        // All nodes have been scanned
        if (nodeIndex >= nodes.size()) {
            return new ScanResult<>(ScanParams.SCAN_POINTER_START, new ArrayList<>());
        }

        List<String> resultKeys;
        String nextCursor;

        Map.Entry<String, ConnectionPool> connectionPoolEntry = nodes.get(nodeIndex);
        Jedis jedis = jedisWrapper.getJedis(connectionPoolEntry.getKey());

        // Perform the scan operation
        ScanResult<String> scanResult;
        if (type != null) {
            // redis 7
            scanResult = jedis.scan(nodeCursor, params, type.name());
        } else {
            // redis 5
            scanResult = jedis.scan(nodeCursor, params);
        }

        resultKeys = new ArrayList<>(scanResult.getResult());

        // Generate the next cursor
        if (!isFirstScan && ScanParams.SCAN_POINTER_START.equals(scanResult.getCursor())) {
            // The current node scan has been completed. Move to the next node
            nodeIndex++;
            if (nodeIndex < nodes.size()) {
                nextCursor = nodeIndex + ":" + ScanParams.SCAN_POINTER_START;
            } else {
                nextCursor = ScanParams.SCAN_POINTER_START;
            }
        } else {
            // The current node has not been fully scanned. Update the composite cursor
            nextCursor = nodeIndex + ":" + scanResult.getCursor();
        }

        return new ScanResult<>(nextCursor, resultKeys);
    }
}
