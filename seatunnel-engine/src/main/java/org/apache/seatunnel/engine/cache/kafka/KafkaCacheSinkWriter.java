/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.cache.kafka;

import org.apache.seatunnel.engine.api.sink.SinkWriter;
import org.apache.seatunnel.engine.api.type.Row;
import org.apache.seatunnel.engine.cache.CacheConfig;
import org.apache.seatunnel.engine.cache.CachePartition;
import org.apache.seatunnel.engine.cache.CacheSinkPartitionSelector;

import java.io.IOException;

public class KafkaCacheSinkWriter implements SinkWriter {
    private CacheSinkPartitionSelector cacheSinkPartitionSelector;
    private CacheConfig cacheConfig;
    private int subTaskIndex;
    private int totalSubTaskNum;

    public KafkaCacheSinkWriter(CacheSinkPartitionSelector cacheSinkPartitionSelector, CacheConfig cacheConfig, int subTaskIndex, int totalSubTaskNum) {
        this.cacheConfig = cacheConfig;
        this.cacheSinkPartitionSelector = cacheSinkPartitionSelector;
        this.subTaskIndex = subTaskIndex;
        this.totalSubTaskNum = totalSubTaskNum;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void write(Row element) throws IOException {
        CachePartition cachePartition = cacheSinkPartitionSelector.selectPartition(subTaskIndex, totalSubTaskNum, element);
        // TODO write to cachePartition
    }

    @Override
    public byte[] snapshotState(int checkpointId) {
        return new byte[0];
    }

    @Override
    public void notifyCheckpointComplete(int checkpointId) {

    }
}
