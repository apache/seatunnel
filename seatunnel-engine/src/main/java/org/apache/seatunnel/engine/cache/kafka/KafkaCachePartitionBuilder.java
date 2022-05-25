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

import org.apache.seatunnel.engine.cache.CacheConfig;
import org.apache.seatunnel.engine.cache.CachePartition;
import org.apache.seatunnel.engine.cache.CacheSinkPartitionSelector;
import org.apache.seatunnel.engine.cache.CacheSourcePartitionSelector;
import org.apache.seatunnel.engine.cache.DataStreamCachePartitionBuilder;
import org.apache.seatunnel.engine.cache.DefaultCacheSinkPartitionSelector;
import org.apache.seatunnel.engine.cache.DefaultCacheSourcePartitionSelector;

public class KafkaCachePartitionBuilder implements DataStreamCachePartitionBuilder {
    @Override
    public CachePartition[] createCachePartitions(CacheConfig cacheConfig, int sourceParallelism) {
        CachePartition[] cachePartitions = new CachePartition[sourceParallelism];
        for (int i = 0; i < sourceParallelism; i++) {
            CachePartition cachePartition = new KafkaPartition();
            cachePartitions[i] = cachePartition;
        }
        return cachePartitions;
    }

    @Override
    public CacheSinkPartitionSelector createCacheSinkPartitionSelector(CacheConfig cacheConfig, CachePartition[] cachePartitions) {
        return new DefaultCacheSinkPartitionSelector(cachePartitions);
    }

    @Override
    public CacheSourcePartitionSelector createCacheSourcePartitionSelector(CacheConfig cacheConfig, CachePartition[] cachePartitions) {
        return new DefaultCacheSourcePartitionSelector(cachePartitions);
    }
}
