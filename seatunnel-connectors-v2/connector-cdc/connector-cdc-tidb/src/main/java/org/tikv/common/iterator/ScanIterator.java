/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.common.iterator;

import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.exception.TiClientInternalException;
import org.tikv.common.key.Key;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** Fixed https://github.com/tikv/client-java/issues/600. */
public abstract class ScanIterator implements Iterator<Kvrpcpb.KvPair> {
    protected final TiConfiguration conf;
    protected final RegionStoreClientBuilder builder;
    protected List<Kvrpcpb.KvPair> currentCache;
    protected ByteString startKey;
    protected int index = -1;
    protected int limit;
    protected boolean keyOnly;
    protected boolean endOfScan = false;

    protected Key endKey;
    protected boolean hasEndKey;
    protected boolean processingLastBatch = false;

    ScanIterator(
            TiConfiguration conf,
            RegionStoreClientBuilder builder,
            ByteString startKey,
            ByteString endKey,
            int limit,
            boolean keyOnly) {
        this.startKey = requireNonNull(startKey, "start key is null");
        this.endKey = Key.toRawKey(requireNonNull(endKey, "end key is null"));
        this.hasEndKey = !endKey.isEmpty();
        this.limit = limit;
        this.keyOnly = keyOnly;
        this.conf = conf;
        this.builder = builder;
    }

    /**
     * Load current region to cache, returns the region if loaded.
     *
     * @return TiRegion of current data loaded to cache
     * @throws GrpcException if scan still fails after backoff
     *     <p>TODO : Add test to check it correctness
     */
    abstract TiRegion loadCurrentRegionToCache() throws GrpcException;

    // return true if current cache is not loaded or empty
    boolean cacheLoadFails() {
        if (endOfScan || processingLastBatch) {
            return true;
        }
        if (startKey == null) {
            return true;
        }
        try {
            TiRegion region = loadCurrentRegionToCache();
            ByteString curRegionEndKey = region.getEndKey();
            // currentCache is null means no keys found, whereas currentCache is empty means no
            // values
            // found. The difference lies in whether to continue scanning, because chances are that
            // an empty region exists due to deletion, region split, e.t.c.
            // See https://github.com/pingcap/tispark/issues/393 for details
            if (currentCache == null) {
                return true;
            }
            index = 0;
            Key lastKey = Key.EMPTY;
            // Session should be single-threaded itself
            // so that we don't worry about conf change in the middle
            // of a transaction. Otherwise, below code might lose data
            int scanLimit = Math.min(limit, conf.getScanBatchSize());
            if (currentCache.size() < scanLimit) {
                startKey = curRegionEndKey;
                lastKey = Key.toRawKey(curRegionEndKey);
            } else if (currentCache.size() > scanLimit) {
                throw new IndexOutOfBoundsException(
                        "current cache size = "
                                + currentCache.size()
                                + ", larger than "
                                + scanLimit);
            } else {
                // Start new scan from exact next key in current region
                lastKey = Key.toRawKey(currentCache.get(currentCache.size() - 1).getKey());
                startKey = lastKey.next().toByteString();
            }
            // notify last batch if lastKey is greater than or equal to endKey
            // if startKey is empty, it indicates +∞
            if (hasEndKey && lastKey.compareTo(endKey) >= 0 || startKey.isEmpty()) {
                processingLastBatch = true;
                startKey = null;
            }
        } catch (Exception e) {
            throw new TiClientInternalException("Error scanning data from region.", e);
        }
        return false;
    }
}
