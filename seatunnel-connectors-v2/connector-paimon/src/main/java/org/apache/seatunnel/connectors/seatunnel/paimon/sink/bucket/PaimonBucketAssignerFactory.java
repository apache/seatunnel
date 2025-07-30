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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket;

import org.apache.seatunnel.api.table.catalog.TablePath;

import org.apache.paimon.table.Table;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PaimonBucketAssignerFactory implements Serializable {

    private static final long serialVersionUID = 1L;
    private final ConcurrentHashMap<TablePath, Map<Integer, PaimonBucketAssigner>>
            bucketAssignerMap = new ConcurrentHashMap<>();

    public PaimonBucketAssignerFactory() {}

    public void init(final TablePath tableId, final Table table, final int numAssigners) {
        bucketAssignerMap.computeIfAbsent(
                tableId,
                t -> {
                    Map<Integer, PaimonBucketAssigner> map = new ConcurrentHashMap<>();
                    for (int i = 0; i < numAssigners; i++) {
                        map.put(i, new PaimonBucketAssigner(table, numAssigners, i));
                    }
                    return map;
                });
    }

    public PaimonBucketAssigner getBucketAssigner(final TablePath tableId, final int assignId) {
        return bucketAssignerMap.get(tableId).get(assignId);
    }

    public void clear(final TablePath tableId, final int assignId) {
        if (bucketAssignerMap.containsKey(tableId)) {
            Map<Integer, PaimonBucketAssigner> paimonBucketAssignerMap =
                    bucketAssignerMap.get(tableId);
            boolean isRunning =
                    paimonBucketAssignerMap.values().stream()
                            .anyMatch(PaimonBucketAssigner::isRunning);
            if (!isRunning) {
                bucketAssignerMap.remove(tableId);
            } else {
                paimonBucketAssignerMap.get(assignId).finish();
            }
        }
    }
}
