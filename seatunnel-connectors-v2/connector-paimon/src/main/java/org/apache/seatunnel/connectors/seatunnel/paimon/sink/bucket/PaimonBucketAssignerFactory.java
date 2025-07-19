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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PaimonBucketAssignerFactory {
    private static final ConcurrentHashMap<TablePath, Map<Integer, PaimonBucketAssigner>>
            BUCKET_ASSIGNER_MAP = new ConcurrentHashMap<>();

    public static PaimonBucketAssigner getBucketAssigner(
            final TablePath tableId,
            final Table table,
            final int numAssigners,
            final int assignId) {
        return BUCKET_ASSIGNER_MAP
                .computeIfAbsent(
                        tableId,
                        t -> {
                            Map<Integer, PaimonBucketAssigner> map = new ConcurrentHashMap<>();
                            for (int i = 0; i < numAssigners; i++) {
                                map.put(i, new PaimonBucketAssigner(table, numAssigners, i));
                            }
                            return map;
                        })
                .get(assignId);
    }
}
