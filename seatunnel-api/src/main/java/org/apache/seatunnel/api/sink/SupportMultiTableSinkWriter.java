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

package org.apache.seatunnel.api.sink;

import java.util.Optional;

/** The Sink Connector Writer which support multi table should implement this interface */
public interface SupportMultiTableSinkWriter<T> extends SupportResourceShare<T> {

    /**
     * The primary key index of the table in SeaTunnelRow, use it to make sure the same key value
     * will be written to the same sink writer. It is used for routing only when {@link
     * #replicaMode()} returns {@link MultiTableSinkReplicaMode#KEYED}.
     */
    default Optional<Integer> primaryKey() {
        return Optional.empty();
    }

    /**
     * Declares whether this writer can run with {@code multi_table_sink_replica} above 1, and how
     * rows are routed between its replicas. A key from {@link #primaryKey()} alone does not enable
     * several replicas; the writer must also return {@link MultiTableSinkReplicaMode#KEYED} here.
     *
     * <p>All replicas of one table must return the same mode and, for {@code KEYED}, the same key
     * index. The value is read once when the multi-table writer is created or restored.
     *
     * @return the replica mode, {@link MultiTableSinkReplicaMode#UNSUPPORTED} by default
     */
    default MultiTableSinkReplicaMode replicaMode() {
        return MultiTableSinkReplicaMode.UNSUPPORTED;
    }
}
