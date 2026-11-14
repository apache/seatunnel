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

/**
 * How a {@link SupportMultiTableSinkWriter} can be used when {@code multi_table_sink_replica} is
 * greater than 1, that is, when several writers of the same table run in one subtask and rows are
 * spread over them.
 *
 * <p>With a single replica the mode is not used: all rows of a table go to its only writer.
 */
public enum MultiTableSinkReplicaMode {

    /**
     * The writer does not support several replicas. A job that sets {@code
     * multi_table_sink_replica} above 1 for it is rejected before any row is written. This is the
     * default, because rows of the same destination record could otherwise reach different replicas
     * and be applied out of order.
     */
    UNSUPPORTED,

    /**
     * Rows are routed by the field at {@link SupportMultiTableSinkWriter#primaryKey()}, so all
     * changes of one key reach the same replica in order. The writer must return a key index that
     * matches the identity of the record it writes at the destination.
     */
    KEYED,

    /**
     * Rows may be spread over the replicas at random. Only declare this when the destination result
     * does not depend on the order in which the replicas write.
     */
    RANDOM
}
