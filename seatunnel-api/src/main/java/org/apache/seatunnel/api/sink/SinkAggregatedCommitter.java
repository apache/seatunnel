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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * The committer combine taskManager/Worker Commit message. Then commit it uses {@link
 * SinkAggregatedCommitter#commit(List)}. This class will execute in single thread.
 *
 * <p>See Also {@link SinkCommitter}
 *
 * @param <CommitInfoT> The type of commit message.
 * @param <AggregatedCommitInfoT> The type of commit message after combine.
 */
public interface SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> extends Serializable {

    /** init sink aggregated committer */
    default void init() {};

    /** Re-commit message to third party data receiver, The method need to achieve idempotency. */
    default List<AggregatedCommitInfoT> restoreCommit(
            List<AggregatedCommitInfoT> aggregatedCommitInfo) throws IOException {
        return commit(aggregatedCommitInfo);
    }

    /**
     * Commit message to third party data receiver, The method need to achieve idempotency.
     *
     * @param aggregatedCommitInfo The list of combine commit message.
     * @return The commit message which need retry.
     * @throws IOException throw IOException when commit failed.
     */
    List<AggregatedCommitInfoT> commit(List<AggregatedCommitInfoT> aggregatedCommitInfo)
            throws IOException;

    /**
     * The logic about how to combine commit message.
     *
     * @param commitInfos The list of commit message.
     * @return The commit message after combine.
     */
    AggregatedCommitInfoT combine(List<CommitInfoT> commitInfos);

    /**
     * If {@link #commit(List)} failed, this method will be called (**Only** on Spark engine at
     * now).
     *
     * @param aggregatedCommitInfo The list of combine commit message.
     * @throws Exception throw Exception when abort failed.
     */
    void abort(List<AggregatedCommitInfoT> aggregatedCommitInfo) throws Exception;

    /**
     * Close this resource.
     *
     * @throws IOException throw IOException when close failed.
     */
    void close() throws IOException;
}
