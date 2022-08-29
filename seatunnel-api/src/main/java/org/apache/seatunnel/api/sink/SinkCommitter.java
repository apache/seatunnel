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
 * The committer to commit message. We strongly recommend implementing {@link SinkAggregatedCommitter} first,
 * as the current version of {@link SinkAggregatedCommitter} can provide more consistent behavior.
 * <p>
 * See Also {@link SinkAggregatedCommitter}
 *
 * @param <CommitInfoT> The type of commit message.
 */
public interface SinkCommitter<CommitInfoT> extends Serializable {

    /**
     * Commit message to third party data receiver, The method need to achieve idempotency.
     *
     * @param commitInfos The list of commit message
     * @return The commit message need retry.
     * @throws IOException throw IOException when commit failed.
     */
    List<CommitInfoT> commit(List<CommitInfoT> commitInfos) throws IOException;

    /**
     * Abort the transaction, this method will be called (**Only** on Spark engine) when the commit is failed.
     *
     * @param commitInfos The list of commit message, used to abort the commit.
     * @throws IOException throw IOException when close failed.
     */
    void abort(List<CommitInfoT> commitInfos) throws IOException;
}
