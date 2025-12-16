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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.sink.SinkCommitter;

import org.apache.flink.api.connector.sink2.Committer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The committer wrapper of {@link SinkCommitter}, which is created by {@link
 * org.apache.flink.api.connector.sink2.SupportsCommitter#createCommitter()}, used to unify the
 * different sink committer implementations
 *
 * @param <CommT> The generic type of commit message
 */
@Slf4j
public class FlinkCommitter<CommT> implements Committer<CommitWrapper<CommT>> {

    private final SinkCommitter<CommT> sinkCommitter;

    public FlinkCommitter(SinkCommitter<CommT> sinkCommitter) {
        this.sinkCommitter = sinkCommitter;
    }

    @Override
    public void commit(Collection<Committer.CommitRequest<CommitWrapper<CommT>>> committables)
            throws IOException, InterruptedException {
        if (committables == null || committables.isEmpty()) {
            return;
        }

        // Extract commit info from CommitRequest wrappers
        List<CommT> commitInfos =
                committables.stream()
                        .map(request -> request.getCommittable().getCommit())
                        .collect(Collectors.toList());

        try {
            // Call SeaTunnel's commit method
            List<CommT> reCommittable = sinkCommitter.commit(commitInfos);

            if (reCommittable != null && !reCommittable.isEmpty()) {
                log.warn(
                        "SeaTunnel committer returned {} items for re-commit, but Flink 1.20 sink2 API doesn't support re-commit. These will be ignored.",
                        reCommittable.size());
                // In Flink 1.20 sink2 API, we can't return failed commits for retry
                // We mark them as failed with known reason
                for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                    if (reCommittable.contains(request.getCommittable().getCommit())) {
                        request.signalFailedWithKnownReason(
                                new IOException(
                                        "Commit failed and re-commit is not supported in Flink 1.20"));
                    } else {
                        request.signalAlreadyCommitted();
                    }
                }
            } else {
                // All commits succeeded, mark them as committed
                for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                    request.signalAlreadyCommitted();
                }
            }
        } catch (Exception e) {
            log.error("Error during commit operation", e);
            // Mark all requests as failed
            for (Committer.CommitRequest<CommitWrapper<CommT>> request : committables) {
                request.signalFailedWithKnownReason(e);
            }
            throw new IOException("Failed to commit data", e);
        }
    }

    @Override
    public void close() throws Exception {}
}
