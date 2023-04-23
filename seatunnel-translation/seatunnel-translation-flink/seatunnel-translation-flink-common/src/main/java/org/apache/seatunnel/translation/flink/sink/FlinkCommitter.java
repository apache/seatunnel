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

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.Sink;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The committer wrapper of {@link SinkCommitter}, which is created by {@link
 * Sink#createCommitter()}, used to unify the different sink committer implementations
 *
 * @param <CommT> The generic type of commit message
 */
@Slf4j
public class FlinkCommitter<CommT> implements Committer<CommitWrapper<CommT>> {

    private final SinkCommitter<CommT> sinkCommitter;

    FlinkCommitter(SinkCommitter<CommT> sinkCommitter) {
        this.sinkCommitter = sinkCommitter;
    }

    @Override
    public List<CommitWrapper<CommT>> commit(List<CommitWrapper<CommT>> committables)
            throws IOException {
        List<CommT> reCommittable =
                sinkCommitter.commit(
                        committables.stream()
                                .map(CommitWrapper::getCommit)
                                .collect(Collectors.toList()));
        if (reCommittable != null && !reCommittable.isEmpty()) {
            log.warn("this version not support re-commit when use flink engine");
        }
        // TODO re-commit the data
        return new ArrayList<>();
    }

    @Override
    public void close() throws Exception {}
}
