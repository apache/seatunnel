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

package org.apache.seatunnel.connectors.seatunnel.fts.sink.commit;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.fts.exception.FlinkTableStoreConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fts.exception.FlinkTableStoreConnectorException;

import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.table.SupportsWrite;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.table.store.table.sink.SerializableCommittable;
import org.apache.flink.table.store.table.sink.TableCommit;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class FlinkTableStoreAggregatedCommitter
        implements SinkAggregatedCommitter<
                FlinkTableStoreCommitInfo, FlinkTableStoreAggregatedCommitInfo> {

    private static final long serialVersionUID = 1L;

    private final Lock.Factory localFactory = Lock.emptyFactory();

    private final Table table;

    private final String commitUser;

    public FlinkTableStoreAggregatedCommitter(Table table, String commitUser) {
        this.table = table;
        this.commitUser = commitUser;
    }

    @Override
    public List<FlinkTableStoreAggregatedCommitInfo> commit(
            List<FlinkTableStoreAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        try (TableCommit tableCommit =
                ((SupportsWrite) table).newCommit(commitUser).withLock(localFactory.create())) {
            List<FileCommittable> fileCommittables =
                    aggregatedCommitInfo.stream()
                            .map(FlinkTableStoreAggregatedCommitInfo::getCommittables)
                            .flatMap(List::stream)
                            .flatMap(List::stream)
                            .map(SerializableCommittable::delegate)
                            .collect(Collectors.toList());
            // TODO: use checkpoint id
            tableCommit.commit(0L, fileCommittables);
        } catch (Exception e) {
            throw new FlinkTableStoreConnectorException(
                    FlinkTableStoreConnectorErrorCode.TABLE_WRITE_COMMIT_FAILED,
                    "Flink table store commit operation failed",
                    e);
        }
        return Collections.emptyList();
    }

    @Override
    public FlinkTableStoreAggregatedCommitInfo combine(
            List<FlinkTableStoreCommitInfo> commitInfos) {
        List<List<SerializableCommittable>> committables = new ArrayList<>();
        commitInfos.forEach(commitInfo -> committables.add(commitInfo.getCommittables()));
        return new FlinkTableStoreAggregatedCommitInfo(committables);
    }

    @Override
    public void abort(List<FlinkTableStoreAggregatedCommitInfo> aggregatedCommitInfo)
            throws Exception {
        // TODO find the right way to abort
    }

    @Override
    public void close() throws IOException {}
}
