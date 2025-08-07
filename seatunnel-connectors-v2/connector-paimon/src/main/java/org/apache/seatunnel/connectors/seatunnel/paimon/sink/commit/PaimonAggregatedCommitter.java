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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonHadoopConfiguration;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.security.PaimonSecurityContext;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommit;
import org.apache.paimon.table.sink.TableCommitImpl;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/** Paimon connector aggregated committer class */
@Slf4j
public class PaimonAggregatedCommitter
        implements SinkAggregatedCommitter<PaimonCommitInfo, PaimonAggregatedCommitInfo>,
                SupportMultiTableSinkAggregatedCommitter {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;

    private final String commitUser;

    public PaimonAggregatedCommitter(
            Table table, PaimonHadoopConfiguration paimonHadoopConfiguration, String commitUser) {
        this.table = (FileStoreTable) table;
        this.commitUser = commitUser;
        PaimonSecurityContext.shouldEnableKerberos(paimonHadoopConfiguration);
    }

    @Override
    public List<PaimonAggregatedCommitInfo> commit(
            List<PaimonAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        try (TableCommitImpl tableCommit = table.newCommit(commitUser)) {
            PaimonSecurityContext.runSecured(
                    () -> {
                        log.debug("Trying to commit states streaming mode");
                        Map<Long, List<CommitMessage>> committablesMap =
                                aggregatedCommitInfo.stream()
                                        .flatMap(
                                                paimonAggregatedCommitInfo ->
                                                        paimonAggregatedCommitInfo
                                                                .getCommittablesMap().entrySet()
                                                                .stream())
                                        .collect(
                                                Collectors.toMap(
                                                        Map.Entry::getKey, Map.Entry::getValue));
                        if (!committablesMap.isEmpty()) {
                            committablesMap.forEach(tableCommit::commit);
                        }
                        return null;
                    });
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.TABLE_WRITE_COMMIT_FAILED, e);
        }
        return Collections.emptyList();
    }

    @Override
    public PaimonAggregatedCommitInfo combine(List<PaimonCommitInfo> commitInfos) {
        Map<Long, List<CommitMessage>> committables = new HashMap<>();
        commitInfos.forEach(
                commitInfo ->
                        committables
                                .computeIfAbsent(
                                        commitInfo.getCheckpointId(),
                                        id -> new CopyOnWriteArrayList<>())
                                .addAll(commitInfo.getCommittables()));
        return new PaimonAggregatedCommitInfo(committables);
    }

    @Override
    public void abort(List<PaimonAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {
        try (TableCommit tableCommit = table.newCommit(commitUser)) {
            PaimonSecurityContext.runSecured(
                    () -> {
                        log.debug("Trying to abort states streaming mode");
                        List<CommitMessage> commitMessageList =
                                aggregatedCommitInfo.stream()
                                        .flatMap(
                                                paimonAggregatedCommitInfo ->
                                                        paimonAggregatedCommitInfo
                                                                .getCommittablesMap().values()
                                                                .stream())
                                        .flatMap(List::stream)
                                        .collect(Collectors.toList());
                        if (!commitMessageList.isEmpty()) {
                            tableCommit.abort(commitMessageList);
                        }
                        return null;
                    });
        }
    }

    @Override
    public void close() throws IOException {}
}
