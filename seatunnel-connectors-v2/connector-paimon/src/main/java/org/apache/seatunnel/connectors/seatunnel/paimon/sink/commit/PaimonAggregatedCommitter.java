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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonHadoopConfiguration;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.security.PaimonSecurityContext;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.JobContextUtil;

import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.TableCommit;
import org.apache.paimon.table.sink.WriteBuilder;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Paimon connector aggregated committer class */
@Slf4j
public class PaimonAggregatedCommitter
        implements SinkAggregatedCommitter<PaimonCommitInfo, PaimonAggregatedCommitInfo>,
                SupportMultiTableSinkAggregatedCommitter {

    private static final long serialVersionUID = 1L;

    private final WriteBuilder tableWriteBuilder;

    private final JobContext jobContext;

    public PaimonAggregatedCommitter(
            Table table,
            JobContext jobContext,
            PaimonHadoopConfiguration paimonHadoopConfiguration) {
        this.jobContext = jobContext;
        this.tableWriteBuilder =
                JobContextUtil.isBatchJob(jobContext)
                        ? table.newBatchWriteBuilder()
                        : table.newStreamWriteBuilder();
        PaimonSecurityContext.shouldEnableKerberos(paimonHadoopConfiguration);
    }

    @Override
    public List<PaimonAggregatedCommitInfo> commit(
            List<PaimonAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        try (TableCommit tableCommit = tableWriteBuilder.newCommit()) {
            List<CommitMessage> fileCommittables =
                    aggregatedCommitInfo.stream()
                            .map(PaimonAggregatedCommitInfo::getCommittables)
                            .flatMap(List::stream)
                            .flatMap(List::stream)
                            .collect(Collectors.toList());
            PaimonSecurityContext.runSecured(
                    () -> {
                        if (JobContextUtil.isBatchJob(jobContext)) {
                            log.debug("Trying to commit states batch mode");
                            ((BatchTableCommit) tableCommit).commit(fileCommittables);
                        } else {
                            log.debug("Trying to commit states streaming mode");
                            ((StreamTableCommit) tableCommit)
                                    .commit(Objects.hash(fileCommittables), fileCommittables);
                        }
                        return null;
                    });
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.TABLE_WRITE_COMMIT_FAILED,
                    "Flink table store commit operation failed",
                    e);
        }
        return Collections.emptyList();
    }

    @Override
    public PaimonAggregatedCommitInfo combine(List<PaimonCommitInfo> commitInfos) {
        List<List<CommitMessage>> committables = new ArrayList<>();
        commitInfos.forEach(commitInfo -> committables.add(commitInfo.getCommittables()));
        return new PaimonAggregatedCommitInfo(committables);
    }

    @Override
    public void abort(List<PaimonAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {
        // TODO find the right way to abort
    }

    @Override
    public void close() throws IOException {}
}
