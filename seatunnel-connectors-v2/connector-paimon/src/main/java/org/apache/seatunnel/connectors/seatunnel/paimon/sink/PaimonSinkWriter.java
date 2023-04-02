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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.state.PaimonSinkState;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.RowConverter;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.TableWrite;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class PaimonSinkWriter
        implements SinkWriter<SeaTunnelRow, PaimonCommitInfo, PaimonSinkState> {

    private String commitUser = UUID.randomUUID().toString();

    private TableWrite tableWrite = null;

    private long checkpointId = 0;

    private List<CommitMessage> committables = new ArrayList<>();

    private final Table table;

    private final SeaTunnelRowType seaTunnelRowType;

    private final SinkWriter.Context context;

    public PaimonSinkWriter(Context context, Table table, SeaTunnelRowType seaTunnelRowType) {
        this.table = table;
        this.seaTunnelRowType = seaTunnelRowType;
        this.context = context;
    }

    public PaimonSinkWriter(
            Context context,
            Table table,
            SeaTunnelRowType seaTunnelRowType,
            List<PaimonSinkState> states) {
        this.table = table;
        this.seaTunnelRowType = seaTunnelRowType;
        this.context = context;
        if (Objects.isNull(states) || states.isEmpty()) {
            return;
        }
        this.commitUser = states.get(0).getCommitUser();
        this.checkpointId = states.get(0).getCheckpointId();
        try (BatchTableCommit tableCommit =
                ((InnerTableCommit) table.newBatchWriteBuilder().newCommit())
                        .withLock(Lock.emptyFactory().create())) {
            List<CommitMessage> commitables =
                    states.stream()
                            .map(PaimonSinkState::getCommittables)
                            .flatMap(List::stream)
                            .collect(Collectors.toList());
            log.info("Trying to recommit states {}", commitables);
            tableCommit.commit(commitables);
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.TABLE_WRITE_COMMIT_FAILED, e);
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (Objects.isNull(tableWrite)) {
            tableWrite = table.newBatchWriteBuilder().newWrite();
        }
        InternalRow rowData = RowConverter.convert(element, seaTunnelRowType);
        try {
            tableWrite.write(rowData);
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.TABLE_WRITE_RECORD_FAILED,
                    "This record " + element + " failed to be written",
                    e);
        }
    }

    @Override
    public Optional<PaimonCommitInfo> prepareCommit() throws IOException {
        try {
            List<CommitMessage> fileCommittables = ((BatchTableWrite) tableWrite).prepareCommit();
            committables.addAll(fileCommittables);
            return Optional.of(new PaimonCommitInfo(fileCommittables));
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.TABLE_PRE_COMMIT_FAILED,
                    "Flink table store failed to prepare commit",
                    e);
        }
    }

    @Override
    public List<PaimonSinkState> snapshotState(long checkpointId) throws IOException {
        this.checkpointId = checkpointId;
        PaimonSinkState paimonSinkState =
                new PaimonSinkState(new ArrayList<>(committables), commitUser, checkpointId);
        committables.clear();
        return Collections.singletonList(paimonSinkState);
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {}
}
