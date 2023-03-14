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

package org.apache.seatunnel.connectors.seatunnel.fts.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.fts.exception.FlinkTableStoreConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fts.exception.FlinkTableStoreConnectorException;
import org.apache.seatunnel.connectors.seatunnel.fts.sink.commit.FlinkTableStoreCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.fts.sink.state.FlinkTableStoreState;
import org.apache.seatunnel.connectors.seatunnel.fts.utils.RowConverter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.table.SupportsWrite;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.table.store.table.sink.SerializableCommittable;
import org.apache.flink.table.store.table.sink.TableWrite;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class FlinkTableStoreSinkWriter
        implements SinkWriter<SeaTunnelRow, FlinkTableStoreCommitInfo, FlinkTableStoreState> {

    private final Table table;

    private final SeaTunnelRowType seaTunnelRowType;

    private final SinkWriter.Context context;

    private TableWrite tableWrite;

    public FlinkTableStoreSinkWriter(
            Context context, Table table, SeaTunnelRowType seaTunnelRowType) {
        this.table = table;
        this.seaTunnelRowType = seaTunnelRowType;
        this.context = context;
    }

    public FlinkTableStoreSinkWriter(
            Context context,
            Table table,
            SeaTunnelRowType seaTunnelRowType,
            List<FlinkTableStoreState> states) {
        this.table = table;
        this.seaTunnelRowType = seaTunnelRowType;
        this.context = context;
        // TODO: recommit committables
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (Objects.isNull(tableWrite)) {
            tableWrite = ((SupportsWrite) table).newWrite("SeaTunnel");
        }
        RowData rowData = RowConverter.convert(element, seaTunnelRowType);
        try {
            tableWrite.write(rowData);
        } catch (Exception e) {
            throw new FlinkTableStoreConnectorException(
                    FlinkTableStoreConnectorErrorCode.TABLE_WRITE_RECORD_FAILED,
                    "This record " + element + " failed to be written",
                    e);
        }
    }

    @Override
    public Optional<FlinkTableStoreCommitInfo> prepareCommit() throws IOException {
        if (Objects.isNull(tableWrite)) {
            return Optional.empty();
        }
        try {
            List<FileCommittable> fileCommittables = tableWrite.prepareCommit(true, 0L);
            List<SerializableCommittable> serializableCommittables =
                    fileCommittables.stream()
                            .map(SerializableCommittable::wrap)
                            .collect(Collectors.toList());
            return Optional.of(new FlinkTableStoreCommitInfo(serializableCommittables));
        } catch (Exception e) {
            throw new FlinkTableStoreConnectorException(
                    FlinkTableStoreConnectorErrorCode.TABLE_PRE_COMMIT_FAILED,
                    "Flink table store failed to prepare commit",
                    e);
        }
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {}
}
