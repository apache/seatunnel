package org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.state.HudiCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.state.HudiSinkState;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class HudiSinkWriter implements SinkWriter<SeaTunnelRow, HudiCommitInfo, HudiSinkState> {

    public HudiSinkWriter(
            SinkWriter.Context context,
            List<HudiSinkState> state,
            SeaTunnelRowType seaTunnelRowType,
            Config pluginConfig) {}

    @Override
    public void write(SeaTunnelRow element) throws IOException {}

    @Override
    public Optional<HudiCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {}
}
