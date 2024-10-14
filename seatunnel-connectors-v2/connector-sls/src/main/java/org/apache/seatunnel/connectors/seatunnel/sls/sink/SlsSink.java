package org.apache.seatunnel.connectors.seatunnel.sls.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.sls.state.SlsAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.sls.state.SlsCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.sls.state.SlsSinkState;

import java.io.IOException;
import java.util.Collections;

public class SlsSink
        implements SeaTunnelSink<
                SeaTunnelRow, SlsSinkState, SlsCommitInfo, SlsAggregatedCommitInfo> {
    private final ReadonlyConfig pluginConfig;
    private final SeaTunnelRowType seaTunnelRowType;

    public SlsSink(ReadonlyConfig pluginConfig, SeaTunnelRowType rowType) {
        this.pluginConfig = pluginConfig;
        this.seaTunnelRowType = rowType;
    }

    @Override
    public String getPluginName() {
        return org.apache.seatunnel.connectors.seatunnel.sls.config.Config.CONNECTOR_IDENTITY;
    }

    @Override
    public SinkWriter<SeaTunnelRow, SlsCommitInfo, SlsSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new SlsSinkWriter(context, seaTunnelRowType, pluginConfig, Collections.emptyList());
    }
}
