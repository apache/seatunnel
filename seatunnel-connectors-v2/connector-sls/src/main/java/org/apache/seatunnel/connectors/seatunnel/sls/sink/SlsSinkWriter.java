package org.apache.seatunnel.connectors.seatunnel.sls.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.sls.serialization.SeatunnelRowSerialization;
import org.apache.seatunnel.connectors.seatunnel.sls.state.SlsCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.sls.state.SlsSinkState;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.request.PutLogsRequest;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.ACCESS_KEY_ID;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.ACCESS_KEY_SECRET;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.LOGSTORE;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.LOG_GROUP_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.SOURCE;
import static org.apache.seatunnel.connectors.seatunnel.sls.config.Config.TOPIC;

@Slf4j
public class SlsSinkWriter implements SinkWriter<SeaTunnelRow, SlsCommitInfo, SlsSinkState> {

    private final Client client;
    private final String project;
    private final String logStore;
    private final String topic;
    private final String source;
    private final Integer logGroupSize;
    private final SinkWriter.Context context;
    private final List<SlsSinkState> slsStates;
    private final SeatunnelRowSerialization seatunnelRowSerialization;

    public SlsSinkWriter(
            SinkWriter.Context context,
            SeaTunnelRowType seaTunnelRowType,
            ReadonlyConfig pluginConfig,
            List<SlsSinkState> slsStates) {

        this.client =
                new Client(
                        pluginConfig.get(ENDPOINT),
                        pluginConfig.get(ACCESS_KEY_ID),
                        pluginConfig.get(ACCESS_KEY_SECRET));
        this.project = pluginConfig.get(PROJECT);
        this.logStore = pluginConfig.get(LOGSTORE);
        this.topic = pluginConfig.get(TOPIC);
        this.source = pluginConfig.get(SOURCE);
        this.logGroupSize = pluginConfig.get(LOG_GROUP_SIZE);
        this.context = context;
        this.slsStates = slsStates;
        this.seatunnelRowSerialization = new SeatunnelRowSerialization(seaTunnelRowType);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        List<LogItem> data = this.seatunnelRowSerialization.serializeRow(element);
        PutLogsRequest plr = new PutLogsRequest(project, logStore, topic, source, data);
        try {
            this.client.PutLogs(plr);
        } catch (Throwable e) {
            log.error("write logs failed", e);
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    @Override
    public Optional<SlsCommitInfo> prepareCommit() throws IOException {
        // nothing to do, when write function, data had sended
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public List<SlsSinkState> snapshotState(long checkpointId) {
        return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
        this.client.shutdown();
    }
}
