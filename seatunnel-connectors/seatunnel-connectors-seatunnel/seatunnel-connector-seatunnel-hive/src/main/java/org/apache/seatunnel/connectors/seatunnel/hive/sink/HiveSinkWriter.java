package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.file.writer.FileWriter;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.file.writer.HdfsTxtFileWriter;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HiveSinkWriter implements SinkWriter<SeaTunnelRow, HiveCommitInfo, HiveSinkState> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveSinkWriter.class);

    private SeaTunnelRowTypeInfo seaTunnelRowTypeInfo;
    private Config pluginConfig;
    private SinkWriter.Context context;
    private long jobId;

    private FileWriter fileWriter;

    private HiveSinkConfig hiveSinkConfig;

    public HiveSinkWriter(SeaTunnelRowTypeInfo seaTunnelRowTypeInfo,
                          Config pluginConfig,
                          Context context,
                          long jobId) {
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
        this.pluginConfig = pluginConfig;
        this.context = context;
        this.jobId = jobId;

        hiveSinkConfig = new HiveSinkConfig(this.pluginConfig);
        fileWriter = new HdfsTxtFileWriter(this.seaTunnelRowTypeInfo,
            hiveSinkConfig,
            this.jobId,
            this.context.getIndexOfSubtask());
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        fileWriter.write(element);
    }

    @Override
    public Optional<HiveCommitInfo> prepareCommit() throws IOException {
        fileWriter.finishAndCloseWriteFile();
        /**
         * We will clear the needMoveFiles in {@link #snapshotState()}, So we need copy the needMoveFiles map here.
         */
        Map<String, String> commitInfoMap = new HashMap<>();
        commitInfoMap.putAll(fileWriter.getNeedMoveFiles());
        return Optional.of(new HiveCommitInfo(commitInfoMap));
    }

    @Override
    public void abort() {
        fileWriter.abort();
    }

    @Override
    public void close() throws IOException {
        fileWriter.finishAndCloseWriteFile();
    }

    @Override
    public List<HiveSinkState> snapshotState() throws IOException {
        //clear the map
        fileWriter.resetFileWriter(System.currentTimeMillis() + "");
        return Lists.newArrayList(new HiveSinkState(hiveSinkConfig));
    }
}
