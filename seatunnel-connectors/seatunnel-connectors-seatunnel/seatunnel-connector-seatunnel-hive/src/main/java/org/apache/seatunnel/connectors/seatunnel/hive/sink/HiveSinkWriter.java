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

package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.file.writer.FileWriter;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.file.writer.HdfsTxtFileWriter;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
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
    private HiveSinkState hiveSinkState;
    private long sinkId;

    private FileWriter fileWriter;

    private HiveSinkConfig hiveSinkConfig;

    public HiveSinkWriter(SeaTunnelRowTypeInfo seaTunnelRowTypeInfo,
                          Config pluginConfig,
                          Context context,
                          long jobId,
                          HiveSinkState hiveSinkState) {
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
        this.pluginConfig = pluginConfig;
        this.context = context;
        this.sinkId = jobId;
        this.hiveSinkState = hiveSinkState;
        hiveSinkConfig = new HiveSinkConfig(pluginConfig);
        fileWriter = new HdfsTxtFileWriter(seaTunnelRowTypeInfo, hiveSinkConfig, jobId, context.getIndexOfSubtask());
    }

    public HiveSinkWriter(SeaTunnelRowTypeInfo seaTunnelRowTypeInfo,
                          Config pluginConfig,
                          Context context,
                          long jobId) {
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
        this.pluginConfig = pluginConfig;
        this.context = context;
        this.sinkId = jobId;

        hiveSinkConfig = new HiveSinkConfig(pluginConfig);
        fileWriter = new HdfsTxtFileWriter(seaTunnelRowTypeInfo, hiveSinkConfig, jobId, context.getIndexOfSubtask());
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
        Map<String, String> commitInfoMap = new HashMap<>(fileWriter.getNeedMoveFiles().size());
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
        Map<String, String> commitInfoMap = new HashMap<>(fileWriter.getNeedMoveFiles().size());

        // snapshotState called after prepareCommit, so all files have been added to needMoveFiles
        commitInfoMap.putAll(fileWriter.getNeedMoveFiles());

        //clear the map
        fileWriter.resetFileWriter(System.currentTimeMillis() + "");
        return Lists.newArrayList(new HiveSinkState(commitInfoMap, hiveSinkConfig));
    }
}
