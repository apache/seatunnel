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

package org.apache.seatunnel.connectors.seatunnel.hive.source;

import static org.apache.seatunnel.connectors.seatunnel.hive.config.SourceConfig.FILE_PATH;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.hive.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HivePluginException;
import org.apache.seatunnel.connectors.seatunnel.hive.source.file.reader.format.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.hive.source.file.reader.format.ReadStrategyFactory;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.List;

@AutoService(SeaTunnelSource.class)
public class HiveSource implements SeaTunnelSource<SeaTunnelRow, HiveSourceSplit, HiveSourceState> {

    private SeaTunnelContext seaTunnelContext;

    private SeaTunnelRowType typeInfo;

    private ReadStrategy readStrategy;

    private HadoopConf hadoopConf;

    private List<String> filesPath;

    @Override
    public String getPluginName() {
        return "Hive";
    }

    @Override
    public void prepare(Config pluginConfig) {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, FILE_PATH, FS_DEFAULT_NAME_KEY);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
        // use factory to generate readStrategy
        readStrategy = ReadStrategyFactory.of(pluginConfig.getString(SourceConfig.FILE_TYPE));
        String path = pluginConfig.getString(FILE_PATH);
        hadoopConf = new HadoopConf(pluginConfig.getString(FS_DEFAULT_NAME_KEY));
        try {
            filesPath = readStrategy.getFileNamesByPath(hadoopConf, path);
        } catch (IOException e) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, "Check file path fail.");
        }
        try {
            // should read from config or read from hive metadata( wait catlog done)
            this.typeInfo = readStrategy.getSeaTunnelRowTypeInfo(hadoopConf, filesPath.get(0));
        } catch (HivePluginException e) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, "Read hive file type error.", e);
        }
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.typeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, HiveSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new HiveSourceReader(this.readStrategy, this.hadoopConf, readerContext);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceSplitEnumerator<HiveSourceSplit, HiveSourceState> createEnumerator(SourceSplitEnumerator.Context<HiveSourceSplit> enumeratorContext) throws Exception {
        return new HiveSourceSplitEnumerator(enumeratorContext, filesPath);
    }

    @Override
    public SourceSplitEnumerator<HiveSourceSplit, HiveSourceState> restoreEnumerator(SourceSplitEnumerator.Context<HiveSourceSplit> enumeratorContext, HiveSourceState checkpointState) throws Exception {
        return new HiveSourceSplitEnumerator(enumeratorContext, filesPath, checkpointState);
    }

}
