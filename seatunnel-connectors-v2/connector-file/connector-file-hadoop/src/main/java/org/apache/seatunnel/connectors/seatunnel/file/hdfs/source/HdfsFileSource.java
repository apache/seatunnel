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

package org.apache.seatunnel.connectors.seatunnel.file.hdfs.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.config.MultipleTableHdfsFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseMultipleTableFileSource;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSplitStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.MultipleTableFileSplitStrategy;

import java.util.HashMap;
import java.util.Map;

public class HdfsFileSource extends BaseMultipleTableFileSource {

    public HdfsFileSource(ReadonlyConfig readonlyConfig) {
        this(new MultipleTableHdfsFileSourceConfig(readonlyConfig));
    }

    private HdfsFileSource(MultipleTableHdfsFileSourceConfig sourceConfig) {
        super(sourceConfig, initFileSplitStrategy(sourceConfig));
    }

    @Override
    public String getPluginName() {
        return FileSystemType.HDFS.getFileSystemPluginName();
    }

    private static FileSplitStrategy initFileSplitStrategy(
            MultipleTableHdfsFileSourceConfig config) {
        Map<String, FileSplitStrategy> splitStrategies = new HashMap<>();
        for (BaseFileSourceConfig fileSourceConfig : config.getFileSourceConfigs()) {
            String tableId =
                    fileSourceConfig.getCatalogTable().getTableId().toTablePath().toString();
            splitStrategies.put(
                    tableId,
                    FileSplitStrategyFactory.initFileSplitStrategy(
                            fileSourceConfig.getBaseFileSourceConfig(),
                            fileSourceConfig.getHadoopConfig()));
        }
        return new MultipleTableFileSplitStrategy(splitStrategies);
    }
}
