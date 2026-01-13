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
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.config.MultipleTableHdfsFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.split.HdfsFileSplitStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.split.HdfsMultipleTableFileSourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseMultipleTableFileSource;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;

public class HdfsFileSource extends BaseMultipleTableFileSource {

    private final MultipleTableHdfsFileSourceConfig sourceConfig;
    private final FileSplitStrategy fileSplitStrategy;

    public HdfsFileSource(ReadonlyConfig readonlyConfig) {
        this(
                new MultipleTableHdfsFileSourceConfig(readonlyConfig),
                HdfsFileSplitStrategyFactory.initFileSplitStrategy(readonlyConfig));
    }

    private HdfsFileSource(
            MultipleTableHdfsFileSourceConfig sourceConfig, FileSplitStrategy fileSplitStrategy) {
        super(sourceConfig, fileSplitStrategy);
        this.sourceConfig = sourceConfig;
        this.fileSplitStrategy = fileSplitStrategy;
    }

    @Override
    public String getPluginName() {
        return FileSystemType.HDFS.getFileSystemPluginName();
    }

    @Override
    public SourceSplitEnumerator<FileSourceSplit, FileSourceState> createEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> enumeratorContext) {
        return new HdfsMultipleTableFileSourceSplitEnumerator(
                enumeratorContext, sourceConfig, fileSplitStrategy);
    }

    @Override
    public SourceSplitEnumerator<FileSourceSplit, FileSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> enumeratorContext,
            FileSourceState checkpointState) {
        return new HdfsMultipleTableFileSourceSplitEnumerator(
                enumeratorContext, sourceConfig, fileSplitStrategy, checkpointState);
    }
}
