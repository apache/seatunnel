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

package org.apache.seatunnel.connectors.seatunnel.file.local.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.local.source.config.MultipleTableLocalFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.local.source.split.LocalFileAccordingToSplitSizeSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseMultipleTableFileSource;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.DefaultFileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSplitStrategy;

import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions.DEFAULT_ROW_DELIMITER;

public class LocalFileSource extends BaseMultipleTableFileSource {

    public LocalFileSource(ReadonlyConfig readonlyConfig) {
        super(
                new MultipleTableLocalFileSourceConfig(readonlyConfig),
                initFileSplitStrategy(readonlyConfig));
    }

    @Override
    public String getPluginName() {
        return FileSystemType.LOCAL.getFileSystemPluginName();
    }

    private static FileSplitStrategy initFileSplitStrategy(ReadonlyConfig readonlyConfig) {
        if (readonlyConfig.get(FileBaseSourceOptions.ENABLE_FILE_SPLIT)) {
            return new DefaultFileSplitStrategy();
        }
        String rowDelimiter =
                !readonlyConfig.getOptional(FileBaseSourceOptions.ROW_DELIMITER).isPresent()
                        ? DEFAULT_ROW_DELIMITER
                        : readonlyConfig.get(FileBaseSourceOptions.ROW_DELIMITER);
        long skipHeaderRowNumber =
                readonlyConfig.get(FileBaseSourceOptions.CSV_USE_HEADER_LINE)
                        ? 1L
                        : readonlyConfig.get(FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER);
        String encodingName = readonlyConfig.get(FileBaseSourceOptions.ENCODING);
        long splitSize = readonlyConfig.get(FileBaseSourceOptions.FILE_SPLIT_SIZE);
        return new LocalFileAccordingToSplitSizeSplitStrategy(
                rowDelimiter, skipHeaderRowNumber, encodingName, splitSize);
    }
}
