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
package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.config.ArchiveCompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;

import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions.DEFAULT_ROW_DELIMITER;

@Slf4j
public class FileSplitStrategyFactory {

    public static FileSplitStrategy initFileSplitStrategy(
            ReadonlyConfig readonlyConfig, HadoopConf hadoopConf) {
        if (!readonlyConfig.get(FileBaseSourceOptions.ENABLE_FILE_SPLIT)) {
            return new DefaultFileSplitStrategy();
        }
        FileFormat fileFormat = readonlyConfig.get(FileBaseSourceOptions.FILE_FORMAT_TYPE);
        if (!fileFormat.supportFileSplit()) {
            log.warn(
                    "enable_file_split=true but file_format_type={} does not support file split. "
                            + "Falling back to non-splitting mode.",
                    fileFormat);
            return new DefaultFileSplitStrategy();
        }
        CompressFormat compressCodec = readonlyConfig.get(FileBaseSourceOptions.COMPRESS_CODEC);
        ArchiveCompressFormat archiveCompressCodec =
                readonlyConfig.get(FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC);
        if (compressCodec != CompressFormat.NONE
                || archiveCompressCodec != ArchiveCompressFormat.NONE) {
            log.warn(
                    "enable_file_split=true but compress_codec={} or archive_compress_codec={} is not NONE. "
                            + "Falling back to non-splitting mode.",
                    compressCodec,
                    archiveCompressCodec);
            return new DefaultFileSplitStrategy();
        }

        Objects.requireNonNull(
                hadoopConf, "hadoopConf must not be null when file split is enabled");

        long fileSplitSize = readonlyConfig.get(FileBaseSourceOptions.FILE_SPLIT_SIZE);
        if (fileSplitSize <= 0) {
            throw new SeaTunnelRuntimeException(
                    FileConnectorErrorCode.FILE_SPLIT_SIZE_ILLEGAL,
                    String.format(
                            "file_split_size must be greater than 0 when enable_file_split=true, but got: %d",
                            fileSplitSize));
        }
        if (FileFormat.PARQUET == fileFormat) {
            return new ParquetFileSplitStrategy(fileSplitSize, hadoopConf);
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
        return new AccordingToSplitSizeSplitStrategy(
                hadoopConf, rowDelimiter, skipHeaderRowNumber, encodingName, fileSplitSize);
    }
}
