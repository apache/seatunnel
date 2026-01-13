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
package org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.split;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.ArchiveCompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.config.HdfsFileHadoopConfig;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.DefaultFileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSplitStrategy;

import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions.DEFAULT_ROW_DELIMITER;

public class HdfsFileSplitStrategyFactory {

    public static FileSplitStrategy initFileSplitStrategy(ReadonlyConfig readonlyConfig) {
        if (!readonlyConfig.get(FileBaseSourceOptions.ENABLE_FILE_SPLIT)) {
            return new DefaultFileSplitStrategy();
        }
        if (!readonlyConfig.get(FileBaseSourceOptions.FILE_FORMAT_TYPE).supportFileSplit()) {
            return new DefaultFileSplitStrategy();
        }
        if (readonlyConfig.get(FileBaseSourceOptions.COMPRESS_CODEC) != CompressFormat.NONE
                || readonlyConfig.get(FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC)
                        != ArchiveCompressFormat.NONE) {
            return new DefaultFileSplitStrategy();
        }
        long fileSplitSize = readonlyConfig.get(FileBaseSourceOptions.FILE_SPLIT_SIZE);
        HadoopConf hadoopConf = buildHadoopConf(readonlyConfig);
        if (FileFormat.PARQUET == readonlyConfig.get(FileBaseSourceOptions.FILE_FORMAT_TYPE)) {
            return new HdfsParquetFileSplitStrategy(fileSplitSize, hadoopConf);
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
        return new HdfsFileAccordingToSplitSizeSplitStrategy(
                hadoopConf, rowDelimiter, skipHeaderRowNumber, encodingName, fileSplitSize);
    }

    private static HadoopConf buildHadoopConf(ReadonlyConfig readonlyConfig) {
        HdfsFileHadoopConfig hadoopConf =
                new HdfsFileHadoopConfig(readonlyConfig.get(HdfsSourceConfigOptions.DEFAULT_FS));
        if (readonlyConfig.getOptional(HdfsSourceConfigOptions.HDFS_SITE_PATH).isPresent()) {
            hadoopConf.setHdfsSitePath(readonlyConfig.get(HdfsSourceConfigOptions.HDFS_SITE_PATH));
        }
        if (readonlyConfig.getOptional(HdfsSourceConfigOptions.REMOTE_USER).isPresent()) {
            hadoopConf.setRemoteUser(readonlyConfig.get(HdfsSourceConfigOptions.REMOTE_USER));
        }
        if (readonlyConfig.getOptional(HdfsSourceConfigOptions.KRB5_PATH).isPresent()) {
            hadoopConf.setKrb5Path(readonlyConfig.get(HdfsSourceConfigOptions.KRB5_PATH));
        }
        if (readonlyConfig.getOptional(HdfsSourceConfigOptions.KERBEROS_PRINCIPAL).isPresent()) {
            hadoopConf.setKerberosPrincipal(
                    readonlyConfig.get(HdfsSourceConfigOptions.KERBEROS_PRINCIPAL));
        }
        if (readonlyConfig.getOptional(HdfsSourceConfigOptions.KERBEROS_KEYTAB_PATH).isPresent()) {
            hadoopConf.setKerberosKeytabPath(
                    readonlyConfig.get(HdfsSourceConfigOptions.KERBEROS_KEYTAB_PATH));
        }
        return hadoopConf;
    }
}
