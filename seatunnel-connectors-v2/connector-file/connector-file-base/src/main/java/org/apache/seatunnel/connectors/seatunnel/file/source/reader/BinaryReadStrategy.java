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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/** Used to read file to binary stream */
public class BinaryReadStrategy extends AbstractReadStrategy {

    public static SeaTunnelRowType binaryRowType =
            new SeaTunnelRowType(
                    new String[] {"data", "relativePath", "partIndex"},
                    new SeaTunnelDataType[] {
                        PrimitiveByteArrayType.INSTANCE, BasicType.STRING_TYPE, BasicType.LONG_TYPE
                    });

    private File basePath;

    @Override
    public void init(HadoopConf conf) {
        super.init(conf);
        basePath = new File(pluginConfig.getString(BaseSourceConfigOptions.FILE_PATH.key()));
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        try (InputStream inputStream = hadoopFileSystemProxy.getInputStream(path)) {
            String relativePath;
            if (hadoopFileSystemProxy.isFile(basePath.getAbsolutePath())) {
                relativePath = basePath.getName();
            } else {
                relativePath =
                        path.substring(
                                path.indexOf(basePath.getAbsolutePath())
                                        + basePath.getAbsolutePath().length());
                if (relativePath.startsWith(File.separator)) {
                    relativePath = relativePath.substring(File.separator.length());
                }
            }
            // TODO config this size
            int maxSize = 1024;
            byte[] buffer = new byte[maxSize];
            long partIndex = 0;
            int readSize;
            while ((readSize = inputStream.read(buffer)) != -1) {
                if (readSize != maxSize) {
                    buffer = Arrays.copyOf(buffer, readSize);
                }
                SeaTunnelRow row = new SeaTunnelRow(new Object[] {buffer, relativePath, partIndex});
                buffer = new byte[1024];
                row.setTableId(tableId);
                output.collect(row);
                partIndex++;
            }
        }
    }

    /**
     * Returns a fixed SeaTunnelRowType used to store file fragments.
     *
     * <p>`data`: Holds the binary data of the file fragment. When the data is empty, it indicates
     * the end of the file.
     *
     * <p>`relativePath`: Represents the sub-path of the file.
     *
     * <p>`partIndex`: Indicates the order of the file fragment.
     */
    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        return binaryRowType;
    }
}
