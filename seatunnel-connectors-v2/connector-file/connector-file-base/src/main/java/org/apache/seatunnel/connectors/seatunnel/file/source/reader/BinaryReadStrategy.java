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
import org.apache.seatunnel.api.table.type.BinaryObject;
import org.apache.seatunnel.api.table.type.BinaryType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;

/** @describe @Author jxm */
@Slf4j
public class BinaryReadStrategy extends AbstractReadStrategy {
    private String basePath;

    @Override
    public void init(HadoopConf conf) {
        super.init(conf);
        if (pluginConfig.hasPath(BaseSourceConfigOptions.FILE_PATH.key())) {
            basePath = pluginConfig.getString(BaseSourceConfigOptions.FILE_PATH.key());
        }
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        super.setSeaTunnelRowTypeInfo(seaTunnelRowType);
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws FileConnectorException, IOException {
        InputStream inputStream = hadoopFileSystemProxy.getInputStream(path);
        try {
            int isFirst = 1;
            byte[] bys = new byte[1024 * 1024];
            int len = 0;
            int offset = 0;
            while (true) {
                len = inputStream.read(bys, offset, bys.length - offset);
                offset += len;
                if (offset >= bys.length) {
                    putData(tableId, output, path, bys, offset, isFirst);
                    isFirst = 0;
                    len = 0;
                    offset = 0;
                }
                if (len == -1) {
                    if (offset > 0) {
                        putData(tableId, output, path, bys, offset, isFirst);
                    }
                    break;
                }
            }
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("BinaryFile", "read", path, e);
        }
    }

    private void putData(
            String tableId,
            Collector<SeaTunnelRow> output,
            String path,
            byte[] bys,
            int len,
            int isFirst) {
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(1);
        seaTunnelRow.setField(
                0,
                new BinaryObject(
                        StringUtils.substringAfterLast(path, basePath),
                        // TODO Base64 Resolve garbled code
                        Base64.getEncoder().encodeToString(bys),
                        len,
                        isFirst));
        seaTunnelRow.setTableId(tableId);
        output.collect(seaTunnelRow);
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        seaTunnelRowType =
                new SeaTunnelRowType(
                        // TODO
                        new String[] {"binary"}, new SeaTunnelDataType[] {BinaryType.INSTANCE});
        return seaTunnelRowType;
    }
}
