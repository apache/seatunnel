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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.XmlWriter;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * An implementation of the AbstractWriteStrategy class that writes data in XML format.
 *
 * <p>This strategy stores multiple XmlWriter instances for different files being written and
 * ensures that each file is written to only once. It writes the data by passing the data row to the
 * corresponding XmlWriter instance.
 */
public class XmlWriteStrategy extends AbstractWriteStrategy<XmlWriter> {

    private final LinkedHashMap<String, XmlWriter> beingWrittenWriter;

    public XmlWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenWriter = new LinkedHashMap<>();
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) throws FileConnectorException {
        super.write(seaTunnelRow);
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        XmlWriter xmlDocWriter = getOrCreateOutputStream(filePath);
        xmlDocWriter.writeData(seaTunnelRow);
    }

    @Override
    public void finishAndCloseFile() {
        this.beingWrittenWriter.forEach(
                (k, v) -> {
                    try {
                        hadoopFileSystemProxy.createFile(k);
                        FSDataOutputStream fileOutputStream =
                                hadoopFileSystemProxy.getOutputStream(k);
                        v.flushAndCloseXmlWriter(fileOutputStream);
                        fileOutputStream.close();
                    } catch (IOException e) {
                        throw CommonError.fileOperationFailed("XmlFile", "write", k, e);
                    }
                    needMoveFiles.put(k, getTargetLocation(k));
                });
        this.beingWrittenWriter.clear();
    }

    @Override
    public XmlWriter getOrCreateOutputStream(String filePath) {
        return beingWrittenWriter.computeIfAbsent(
                filePath,
                k -> new XmlWriter(fileSinkConfig, sinkColumnsIndexInRow, seaTunnelRowType));
    }
}
