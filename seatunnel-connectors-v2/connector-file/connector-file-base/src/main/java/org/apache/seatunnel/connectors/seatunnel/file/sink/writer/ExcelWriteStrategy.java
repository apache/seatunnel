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
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.ExcelGenerator;

import org.apache.hadoop.fs.FSDataOutputStream;

import lombok.NonNull;

import java.io.IOException;
import java.util.LinkedHashMap;

public class ExcelWriteStrategy extends AbstractWriteStrategy<ExcelGenerator> {
    private final LinkedHashMap<String, ExcelGenerator> beingWrittenWriter;

    public ExcelWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenWriter = new LinkedHashMap<>();
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) {
        super.write(seaTunnelRow);
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        ExcelGenerator excelGenerator = getOrCreateOutputStream(filePath);
        excelGenerator.writeData(seaTunnelRow);
    }

    @Override
    public void finishAndCloseFile() {
        this.beingWrittenWriter.forEach(
                (k, v) -> {
                    try {
                        hadoopFileSystemProxy.createFile(k);
                        FSDataOutputStream fileOutputStream =
                                hadoopFileSystemProxy.getOutputStream(k);
                        v.flushAndCloseExcel(fileOutputStream);
                        fileOutputStream.close();
                    } catch (IOException e) {
                        throw CommonError.fileOperationFailed("ExcelFile", "write", k, e);
                    }
                    needMoveFiles.put(k, getTargetLocation(k));
                });
        beingWrittenWriter.clear();
    }

    @Override
    public ExcelGenerator getOrCreateOutputStream(@NonNull String filePath) {
        ExcelGenerator excelGenerator = this.beingWrittenWriter.get(filePath);
        if (excelGenerator == null) {
            excelGenerator =
                    new ExcelGenerator(sinkColumnsIndexInRow, seaTunnelRowType, fileSinkConfig);
            this.beingWrittenWriter.put(filePath, excelGenerator);
        }
        return excelGenerator;
    }
}
