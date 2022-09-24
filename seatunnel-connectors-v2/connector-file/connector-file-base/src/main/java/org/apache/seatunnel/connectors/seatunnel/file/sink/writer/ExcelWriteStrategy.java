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
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.TextFileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.ExcelGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.FileSystemUtils;

import lombok.NonNull;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExcelWriteStrategy extends AbstractWriteStrategy {
    private Map<String, ExcelGenerator> beingWrittenWriter;

    public ExcelWriteStrategy(TextFileSinkConfig textFileSinkConfig) {
        super(textFileSinkConfig);
        this.beingWrittenWriter = new HashMap<>();
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) throws Exception {
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        ExcelGenerator excelGenerator = getOrCreateExcelGenerator(filePath);
        excelGenerator.writeData(seaTunnelRow);
    }

    @Override
    public void finishAndCloseFile() {
        this.beingWrittenWriter.forEach((k, v) -> {
            try {
                FileSystemUtils.createFile(k);
                FSDataOutputStream fileOutputStream = FileSystemUtils.getOutputStream(k);
                v.flushAndCloseExcel(fileOutputStream);
                fileOutputStream.close();
            } catch (IOException e) {
                log.error("can not get output file stream");
                throw new RuntimeException(e);
            }
            needMoveFiles.put(k, getTargetLocation(k));
        });
    }

    private ExcelGenerator getOrCreateExcelGenerator(@NonNull String filePath) {
        ExcelGenerator excelGenerator = this.beingWrittenWriter.get(filePath);
        if (excelGenerator == null) {
            excelGenerator = new ExcelGenerator(sinkColumnsIndexInRow, seaTunnelRowType, textFileSinkConfig);
            this.beingWrittenWriter.put(filePath, excelGenerator);
        }
        return excelGenerator;
    }
}
