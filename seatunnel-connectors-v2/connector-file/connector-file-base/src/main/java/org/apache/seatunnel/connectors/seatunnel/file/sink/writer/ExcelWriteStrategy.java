package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.ExcelGenerator;

import org.apache.hadoop.fs.FSDataOutputStream;

import lombok.NonNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExcelWriteStrategy extends AbstractWriteStrategy {
    private final Map<String, ExcelGenerator> beingWrittenWriter;

    public ExcelWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenWriter = new HashMap<>();
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) {
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        ExcelGenerator excelGenerator = getOrCreateExcelGenerator(filePath);
        excelGenerator.writeData(seaTunnelRow);
    }

    @Override
    public void finishAndCloseFile() {
        this.beingWrittenWriter.forEach(
                (key, value) -> {
                    try (FSDataOutputStream fileOutputStream =
                            fileSystemUtils.getOutputStream(key)) {
                        fileSystemUtils.createFile(key);
                        value.flushAndCloseExcel(fileOutputStream);
                    } catch (IOException e) {
                        throw new FileConnectorException(
                                CommonErrorCode.FLUSH_DATA_FAILED,
                                String.format("Flush data to this file [%s] failed", key),
                                e);
                    }
                    needMoveFiles.put(key, getTargetLocation(key));
                });
    }

    private ExcelGenerator getOrCreateExcelGenerator(@NonNull String filePath) {
        ExcelGenerator excelGenerator = this.beingWrittenWriter.get(filePath);
        if (excelGenerator == null) {
            excelGenerator =
                    new ExcelGenerator(sinkColumnsIndexInRow, seaTunnelRowType, fileSinkConfig);
            this.beingWrittenWriter.put(filePath, excelGenerator);
        }
        return excelGenerator;
    }
}
