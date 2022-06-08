package org.apache.seatunnel.connectors.seatunnel.hive.sink.file.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Map;

public interface FileWriter {

    void write(SeaTunnelRow seaTunnelRow);

    Map<String, String> getNeedMoveFiles();

    /**
     * In this method we need finish write the file. The following operations are often required:
     * 1. Flush memory to disk.
     * 2. Close output stream.
     * 3. Add the mapping relationship between seatunnel file path and hive file path to needMoveFiles.
     */
    void finishAndCloseWriteFile();

    /**
     * The writer needs to be reset after each checkpoint is completed
     *
     * @param checkpointId checkpointId
     */
    void resetFileWriter(String checkpointId);

    void abort();
}
