package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import java.util.Collections;
import java.util.List;

public class FileSplitStrategy {

    public List<FileSourceSplit> split(String tableId, String filePath, long splitSize) {
        return Collections.singletonList(new FileSourceSplit(tableId, filePath));
    }
}
