package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

import java.util.Map;

@Data
@AllArgsConstructor
public class HiveAggregatedCommitInfo {

    /**
     * Storage the commit info in map.
     * K is the file path need to be moved to hive data dir.
     * V is the target file path of the data file.
     */
    private Map<String, String> needMoveFiles;
}
