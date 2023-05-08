package org.apache.seatunnel.connectors.seatunnel.starrocks.sink.state;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
@EqualsAndHashCode
public class StarRocksSinkState {

    //    private final String labelPrefix;
    //    private final long checkpointId;

    private final String labelPrefix;
    private final long checkpointId;
    private final int subTaskIndex;

    //    private final String transactionId;
    //    private final String uuidPrefix;
    //    private final Long checkpointId;
    //    private final Map<String, String> needMoveFiles;
    //    private final Map<String, List<String>> partitionDirAndValuesMap;
    //    private final String transactionDir;

    public StarRocksSinkState(String labelPrefix, long checkpointId, int subTaskIndex) {
        this.labelPrefix = labelPrefix;
        this.checkpointId = checkpointId;
        this.subTaskIndex = subTaskIndex;
    }
}
