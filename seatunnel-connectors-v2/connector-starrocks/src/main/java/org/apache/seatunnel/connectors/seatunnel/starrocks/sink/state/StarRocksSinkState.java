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
    private final String labelPrefix;
    private final long checkpointId;

    public StarRocksSinkState(String labelPrefix, long checkpointId) {
        this.labelPrefix = labelPrefix;
        this.checkpointId = checkpointId;
    }
}
