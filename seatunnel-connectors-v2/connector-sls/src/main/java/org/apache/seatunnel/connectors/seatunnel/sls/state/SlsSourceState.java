package org.apache.seatunnel.connectors.seatunnel.sls.state;

import org.apache.seatunnel.connectors.seatunnel.sls.source.SlsSourceSplit;

import lombok.Data;

import java.io.Serializable;
import java.util.Set;

@Data
public class SlsSourceState implements Serializable {

    private Set<SlsSourceSplit> assignedSplit;

    public SlsSourceState(Set<SlsSourceSplit> assignedSplit) {
        this.assignedSplit = assignedSplit;
    }

    public Set<SlsSourceSplit> getAssignedSplit() {
        return this.assignedSplit;
    }
}
