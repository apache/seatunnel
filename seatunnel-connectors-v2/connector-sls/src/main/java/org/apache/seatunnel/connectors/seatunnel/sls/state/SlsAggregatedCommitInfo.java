package org.apache.seatunnel.connectors.seatunnel.sls.state;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class SlsAggregatedCommitInfo {
    List<SlsCommitInfo> commitInfos;
}
