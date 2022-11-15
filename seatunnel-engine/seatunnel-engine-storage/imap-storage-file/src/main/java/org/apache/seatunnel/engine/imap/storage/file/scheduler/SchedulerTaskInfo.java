package org.apache.seatunnel.engine.imap.storage.file.scheduler;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SchedulerTaskInfo {

    private long scheduledTime;
    private long latestTime;
}
