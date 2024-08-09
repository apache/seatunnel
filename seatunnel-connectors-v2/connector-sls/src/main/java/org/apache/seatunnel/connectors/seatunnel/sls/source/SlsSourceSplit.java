package org.apache.seatunnel.connectors.seatunnel.sls.source;

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.Getter;

public class SlsSourceSplit implements SourceSplit {

    @Getter private String project;
    @Getter private String logStore;
    @Getter private String consumer;
    @Getter private Integer shardId;
    @Getter private String startCursor;
    @Getter private Integer fetchSize;

    SlsSourceSplit(
            String project,
            String logStore,
            String consumer,
            Integer shardId,
            String startCursor,
            Integer fetchSize) {
        this.project = project;
        this.logStore = logStore;
        this.consumer = consumer;
        this.shardId = shardId;
        this.startCursor = startCursor;
        this.fetchSize = fetchSize;
    }

    @Override
    public String splitId() {
        return String.valueOf(shardId);
    }

    public void setStartCursor(String cursor) {
        this.startCursor = cursor;
    }

    public SlsSourceSplit copy() {
        return new SlsSourceSplit(
                this.project,
                this.logStore,
                this.consumer,
                this.shardId,
                this.startCursor,
                this.fetchSize);
    }
}
