package org.apache.seatunnel.connectors.seatunnel.socket.source;

import org.apache.seatunnel.api.source.SourceSplit;

public class SocketSourceSplit implements SourceSplit {
    private final String splitId;

    public SocketSourceSplit(String splitId) {
        this.splitId = splitId;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
