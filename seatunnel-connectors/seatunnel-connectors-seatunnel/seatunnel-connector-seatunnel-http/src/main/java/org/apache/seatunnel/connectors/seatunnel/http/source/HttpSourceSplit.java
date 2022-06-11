package org.apache.seatunnel.connectors.seatunnel.http.source;

import org.apache.seatunnel.api.source.SourceSplit;

public class HttpSourceSplit implements SourceSplit {

    private final String splitId;

    public HttpSourceSplit(String splitId) {
        this.splitId = splitId;
    }

    @Override
    public String splitId() {
        return this.splitId;
    }
}
