package org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink;

import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;

import java.util.HashMap;
import java.util.Map;

public class HttpPutBuilder {
    Map<String, String> header;

    public HttpPutBuilder() {
        header = new HashMap<>();
    }

    public HttpPutBuilder(SinkConfig sinkConfig) {
        header = new HashMap<>();
    }
}
