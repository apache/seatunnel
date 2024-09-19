package org.apache.seatunnel.connectors.seatunnel.prometheus.pojo;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class InstantPoint {
    private Map<String, String> metric;

    private List value;
}
