package org.apache.seatunnel.connectors.seatunnel.prometheus.write;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class Point {

    private Map<String, String> metricLableMap;

    private Double value;

    private Long timestamp;
}
