package org.apache.seatunnel.connectors.seatunnel.prometheus.pojo;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class RangePoint {

    private Map<String, String> metric;

    private List<List> values;
}
