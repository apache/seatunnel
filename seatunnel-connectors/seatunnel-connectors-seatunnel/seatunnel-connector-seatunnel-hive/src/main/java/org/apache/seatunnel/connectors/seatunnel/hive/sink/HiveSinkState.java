package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
@AllArgsConstructor
public class HiveSinkState implements Serializable {
    private HiveSinkConfig hiveSinkConfig;
}
